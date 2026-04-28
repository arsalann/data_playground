{{ config(materialized='table') }}

WITH revenue AS (
    SELECT
        year,
        month_number AS month,
        ROUND(SUM(revenue_usd), 2) AS amount
    FROM {{ ref('stg_sales_fact') }}
    GROUP BY 1, 2
),

cogs AS (
    SELECT
        year,
        month_number AS month,
        ROUND(SUM(cost_usd), 2) AS amount
    FROM {{ ref('stg_sales_fact') }}
    GROUP BY 1, 2
),

opex_by_account AS (
    SELECT
        fiscal_year AS year,
        fiscal_month AS month,
        account_name,
        account_code,
        ROUND(SUM(net_amount), 2) AS amount,
        ROUND(SUM(COALESCE(budget_amount, 0)), 2) AS budget_amount
    FROM {{ ref('stg_financial_summary_monthly') }}
    WHERE account_category = 'OpEx'
    GROUP BY 1, 2, 3, 4
),

total_opex AS (
    SELECT year, month, SUM(amount) AS amount, SUM(budget_amount) AS budget_amount
    FROM opex_by_account
    GROUP BY 1, 2
),

pnl_lines AS (
    SELECT r.year, r.month, 'Revenue' AS line_item, 1 AS sort_order,
           r.amount, NULL AS budget_amount
    FROM revenue r

    UNION ALL

    SELECT c.year, c.month, 'Cost of Goods Sold', 2,
           -c.amount, NULL
    FROM cogs c

    UNION ALL

    SELECT r.year, r.month, 'Gross Profit', 3,
           r.amount - c.amount, NULL
    FROM revenue r
    INNER JOIN cogs c ON r.year = c.year AND r.month = c.month

    UNION ALL

    SELECT year, month,
           CONCAT('  ', account_name) AS line_item,
           10 + CAST(SUBSTR(account_code, 1, 2) AS INT64) AS sort_order,
           -amount, budget_amount
    FROM opex_by_account

    UNION ALL

    SELECT year, month, 'Total Operating Expenses', 90,
           -amount, budget_amount
    FROM total_opex

    UNION ALL

    SELECT r.year, r.month, 'Operating Income', 100,
           r.amount - c.amount - o.amount, NULL
    FROM revenue r
    INNER JOIN cogs c ON r.year = c.year AND r.month = c.month
    INNER JOIN total_opex o ON r.year = o.year AND r.month = o.month
)

SELECT
    year,
    month,
    line_item,
    sort_order,
    ROUND(amount, 2) AS amount,
    ROUND(budget_amount, 2) AS budget_amount,
    ROUND(amount - COALESCE(budget_amount, amount), 2) AS variance,
    ROUND(
        SAFE_DIVIDE(
            amount - COALESCE(budget_amount, amount),
            NULLIF(budget_amount, 0)
        ) * 100,
        2
    ) AS variance_pct
FROM pnl_lines
