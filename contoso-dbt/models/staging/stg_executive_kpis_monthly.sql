{{ config(materialized='table') }}

WITH monthly_sales AS (
    SELECT
        year,
        month_number AS month,
        SUM(revenue_usd) AS total_revenue_usd,
        SUM(cost_usd) AS total_cost_usd,
        SUM(profit_usd) AS gross_profit_usd,
        COUNT(DISTINCT order_key) AS order_count
    FROM {{ ref('stg_sales_fact') }}
    GROUP BY 1, 2
),

monthly_opex AS (
    SELECT
        fiscal_year AS year,
        fiscal_month AS month,
        SUM(CASE WHEN account_category = 'OpEx' THEN net_amount ELSE 0 END) AS total_opex_usd
    FROM {{ ref('stg_financial_summary_monthly') }}
    GROUP BY 1, 2
),

monthly_payroll AS (
    SELECT
        year,
        month,
        SUM(headcount) AS headcount,
        SUM(total_gross_pay) AS total_payroll_usd
    FROM {{ ref('stg_payroll_summary_monthly') }}
    GROUP BY 1, 2
),

monthly_support AS (
    SELECT
        year,
        month,
        ticket_count AS support_tickets,
        avg_satisfaction
    FROM {{ ref('stg_support_metrics_monthly') }}
)

SELECT
    s.year,
    s.month,
    ROUND(s.total_revenue_usd, 2) AS total_revenue_usd,
    ROUND(s.total_cost_usd, 2) AS total_cost_usd,
    ROUND(s.gross_profit_usd, 2) AS gross_profit_usd,
    ROUND(SAFE_DIVIDE(s.gross_profit_usd, NULLIF(s.total_revenue_usd, 0)) * 100, 2) AS gross_margin_pct,
    ROUND(COALESCE(o.total_opex_usd, 0), 2) AS total_opex_usd,
    COALESCE(p.headcount, 0) AS headcount,
    ROUND(COALESCE(p.total_payroll_usd, 0), 2) AS total_payroll_usd,
    ROUND(SAFE_DIVIDE(s.total_revenue_usd, NULLIF(p.headcount, 0)), 2) AS revenue_per_employee,
    COALESCE(sup.support_tickets, 0) AS support_tickets,
    sup.avg_satisfaction,
    s.order_count
FROM monthly_sales s
LEFT JOIN monthly_opex o ON s.year = o.year AND s.month = o.month
LEFT JOIN monthly_payroll p ON s.year = p.year AND s.month = p.month
LEFT JOIN monthly_support sup ON s.year = sup.year AND s.month = sup.month
