{{ config(materialized='table') }}

WITH gl_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'gl_journal_entries') }}
    WHERE journal_entry_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY journal_entry_key
        ORDER BY extracted_at DESC
    ) = 1
),

budgets_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'budgets') }}
    WHERE budget_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY budget_key
        ORDER BY extracted_at DESC
    ) = 1
),

departments_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'departments') }}
    WHERE department_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY department_key
        ORDER BY extracted_at DESC
    ) = 1
),

gl_monthly AS (
    SELECT
        EXTRACT(YEAR FROM entry_date) AS fiscal_year,
        EXTRACT(MONTH FROM entry_date) AS fiscal_month,
        department_key,
        account_code,
        account_name,
        SUM(debit_amount) AS total_debit,
        SUM(credit_amount) AS total_credit,
        SUM(debit_amount) - SUM(credit_amount) AS net_amount
    FROM gl_deduped
    GROUP BY 1, 2, 3, 4, 5
),

budget_monthly AS (
    SELECT
        fiscal_year,
        fiscal_month,
        department_key,
        account_code,
        account_name,
        SUM(budget_amount) AS budget_amount
    FROM budgets_deduped
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    COALESCE(g.fiscal_year, b.fiscal_year) AS fiscal_year,
    COALESCE(g.fiscal_month, b.fiscal_month) AS fiscal_month,
    COALESCE(g.department_key, b.department_key) AS department_key,
    COALESCE(g.account_code, b.account_code) AS account_code,
    d.department_name,
    COALESCE(g.account_name, b.account_name) AS account_name,
    CASE
        WHEN COALESCE(g.account_code, b.account_code) LIKE '4%' THEN 'Revenue'
        WHEN COALESCE(g.account_code, b.account_code) LIKE '5%' THEN 'COGS'
        WHEN COALESCE(g.account_code, b.account_code) LIKE '6%' THEN 'OpEx'
        WHEN COALESCE(g.account_code, b.account_code) LIKE '1%' THEN 'Assets'
        ELSE 'Other'
    END AS account_category,
    COALESCE(g.total_debit, 0) AS total_debit,
    COALESCE(g.total_credit, 0) AS total_credit,
    COALESCE(g.net_amount, 0) AS net_amount,
    b.budget_amount,
    ROUND(COALESCE(g.net_amount, 0) - COALESCE(b.budget_amount, 0), 2) AS variance,
    ROUND(
        SAFE_DIVIDE(
            COALESCE(g.net_amount, 0) - COALESCE(b.budget_amount, 0),
            NULLIF(b.budget_amount, 0)
        ) * 100,
        2
    ) AS variance_pct
FROM gl_monthly g
FULL OUTER JOIN budget_monthly b
    ON g.fiscal_year = b.fiscal_year
    AND g.fiscal_month = b.fiscal_month
    AND g.department_key = b.department_key
    AND g.account_code = b.account_code
LEFT JOIN departments_deduped d
    ON COALESCE(g.department_key, b.department_key) = d.department_key
