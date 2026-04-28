{{ config(materialized='table') }}

WITH payroll_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'payroll') }}
    WHERE payroll_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payroll_key
        ORDER BY extracted_at DESC
    ) = 1
),

employees_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'employees') }}
    WHERE employee_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY employee_key
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
)

SELECT
    EXTRACT(YEAR FROM p.pay_period_start) AS year,
    EXTRACT(MONTH FROM p.pay_period_start) AS month,
    e.department_key,
    d.department_name,
    COUNT(DISTINCT p.employee_key) AS headcount,
    ROUND(SUM(p.gross_pay), 2) AS total_gross_pay,
    ROUND(SUM(p.deductions), 2) AS total_deductions,
    ROUND(SUM(p.net_pay), 2) AS total_net_pay,
    ROUND(SAFE_DIVIDE(SUM(p.gross_pay), COUNT(DISTINCT p.employee_key)), 2) AS avg_gross_per_employee
FROM payroll_deduped p
INNER JOIN employees_deduped e ON p.employee_key = e.employee_key
LEFT JOIN departments_deduped d ON e.department_key = d.department_key
GROUP BY 1, 2, 3, 4
