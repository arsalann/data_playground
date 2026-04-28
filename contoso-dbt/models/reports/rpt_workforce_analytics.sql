{{ config(materialized='table') }}

WITH quarters AS (
    SELECT DISTINCT year, CAST(CEIL(month / 3) AS INT64) AS quarter
    FROM {{ ref('stg_payroll_summary_monthly') }}
),

headcount AS (
    SELECT
        q.year,
        q.quarter,
        e.department_name,
        COUNTIF(
            e.is_active = TRUE
            OR (e.termination_date IS NOT NULL
                AND e.termination_date > DATE(q.year, q.quarter * 3, 1))
        ) AS active_headcount,
        COUNTIF(
            EXTRACT(YEAR FROM e.hire_date) = q.year
            AND CAST(CEIL(EXTRACT(MONTH FROM e.hire_date) / 3) AS INT64) = q.quarter
        ) AS new_hires,
        COUNTIF(
            e.termination_date IS NOT NULL
            AND EXTRACT(YEAR FROM e.termination_date) = q.year
            AND CAST(CEIL(EXTRACT(MONTH FROM e.termination_date) / 3) AS INT64) = q.quarter
        ) AS terminations,
        ROUND(AVG(CASE WHEN e.is_active THEN e.tenure_months END), 1) AS avg_tenure_months,
        ROUND(AVG(CASE WHEN e.is_active THEN e.salary END), 2) AS avg_salary
    FROM quarters q
    CROSS JOIN {{ ref('stg_employee_directory') }} e
    WHERE e.hire_date <= DATE(q.year, q.quarter * 3, 28)
    GROUP BY 1, 2, 3
),

quarterly_payroll AS (
    SELECT
        year,
        CAST(CEIL(month / 3) AS INT64) AS quarter,
        department_name,
        SUM(total_gross_pay) AS quarterly_payroll_cost
    FROM {{ ref('stg_payroll_summary_monthly') }}
    GROUP BY 1, 2, 3
)

SELECT
    h.year,
    h.quarter,
    h.department_name,
    h.active_headcount,
    h.new_hires,
    h.terminations,
    ROUND(SAFE_DIVIDE(h.terminations, NULLIF(h.active_headcount, 0)) * 100, 2) AS turnover_rate,
    h.avg_tenure_months,
    h.avg_salary,
    ROUND(COALESCE(p.quarterly_payroll_cost, 0), 2) AS quarterly_payroll_cost,
    ROUND(SAFE_DIVIDE(p.quarterly_payroll_cost, NULLIF(h.active_headcount, 0)), 2) AS cost_per_employee
FROM headcount h
LEFT JOIN quarterly_payroll p
    ON h.year = p.year AND h.quarter = p.quarter AND h.department_name = p.department_name
WHERE h.active_headcount > 0
