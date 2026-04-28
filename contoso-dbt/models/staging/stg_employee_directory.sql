{{ config(materialized='table') }}

WITH employees_deduped AS (
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
),

stores_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'stores') }}
    WHERE store_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY store_key
        ORDER BY extracted_at DESC
    ) = 1
)

SELECT
    e.employee_key,
    CONCAT(e.first_name, ' ', e.last_name) AS full_name,
    e.email,
    e.job_title,
    e.level,
    d.department_name,
    st.country_name AS store_country,
    CAST(e.hire_date AS DATE) AS hire_date,
    CAST(e.termination_date AS DATE) AS termination_date,
    DATE_DIFF(
        COALESCE(CAST(e.termination_date AS DATE), CURRENT_DATE()),
        CAST(e.hire_date AS DATE),
        MONTH
    ) AS tenure_months,
    e.status = 'Active' AS is_active,
    e.salary,
    CONCAT(m.first_name, ' ', m.last_name) AS manager_name
FROM employees_deduped e
LEFT JOIN departments_deduped d ON e.department_key = d.department_key
LEFT JOIN stores_deduped st ON e.store_key = st.store_key
LEFT JOIN employees_deduped m ON e.manager_key = m.employee_key
