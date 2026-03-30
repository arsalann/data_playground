/* @bruin

name: contoso_staging.employee_directory
type: bq.sql
description: |
  Comprehensive employee directory for Contoso's consumer electronics retail organization.
  Consolidates employee data with organizational context including department affiliations,
  geographic assignments (retail stores vs headquarters), reporting relationships, and
  tenure analytics.

  This staging table serves as the authoritative source for workforce analytics, enabling
  cross-departmental analysis of headcount, retention, compensation, and organizational
  structure. Supports both current active employees and historical terminated records for
  trend analysis.

  Key transformations include tenure calculation (accounting for termination dates),
  hierarchical reporting structure through manager relationships, and geographic assignment
  logic where store-based roles (Sales, Customer Support, Operations, Facilities) are
  assigned to specific retail locations while corporate functions remain at headquarters.

  The dataset reflects Contoso's 7 business units with a leveled promotion structure (L1-L5),
  spanning from entry-level retail associates to C-level executives, with salary bands
  appropriate to each level and geographic market.
connection: gcp-default
tags:
  - hr
  - people_analytics
  - dimension_table
  - staging
  - daily_refresh
  - pii

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_raw.employees
  - contoso_raw.departments
  - contoso_raw.stores


columns:
  - name: employee_key
    type: INTEGER
    description: |
      Primary identifier for each employee record. Serves as the foreign key
      for joining to other employee-related datasets (payroll, performance, etc.).
      Unique across all employees including terminated staff to maintain referential
      integrity in historical analyses.
    primary_key: true
  - name: full_name
    type: VARCHAR
    description: |
      Concatenated first and last name of employee. Derived from separate
      first_name and last_name fields in source data. Used primarily for
      display purposes and reporting - not suitable for matching due to
      potential duplicates across the organization.
  - name: email
    type: VARCHAR
    description: |
      Corporate email address following pattern firstname.lastname@contoso.com.
      Serves as unique business identifier for authentication and communication.
      All employees receive corporate email addresses regardless of role level
      or geographic location.
  - name: job_title
    type: VARCHAR
    description: |
      Official role title within the organization. Maps to specific responsibilities
      and varies by department and level. Ranges from entry positions like
      'Sales Associate' and 'Support Agent' to executive roles like 'CEO' and 'VP'.
      90 distinct titles across the organization reflect career progression paths.
  - name: level
    type: VARCHAR
    description: |
      Standardized job level classification from L1 (entry-level) to L5 (executive).
      Determines salary bands, reporting structure, and promotion pathways.
      Distribution: L1 (35%), L2 (30%), L3 (20%), L4 (10%), L5 (5%) reflecting
      typical organizational hierarchy.
  - name: department_name
    type: VARCHAR
    description: |
      Business unit assignment reflecting employee's functional area. 12 departments
      including Sales (largest), Engineering, Marketing, Finance, HR, Operations,
      Customer Support, Product, Legal, Data & Analytics, Facilities, and Executive.
      Determines budget allocation, reporting chains, and cross-functional collaboration.
  - name: store_country
    type: VARCHAR
    description: |
      Geographic assignment for store-based employees across Contoso's 9-country
      retail presence (US, UK, Germany, France, Italy, Canada, Australia, Netherlands).
      NULL for headquarters-based roles (45% of workforce) in corporate functions.
      'Online' designation represents digital operations team members.
  - name: hire_date
    type: DATE
    description: |
      Original employment start date. Spans from 2008 (company founding) to
      present, enabling tenure and retention analysis. Used as basis for
      calculating anniversary dates, vesting schedules, and cohort analytics.
  - name: termination_date
    type: DATE
    description: |
      Employment end date for separated employees. NULL for active employees (85.6%).
      Enables turnover analysis and retention cohort studies. Dates range through
      2025 reflecting both historical separations and planned future departures.
  - name: tenure_months
    type: INTEGER
    description: |
      Calculated months of service from hire_date to termination_date (or current date
      for active employees). Accounts for actual employment duration regardless of
      status. Critical metric for retention analysis, compensation planning, and
      workforce stability assessment. Average tenure ~104 months across organization.
  - name: is_active
    type: BOOLEAN
    description: |
      Current employment status flag. TRUE for active employees (85.6%), FALSE for
      terminated staff. Derived from source 'status' field for consistent boolean
      representation. Essential for filtering current workforce vs historical records.
  - name: salary
    type: DOUBLE
    description: |
      Annual base salary in USD. Excludes bonuses, equity, or other compensation.
      Ranges from $35K-$55K (L1) to $200K-$350K (L5) reflecting level-based bands.
      All salaries normalized to USD regardless of employee geographic location
      for consistent reporting and benchmarking.
  - name: manager_name
    type: VARCHAR
    description: |-
      Direct report manager's full name for organizational hierarchy mapping.
      NULL for top-level executives and department heads (15.6% of workforce)
      who report to board or have no formal manager. Enables org chart generation
      and span-of-control analysis across management layers.

@bruin */

WITH employees_deduped AS (
    SELECT *
    FROM contoso_raw.employees
    WHERE employee_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY employee_key
        ORDER BY extracted_at DESC
    ) = 1
),

departments_deduped AS (
    SELECT *
    FROM contoso_raw.departments
    WHERE department_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY department_key
        ORDER BY extracted_at DESC
    ) = 1
),

stores_deduped AS (
    SELECT *
    FROM contoso_raw.stores
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
ORDER BY e.employee_key
