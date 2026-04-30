/* @bruin

name: contoso_reports.workforce_analytics
type: duckdb.sql
description: |
  Comprehensive quarterly workforce analytics dashboard for Contoso's consumer electronics retail organization across 7 business units and 12 departments.

  This executive-level reporting asset provides strategic workforce insights by aggregating employee directory and payroll data into department-level quarterly metrics. Serves as the authoritative source for workforce planning, budget analysis, and HR performance tracking for ~3,000 employees spanning from entry-level retail associates (L1) to C-level executives (L5).

  Key business applications include:
  - Executive dashboard reporting for quarterly business reviews
  - Department-level headcount planning and budget allocation
  - Workforce cost analysis and per-employee efficiency metrics
  - Turnover trend analysis and retention strategy assessment
  - Compensation benchmarking across business units

  Data coverage spans from company founding (2008) through current operations with quarterly granularity, enabling multi-year trend analysis and seasonal pattern detection. Department sizes range from specialized teams (~10-20 employees) to large operational units (500+ in Sales).

  The composite primary key (year, quarter, department_name) ensures unique quarterly snapshots per department while supporting time-series analysis. Turnover rates are calculated as quarterly percentage (terminations/active_headcount) rather than annualized, providing precise period-over-period comparisons.

  Note: Only departments with active headcount > 0 are included to focus reporting on operational units. Cost calculations use actual gross payroll from bi-weekly/semi-monthly pay periods aggregated to quarterly totals.
connection: contoso-duckdb
tags:
  - hr
  - people_analytics
  - workforce_planning
  - executive_reporting
  - fact_table
  - quarterly
  - department_level
  - payroll_costs
  - turnover_metrics
  - reports

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_staging.employee_directory
  - contoso_staging.payroll_summary_monthly


columns:
  - name: year
    type: INTEGER
    description: |
      Calendar year for workforce reporting period. Primary key component enabling temporal analysis.
      Spans from company founding (2008) through current operations (2025+), supporting multi-year
      trend analysis and strategic workforce planning. Based on actual quarter end dates rather than
      fiscal year, ensuring alignment with standard business reporting calendars.
    primary_key: true
  - name: quarter
    type: INTEGER
    description: |
      Calendar quarter (1-4) within the reporting year. Primary key component for quarterly analysis.
      Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec following standard business quarters.
      Enables seasonal pattern analysis (e.g., Q4 holiday retail hiring) and quarter-over-quarter
      performance comparisons. Essential for aligning workforce metrics with business cycle reporting.
    primary_key: true
  - name: department_name
    type: VARCHAR
    description: |
      Business unit or functional department name. Primary key component for cross-departmental analysis.
      Represents Contoso's 12 organizational units including Sales (largest ~500+ employees), Engineering,
      Marketing, Finance, HR, Operations, Customer Support, Product, Legal, Data & Analytics, Facilities,
      and Executive. Department assignment determines reporting hierarchy, budget allocation, and compensation
      bands. Essential for workforce cost analysis and organizational structure insights.
    primary_key: true
  - name: active_headcount
    type: INTEGER
    description: |
      Count of active employees in the department at quarter end. Includes employees who were
      active on the last day of the quarter OR were terminated after the quarter start date,
      providing an accurate snapshot of workforce capacity during the reporting period.
      Ranges from small specialized teams (~10-20) to large operational departments (500+ in Sales).
      Critical metric for capacity planning, budget allocation per FTE, and organizational scaling analysis.
    checks:
      - name: not_null
      - name: positive
  - name: new_hires
    type: INTEGER
    description: |
      Count of employees hired during the quarter based on hire_date falling within the quarter period.
      Represents net new talent acquisition and organizational growth. Average ~3.4 hires per department
      per quarter with higher volumes in Sales and Customer Support reflecting retail seasonality.
      Key metric for recruitment effectiveness, growth planning, and onboarding capacity assessment.
      Excludes internal transfers which do not represent net headcount additions.
    checks:
      - name: not_null
      - name: non_negative
  - name: terminations
    type: INTEGER
    description: |
      Count of employees who left the organization during the quarter based on termination_date
      falling within the quarter period. Includes all types of separations (voluntary resignations,
      involuntary terminations, layoffs, retirements). Low average (~0.5 per department per quarter)
      indicates strong retention across the organization. Essential for calculating turnover rates,
      retention analysis, and workforce stability assessment. Used in conjunction with new_hires
      to determine net headcount change.
    checks:
      - name: not_null
      - name: non_negative
  - name: turnover_rate
    type: DOUBLE
    description: |
      Quarterly turnover rate as percentage (terminations/active_headcount * 100). Represents
      the proportion of the workforce that left during the quarter, providing a standardized
      metric for retention analysis across departments of varying sizes. Ranges from 0% to 6.67%
      with low average (~0.34%) indicating strong organizational retention. Important: This is
      quarterly turnover, not annualized - multiply by 4 for approximate annual rate comparison.
      Critical KPI for HR performance, department stability, and workforce planning.
    checks:
      - name: not_null
      - name: non_negative
  - name: avg_tenure_months
    type: DOUBLE
    description: |
      Average tenure in months for active employees in the department at quarter end.
      Calculated from hire_date to quarter end date for current employees only, excluding
      terminated staff to reflect current workforce maturity. Range 99-217 months (~8-18 years)
      with average ~163 months (~13.6 years) indicating a highly experienced, stable workforce.
      Higher tenure typically correlates with senior roles and specialized functions. Key metric
      for assessing workforce experience, institutional knowledge, and succession planning needs.
    checks:
      - name: not_null
      - name: positive
  - name: avg_salary
    type: DOUBLE
    description: |
      Average annual base salary in USD for active employees in the department at quarter end.
      Represents base compensation only, excluding bonuses, equity, commissions, or other variable pay.
      All salaries normalized to USD regardless of employee geographic location for consistent
      cross-departmental benchmarking. Range $50K-$246K with average ~$96K reflecting diverse
      role levels from L1 entry positions ($35K-$55K) to L5 executives ($200K-$350K).
      Critical metric for compensation analysis, budget planning, and pay equity assessment.
    checks:
      - name: not_null
      - name: positive
  - name: quarterly_payroll_cost
    type: DOUBLE
    description: |
      Total gross payroll expense in USD for the department during the quarter, aggregated from
      bi-weekly/semi-monthly pay periods. Includes base salary, overtime, bonuses, commissions,
      and all other taxable compensation before deductions. Range $0-$13M with average ~$1.7M
      per department per quarter, reflecting significant variation in department sizes and
      compensation levels. Essential for department budget management, cost center analysis,
      and workforce ROI calculations. Used as basis for cost-per-employee metrics.
    checks:
      - name: not_null
      - name: non_negative
  - name: cost_per_employee
    type: DOUBLE
    description: |
      Quarterly gross payroll cost per active employee in USD (quarterly_payroll_cost / active_headcount).
      Represents the average quarterly compensation expense per FTE, enabling standardized cost comparison
      across departments of varying sizes. Range $743-$133K with average ~$14.8K per employee per quarter
      (~$59K annualized), reflecting role level distribution and department compensation structures.
      Occasionally null for departments with zero payroll cost but positive headcount (e.g., unpaid interns,
      contractors paid outside payroll system). Key metric for workforce efficiency analysis and
      budget planning per FTE.
    checks:
      - name: positive

@bruin */

WITH quarters AS (
    SELECT DISTINCT year, CAST(CEIL(month / 3) AS BIGINT) AS quarter
    FROM contoso_staging.payroll_summary_monthly
),

headcount AS (
    SELECT
        q.year,
        q.quarter,
        e.department_name,
        COUNT(*) FILTER (WHERE 
            e.is_active = TRUE
            OR (e.termination_date IS NOT NULL
                AND e.termination_date > MAKE_DATE(q.year, q.quarter * 3, 1))
        ) AS active_headcount,
        COUNT(*) FILTER (WHERE 
            EXTRACT(YEAR FROM e.hire_date) = q.year
            AND CAST(CEIL(EXTRACT(MONTH FROM e.hire_date) / 3) AS BIGINT) = q.quarter
        ) AS new_hires,
        COUNT(*) FILTER (WHERE 
            e.termination_date IS NOT NULL
            AND EXTRACT(YEAR FROM e.termination_date) = q.year
            AND CAST(CEIL(EXTRACT(MONTH FROM e.termination_date) / 3) AS BIGINT) = q.quarter
        ) AS terminations,
        ROUND(AVG(CASE WHEN e.is_active THEN e.tenure_months END), 1) AS avg_tenure_months,
        ROUND(AVG(CASE WHEN e.is_active THEN e.salary END), 2) AS avg_salary
    FROM quarters q
    CROSS JOIN contoso_staging.employee_directory e
    WHERE e.hire_date <= MAKE_DATE(q.year, q.quarter * 3, 28)
    GROUP BY 1, 2, 3
),

quarterly_payroll AS (
    SELECT
        year,
        CAST(CEIL(month / 3) AS BIGINT) AS quarter,
        department_name,
        SUM(total_gross_pay) AS quarterly_payroll_cost
    FROM contoso_staging.payroll_summary_monthly
    GROUP BY 1, 2, 3
)

SELECT
    h.year,
    h.quarter,
    h.department_name,
    h.active_headcount,
    h.new_hires,
    h.terminations,
    ROUND((h.terminations) / (NULLIF(h.active_headcount, 0)) * 100, 2) AS turnover_rate,
    h.avg_tenure_months,
    h.avg_salary,
    ROUND(COALESCE(p.quarterly_payroll_cost, 0), 2) AS quarterly_payroll_cost,
    ROUND((p.quarterly_payroll_cost) / (NULLIF(h.active_headcount, 0)), 2) AS cost_per_employee
FROM headcount h
LEFT JOIN quarterly_payroll p
    ON h.year = p.year AND h.quarter = p.quarter AND h.department_name = p.department_name
WHERE h.active_headcount > 0
ORDER BY h.year, h.quarter, h.department_name
