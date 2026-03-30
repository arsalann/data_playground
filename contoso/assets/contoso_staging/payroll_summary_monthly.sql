/* @bruin

name: contoso_staging.payroll_summary_monthly
type: bq.sql
description: |
  Monthly payroll summary aggregating employee compensation data across Contoso's 7 business units for workforce analytics and financial reporting.

  This asset consolidates individual payroll records into department-level monthly summaries, providing executive-level visibility into
  compensation costs, headcount trends, and per-employee metrics across Sales, HR, Finance, Marketing, Engineering, Operations, and
  Customer Support departments. Serves as the primary data source for workforce cost analysis, budget planning, and executive dashboards.

  Key transformations include:
  - Deduplication of source tables using latest extracted_at timestamps to handle append-only raw data patterns
  - Monthly aggregation from bi-weekly/semi-monthly pay periods using pay_period_start dates
  - Department-level rollups with both absolute totals and per-employee averages
  - Inner join to employees ensures only current/valid employees are included; departments without employees excluded via LEFT JOIN

  Data characteristics:
  - Covers ~2.5K rows spanning multiple years with consistent monthly cadence
  - Department headcount ranges from small specialized teams (~10-20) to large operational departments (500+ in Sales)
  - Average monthly gross pay per employee ranges from ~$1.5K to ~$8.8K reflecting diverse role levels and geographic markets
  - Deductions typically represent ~30% of gross pay (taxes, benefits, retirement contributions)
  - Data completeness is high with no null values in core metrics, indicating robust payroll processing

  Business context: Used for monthly workforce reporting, budget variance analysis, compensation benchmarking, and strategic workforce planning.
connection: gcp-default
tags:
  - hr
  - payroll
  - people_analytics
  - fact_table
  - monthly
  - workforce_costs
  - internal
  - financial_reporting
  - staging

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_raw.payroll
  - contoso_raw.employees
  - contoso_raw.departments


columns:
  - name: year
    type: INTEGER
    description: |
      Calendar year extracted from pay_period_start. Primary key component for temporal partitioning.
      Based on actual pay period dates rather than processing dates, ensuring accurate monthly attribution
      of compensation expenses. Typically ranges from 2007-2026+ in the dataset.
    primary_key: true
    checks:
      - name: not_null
  - name: month
    type: INTEGER
    description: |
      Calendar month (1-12) extracted from pay_period_start. Primary key component representing
      the month within the fiscal year. Enables monthly trending analysis and seasonal pattern detection
      in workforce costs and headcount changes.
    primary_key: true
    checks:
      - name: not_null
      - name: accepted_values
  - name: department_key
    type: INTEGER
    description: |
      Department identifier (1-12) referencing contoso_raw.departments. Primary key component enabling
      cross-departmental analysis. Maps to Contoso's 7 core business units plus support functions.
      Used for joining to department hierarchies and budget allocations.
    primary_key: true
    checks:
      - name: not_null
  - name: department_name
    type: VARCHAR
    description: |
      Human-readable department name (e.g., 'Sales', 'Engineering', 'Customer Support'). Denormalized from
      departments dimension for reporting convenience. Enables intuitive filtering and grouping in dashboards
      without requiring additional joins. Typically 5-16 characters reflecting business unit names.
  - name: headcount
    type: INTEGER
    description: |
      Total count of distinct employees who received pay in this department during the month.
      Calculated using COUNT(DISTINCT employee_key) to handle cases where employees may have multiple
      pay records per month. Represents active workforce size for capacity planning and cost-per-employee
      analysis. Ranges from small teams (~10-20) to large operational departments (500+).
    checks:
      - name: not_null
      - name: positive
  - name: total_gross_pay
    type: DOUBLE
    description: |
      Sum of gross compensation in USD for all employees in this department/month before any deductions.
      Includes base salary, overtime, bonuses, commissions, and other taxable compensation.
      Rounded to 2 decimal places for financial precision. Used for budget variance analysis and
      department cost center reporting. Typical range: $1.5K-$4.4M per department per month.
    checks:
      - name: not_null
      - name: non_negative
  - name: total_deductions
    type: DOUBLE
    description: |
      Sum of all payroll deductions in USD including federal/state taxes, Social Security, Medicare,
      health insurance premiums, retirement contributions, and other voluntary deductions.
      Typically represents ~30% of gross pay. Rounded to 2 decimal places. Used for net cost analysis
      and benefits administration reporting. Range: $400-$1.3M per department per month.
    checks:
      - name: not_null
      - name: non_negative
  - name: total_net_pay
    type: DOUBLE
    description: |
      Sum of net take-home pay in USD (gross_pay - deductions) disbursed to employees via direct deposit
      or check. Represents actual cash outflow for payroll disbursements. Rounded to 2 decimal places.
      Used for cash flow forecasting and payroll funding analysis. Range: $1K-$3.1M per department per month.
    checks:
      - name: not_null
      - name: non_negative
  - name: avg_gross_per_employee
    type: DOUBLE
    description: |
      Average monthly gross pay per employee in USD (total_gross_pay / headcount). Key metric for
      compensation benchmarking, budget planning per FTE, and cross-departmental cost comparison.
      Reflects role levels, geographic markets, and department-specific compensation strategies.
      Calculated using SAFE_DIVIDE to handle edge cases. Range: ~$1.5K-$8.8K per employee per month.
    checks:
      - name: not_null
      - name: non_negative

custom_checks:
  - name: payroll_math_validation
    description: Verify that total_net_pay approximately equals total_gross_pay minus total_deductions
    value: 0
    count: 0
    query: |
      SELECT *
      FROM {{ this }}
      WHERE ABS(total_net_pay - (total_gross_pay - total_deductions)) > 0.01
  - name: reasonable_deduction_rate
    description: Ensure deduction rate is within reasonable bounds (15-50% of gross pay)
    value: 0
    count: 0
    query: |
      SELECT *
      FROM {{ this }}
      WHERE total_deductions / NULLIF(total_gross_pay, 0) NOT BETWEEN 0.15 AND 0.50

@bruin */

WITH payroll_deduped AS (
    SELECT *
    FROM contoso_raw.payroll
    WHERE payroll_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payroll_key
        ORDER BY extracted_at DESC
    ) = 1
),

employees_deduped AS (
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
ORDER BY year, month, department_key
