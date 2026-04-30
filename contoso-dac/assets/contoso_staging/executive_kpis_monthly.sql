/* @bruin

name: contoso_staging.executive_kpis_monthly
type: duckdb.sql
description: |
  Executive dashboard monthly KPI rollup combining key metrics across all business units.

  This is the primary executive reporting table aggregating sales performance, financial health,
  operational efficiency, and customer satisfaction metrics for Contoso's monthly board reporting.
  Each row represents a complete month of business performance with metrics derived from sales
  transactions, GL journal entries, payroll records, and support ticket data.

  The table joins monthly sales facts with operational expense summaries, payroll costs, and
  customer support metrics to provide a unified view of business performance. Revenue and costs
  are converted to USD for consistent reporting. Headcount reflects employees who received
  payroll during the month.

  Key business insights: gross margin percentage shows profitability trends, revenue per
  employee indicates productivity, and support ticket volume with satisfaction scores measure
  customer experience quality. OpEx includes all operating expenses excluding payroll.

  Used for: monthly board reports, executive dashboards, trend analysis, and variance reporting.
connection: contoso-duckdb
tags:
  - executive_reporting
  - finance
  - kpi_dashboard
  - monthly_aggregation
  - cross_department
  - fact_table
  - board_reporting
  - performance_metrics
  - staging

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_staging.sales_fact
  - contoso_staging.financial_summary_monthly
  - contoso_staging.marketing_performance
  - contoso_staging.support_metrics_monthly
  - contoso_staging.payroll_summary_monthly


columns:
  - name: year
    type: INTEGER
    description: Calendar year (e.g., 2021, 2022) - fiscal year aligns with calendar year for Contoso
    primary_key: true
    checks:
      - name: not_null
  - name: month
    type: INTEGER
    description: Calendar month number (1-12), used with year to form unique time identifier
    primary_key: true
    checks:
      - name: not_null
      - name: min
        value: 1
      - name: max
        value: 12
  - name: total_revenue_usd
    type: DOUBLE
    description: Total recognized revenue in USD, sum of all order line items with currency conversion applied
    checks:
      - name: not_null
      - name: min
        value: 0
  - name: total_cost_usd
    type: DOUBLE
    description: Total cost of goods sold (COGS) in USD, direct product costs excluding labor and overhead
    checks:
      - name: not_null
      - name: min
        value: 0
  - name: gross_profit_usd
    type: DOUBLE
    description: Gross profit in USD, calculated as total_revenue_usd minus total_cost_usd
    checks:
      - name: not_null
  - name: gross_margin_pct
    type: DOUBLE
    description: Gross margin percentage (0-100), calculated as (gross_profit_usd / total_revenue_usd) * 100
    checks:
      - name: not_null
      - name: min
        value: 0
      - name: max
        value: 100
  - name: total_opex_usd
    type: DOUBLE
    description: Total operating expenses in USD excluding payroll - includes facilities, marketing, technology, and other OpEx categories from GL
    checks:
      - name: not_null
      - name: min
        value: 0
  - name: headcount
    type: INTEGER
    description: Number of employees who received payroll during the month - reflects active workforce count
    checks:
      - name: not_null
      - name: min
        value: 0
  - name: total_payroll_usd
    type: DOUBLE
    description: Total gross payroll cost in USD before deductions - includes salaries, wages, benefits, and bonuses
    checks:
      - name: not_null
      - name: min
        value: 0
  - name: revenue_per_employee
    type: DOUBLE
    description: Revenue efficiency metric in USD per employee, calculated as total_revenue_usd divided by headcount
    checks:
      - name: not_null
      - name: min
        value: 0
  - name: support_tickets
    type: INTEGER
    description: Total customer support tickets created during the month across all channels (phone, email, chat)
    checks:
      - name: not_null
      - name: min
        value: 0
  - name: avg_satisfaction
    type: DOUBLE
    description: Average customer satisfaction rating on 1-5 scale where 5 is highest satisfaction, from post-ticket surveys
    checks:
      - name: not_null
      - name: min
        value: 1
      - name: max
        value: 5
  - name: order_count
    type: INTEGER
    description: Total number of distinct orders processed during the month - measures transaction volume
    checks:
      - name: not_null
      - name: min
        value: 0

@bruin */

WITH monthly_sales AS (
    SELECT
        year,
        month_number AS month,
        SUM(revenue_usd) AS total_revenue_usd,
        SUM(cost_usd) AS total_cost_usd,
        SUM(profit_usd) AS gross_profit_usd,
        COUNT(DISTINCT order_key) AS order_count
    FROM contoso_staging.sales_fact
    GROUP BY 1, 2
),

monthly_opex AS (
    SELECT
        fiscal_year AS year,
        fiscal_month AS month,
        SUM(CASE WHEN account_category = 'OpEx' THEN net_amount ELSE 0 END) AS total_opex_usd
    FROM contoso_staging.financial_summary_monthly
    GROUP BY 1, 2
),

monthly_payroll AS (
    SELECT
        year,
        month,
        SUM(headcount) AS headcount,
        SUM(total_gross_pay) AS total_payroll_usd
    FROM contoso_staging.payroll_summary_monthly
    GROUP BY 1, 2
),

monthly_support AS (
    SELECT
        year,
        month,
        ticket_count AS support_tickets,
        avg_satisfaction
    FROM contoso_staging.support_metrics_monthly
)

SELECT
    s.year,
    s.month,
    ROUND(s.total_revenue_usd, 2) AS total_revenue_usd,
    ROUND(s.total_cost_usd, 2) AS total_cost_usd,
    ROUND(s.gross_profit_usd, 2) AS gross_profit_usd,
    ROUND((s.gross_profit_usd) / (NULLIF(s.total_revenue_usd, 0)) * 100, 2) AS gross_margin_pct,
    ROUND(COALESCE(o.total_opex_usd, 0), 2) AS total_opex_usd,
    COALESCE(p.headcount, 0) AS headcount,
    ROUND(COALESCE(p.total_payroll_usd, 0), 2) AS total_payroll_usd,
    ROUND((s.total_revenue_usd) / (NULLIF(p.headcount, 0)), 2) AS revenue_per_employee,
    COALESCE(sup.support_tickets, 0) AS support_tickets,
    sup.avg_satisfaction,
    s.order_count
FROM monthly_sales s
LEFT JOIN monthly_opex o ON s.year = o.year AND s.month = o.month
LEFT JOIN monthly_payroll p ON s.year = p.year AND s.month = p.month
LEFT JOIN monthly_support sup ON s.year = sup.year AND s.month = sup.month
ORDER BY s.year, s.month
