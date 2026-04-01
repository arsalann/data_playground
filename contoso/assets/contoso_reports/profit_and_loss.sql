/* @bruin

name: contoso_reports.profit_and_loss
type: bq.sql
description: |
  Monthly profit and loss statement for Contoso's consumer electronics retail operations,
  formatted as a standardized financial reporting table suitable for executive dashboards
  and regulatory compliance. This asset consolidates revenue and cost data from sales
  operations with operational expense data from departmental budgets to provide a
  comprehensive monthly financial performance view.

  The P&L follows standard financial statement structure:
  - Revenue (top-line from sales transactions)
  - Cost of Goods Sold (product costs from sales)
  - Gross Profit (Revenue minus COGS)
  - Operating Expenses (departmental OpEx with budget comparison)
  - Operating Income (bottom-line profitability)

  Key business insights:
  - Revenue represents actual sales across all channels and geographies in USD
  - COGS includes direct product costs, returns as negative values for proper P&L presentation
  - OpEx line items are prefixed with "  " (double space) for visual indentation in reports
  - Budget data only exists for operational expenses; revenue and COGS are actual-only
  - Variance calculations highlight budget performance for controllable expenses
  - Sort_order enables proper financial statement display order regardless of data insertion sequence

  Data lineage and transformations:
  - Revenue and COGS sourced from sales_fact (actual transactions)
  - Operating expenses sourced from financial_summary_monthly (GL actuals vs budget)
  - All amounts normalized to USD for consistent reporting
  - Variance calculations use SAFE_DIVIDE to handle division by zero gracefully
  - Budget amounts remain NULL for line items without budgeted targets

  Operational characteristics:
  - Refreshed daily via create+replace to reflect latest financial data
  - Covers ~149 months of financial history (2015-2025+) with 14 distinct line items
  - Budget variance analysis limited to OpEx categories where budgets exist
  - Supports drill-down analysis through upstream staging tables
  - Used as primary data source for executive financial reporting and board presentations
connection: bruin-playground-eu
tags:
  - finance
  - reports
  - financial_reporting
  - profit_and_loss
  - monthly
  - executive_reporting
  - budget_variance
  - internal

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_staging.financial_summary_monthly
  - contoso_staging.sales_fact


columns:
  - name: year
    type: INTEGER
    description: Fiscal year for the financial period (e.g., 2024). Forms part of primary key for temporal partitioning and enables year-over-year analysis.
    primary_key: true
  - name: month
    type: INTEGER
    description: Fiscal month number (1-12) within the fiscal year. Forms part of primary key enabling month-over-month trending and seasonal analysis.
    primary_key: true
  - name: line_item
    type: VARCHAR
    description: |
      Standardized P&L line item name following financial reporting conventions. Values include:
      'Revenue', 'Cost of Goods Sold', 'Gross Profit', 'Total Operating Expenses', 'Operating Income',
      and specific OpEx categories (prefixed with "  " for visual indentation). Forms part of primary key
      ensuring one record per line item per month. OpEx categories reflect departmental spending patterns
      and account hierarchy from the GL chart of accounts.
    primary_key: true
  - name: sort_order
    type: INTEGER
    description: |
      Display sequence number for proper P&L statement ordering (1-100). Revenue=1, COGS=2, Gross Profit=3,
      OpEx items=10-89 (derived from GL account codes), Total OpEx=90, Operating Income=100. Ensures
      consistent financial statement presentation regardless of data processing order.
  - name: amount
    type: DOUBLE
    description: |
      Financial amount in USD rounded to 2 decimal places. Positive values represent income/favorable
      variances, negative values represent expenses/unfavorable variances. Revenue and gross profit
      are positive, COGS and operating expenses are negative, following standard P&L sign conventions.
      Derived from actual transaction data (revenue/COGS) or GL journal entries (OpEx).
  - name: budget_amount
    type: DOUBLE
    description: |
      Budgeted amount in USD rounded to 2 decimal places, nullable. Only populated for operational
      expense line items where departmental budgets exist. NULL for revenue, COGS, and gross profit
      as these represent actual performance without budgeted targets. Enables budget-to-actual
      variance analysis for controllable expenses.
  - name: variance
    type: DOUBLE
    description: |
      Budget variance calculated as actual minus budget in USD (amount - budget_amount). Positive
      values indicate favorable performance (revenue exceeds budget or expenses under budget).
      Automatically computed as zero when budget_amount is NULL (for non-budgeted line items).
      Used for budget performance tracking and management reporting.
  - name: variance_pct
    type: DOUBLE
    description: |-
      Budget variance as percentage of budget ((amount - budget_amount) / budget_amount * 100).
      NULL when budget_amount is zero or NULL. Negative percentages indicate unfavorable performance.
      Uses SAFE_DIVIDE to prevent division by zero errors. Enables percentage-based budget performance
      analysis and benchmarking across different spending categories and time periods.

@bruin */

WITH revenue AS (
    SELECT
        year,
        month_number AS month,
        ROUND(SUM(revenue_usd), 2) AS amount
    FROM contoso_staging.sales_fact
    GROUP BY 1, 2
),

cogs AS (
    SELECT
        year,
        month_number AS month,
        ROUND(SUM(cost_usd), 2) AS amount
    FROM contoso_staging.sales_fact
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
    FROM contoso_staging.financial_summary_monthly
    WHERE account_category = 'OpEx'
    GROUP BY 1, 2, 3, 4
),

total_opex AS (
    SELECT year, month, SUM(amount) AS amount, SUM(budget_amount) AS budget_amount
    FROM opex_by_account
    GROUP BY 1, 2
),

pnl_lines AS (
    -- Revenue
    SELECT r.year, r.month, 'Revenue' AS line_item, 1 AS sort_order,
           r.amount, NULL AS budget_amount
    FROM revenue r

    UNION ALL

    -- COGS
    SELECT c.year, c.month, 'Cost of Goods Sold', 2,
           -c.amount, NULL
    FROM cogs c

    UNION ALL

    -- Gross Profit
    SELECT r.year, r.month, 'Gross Profit', 3,
           r.amount - c.amount, NULL
    FROM revenue r
    INNER JOIN cogs c ON r.year = c.year AND r.month = c.month

    UNION ALL

    -- OpEx line items
    SELECT year, month,
           CONCAT('  ', account_name) AS line_item,
           10 + CAST(SUBSTR(account_code, 1, 2) AS INT64) AS sort_order,
           -amount, budget_amount
    FROM opex_by_account

    UNION ALL

    -- Total OpEx
    SELECT year, month, 'Total Operating Expenses', 90,
           -amount, budget_amount
    FROM total_opex

    UNION ALL

    -- Operating Income
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
ORDER BY year, month, sort_order
