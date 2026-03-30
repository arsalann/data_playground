/* @bruin

name: contoso_staging.financial_summary_monthly
type: bq.sql
description: |
  Monthly financial summary aggregating GL journal entries against departmental budgets for Contoso's consumer electronics retail business.

  This asset consolidates actual financial performance (debits/credits) with budgeted amounts to enable variance analysis across 7 business units:
  Sales, HR, Finance, Marketing, Engineering, Operations, and Customer Support. Used as the primary data source for P&L reporting,
  budget variance analysis, and executive financial dashboards.

  Key transformations include:
  - Deduplication of source data using latest extracted_at timestamps to handle append-only raw tables
  - Monthly aggregation of journal entries by department and account code
  - Account categorization based on GL coding standards (4xxx=Revenue, 5xxx=COGS, 6xxx=OpEx, 1xxx=Assets)
  - Budget-to-actual variance calculations with percentage variance for performance tracking

  Data characteristics:
  - Covers fiscal years 2015-2025+ with ~15K monthly department/account combinations
  - Budget data may be sparse (not all accounts have budgets) resulting in nulls for budget_amount and variance_pct
  - Balances are in USD; journal entries inherently balance (total_debit ≈ total_credit across all entries)
  - Account codes follow standard GL hierarchy with 4-digit numeric codes
connection: gcp-default
tags:
  - finance
  - staging
  - fact_table
  - monthly
  - budget_analysis
  - internal
  - financial_reporting

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_raw.gl_journal_entries
  - contoso_raw.budgets
  - contoso_raw.departments


columns:
  - name: fiscal_year
    type: INTEGER
    description: Fiscal year (e.g., 2024). Primary key component for temporal partitioning.
    primary_key: true
    nullable: false
  - name: fiscal_month
    type: INTEGER
    description: Fiscal month (1-12). Primary key component representing calendar month within fiscal year.
    primary_key: true
    nullable: false
  - name: department_key
    type: INTEGER
    description: Department identifier (1-12). Primary key component referencing contoso_raw.departments. Maps to business units like Sales, Finance, Marketing.
    primary_key: true
    nullable: false
  - name: account_code
    type: VARCHAR
    description: 4-digit GL account code. Primary key component following standard chart of accounts (4xxx=Revenue, 5xxx=COGS, 6xxx=OpEx, 1xxx=Assets).
    primary_key: true
    nullable: false
  - name: department_name
    type: VARCHAR
    description: Human-readable department name (e.g., 'Sales', 'Marketing'). Denormalized from departments dimension for reporting convenience.
  - name: account_name
    type: VARCHAR
    description: Human-readable GL account name (e.g., 'Product Revenue', 'Office Rent'). Provides business context for account codes.
  - name: account_category
    type: VARCHAR
    description: High-level financial statement category derived from account_code. Values are 'Revenue', 'COGS', 'OpEx', 'Assets', or 'Other'.
  - name: total_debit
    type: DOUBLE
    description: Sum of all debit amounts in USD for this department/account/month. Always non-negative, represents left-side journal entries.
  - name: total_credit
    type: DOUBLE
    description: Sum of all credit amounts in USD for this department/account/month. Always non-negative, represents right-side journal entries.
  - name: net_amount
    type: DOUBLE
    description: Net financial impact calculated as (total_debit - total_credit). Positive values represent net debits, negative values represent net credits. Used for P&L line item amounts.
  - name: budget_amount
    type: DOUBLE
    description: Budgeted amount in USD from annual planning process. Null when no budget exists for this department/account combination (~17% of rows). Represents planned spending/revenue targets.
  - name: variance
    type: DOUBLE
    description: Budget variance calculated as (net_amount - budget_amount). Positive values indicate over-budget performance, negative values indicate under-budget. Null when budget_amount is null.
  - name: variance_pct
    type: DOUBLE
    description: Variance as percentage of budget calculated as ((net_amount - budget_amount) / budget_amount * 100). Null when budget_amount is null or zero. Used for performance dashboards and exception reporting.

@bruin */

WITH gl_deduped AS (
    SELECT *
    FROM contoso_raw.gl_journal_entries
    WHERE journal_entry_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY journal_entry_key
        ORDER BY extracted_at DESC
    ) = 1
),

budgets_deduped AS (
    SELECT *
    FROM contoso_raw.budgets
    WHERE budget_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY budget_key
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
ORDER BY fiscal_year, fiscal_month, department_key, account_code
