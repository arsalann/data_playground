"""@bruin

name: contoso_raw.gl_journal_entries
description: |
  Contoso general ledger journal entries representing the consumer electronics retailer's
  complete financial transaction history in double-entry bookkeeping format. Contains
  ~1.9M individual GL line items covering revenue recognition, cost of goods sold, and
  operational expenses across 12 departments spanning 2016-2026.

  This synthetic dataset simulates real-world financial accounting with balanced journal
  entries where each business transaction generates paired debit/credit entries ensuring
  sum(debit_amount) = sum(credit_amount) for each journal entry group. The data originates
  from two primary sources: sales transactions (sampled at ~20% for performance) and monthly
  departmental overhead expenses including rent, software, travel, professional services,
  office supplies, training, and depreciation.

  Key business characteristics:
  - Revenue entries: DR Accounts Receivable (1200) / CR Revenue (4000) from sales orders
  - COGS entries: DR Cost of Goods Sold (5000) / CR Inventory (1400) for sold products
  - Overhead entries: DR various 6xxx expense accounts / CR Cash (1000) for monthly OpEx
  - Uses 12 distinct GL account codes representing chart of accounts structure
  - All monetary amounts in USD with deterministic generation (seed=42)
  - Entry dates span from 2016-01-01 to 2026-12-01 covering 3,532 unique transaction dates
  - Links to contoso_raw.departments for organizational hierarchy and budget allocation

  Essential for downstream financial reporting including P&L statements, budget variance
  analysis, and departmental cost allocation in contoso_staging.financial_summary_monthly.
  Serves as the authoritative source for all GL-based financial analytics and executive
  KPI dashboards across the Contoso organization.
connection: contoso-duckdb
instance: b1.xlarge
tags:
  - domain:finance
  - domain:accounting
  - data_type:fact_table
  - data_type:transactional_data
  - sensitivity:internal
  - pipeline_role:raw
  - update_pattern:batch_replace
  - synthetic_data
  - double_entry_bookkeeping
  - financial_reporting
  - general_ledger

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: journal_entry_key
    type: INTEGER
    description: |
      Sequential journal entry line identifier (1-1,887,160) providing unique tracking
      for each debit/credit line item within the GL system. Not a true primary key as
      each business transaction creates multiple journal entries (balanced debit/credit pairs).
      Essential for data lineage, audit trails, and detailed financial reconciliation.
      References the same underlying business transaction across paired entries.
    checks:
      - name: not_null
  - name: entry_date
    type: DATE
    description: |
      Business date when the underlying transaction occurred, ranging from 2016-01-01 to
      2026-12-01 across 3,532 unique dates. For sales-derived entries, matches the original
      order date; for overhead expenses, represents the first day of the applicable month.
      Critical for period-based financial reporting, monthly closings, and fiscal year
      analysis. All dates represent when economic events occurred, not posting dates.
    checks:
      - name: not_null
  - name: account_code
    type: VARCHAR
    description: |
      4-character general ledger account code from Contoso's chart of accounts (12 unique values).
      Follows standard accounting structure: 1xxx (Assets), 4xxx (Revenue), 5xxx (COGS),
      6xxx (Operating Expenses). Key codes include: 1000 (Cash), 1200 (Accounts Receivable),
      1400 (Inventory), 4000 (Revenue), 5000 (Cost of Goods Sold), and expense accounts
      6300-6900 (Rent, Software, Travel, Services, Supplies, Training, Depreciation).
      Essential for financial statement categorization and management reporting.
    checks:
      - name: not_null
  - name: account_name
    type: VARCHAR
    description: |
      Human-readable general ledger account description corresponding to account_code
      (12 unique values, 4-22 characters). Provides business-friendly names for accounting
      categories such as "Accounts Receivable", "Cost of Goods Sold", "Professional Services".
      Used in financial reports, dashboards, and user-facing analytics where technical
      account codes would be unclear. Always paired 1:1 with account_code values.
    checks:
      - name: not_null
  - name: department_key
    type: INTEGER
    description: |
      Foreign key to contoso_raw.departments (1-12) indicating the business unit responsible
      for or benefiting from the transaction. Maps to organizational hierarchy including
      Executive (1), Sales (2), Marketing (3), Finance (4), HR (5), Engineering (6),
      Customer Support (7), Operations (8), Legal (9), Product (10), Facilities (11),
      and Data & Analytics (12). Critical for departmental cost allocation, budget variance
      analysis, and management reporting by business unit.
    checks:
      - name: not_null
  - name: description
    type: VARCHAR
    description: |
      Detailed transaction description providing context for each journal entry (20-40 characters,
      756K unique values). For sales entries, includes order key reference (e.g., "Revenue - Order 12345").
      For overhead expenses, includes department and expense type (e.g., "Monthly Software & Tools - Dept 6").
      Generated dynamically based on transaction source and attributes, enabling detailed audit
      trails and transaction-level analysis. Essential for financial investigation and reconciliation.
    checks:
      - name: not_null
  - name: debit_amount
    type: DOUBLE
    description: |
      Debit amount in USD for this journal entry line (≥ 0, max $100,000). Contains the actual
      monetary value when the account is being debited, otherwise 0. Part of double-entry
      bookkeeping where increases to assets/expenses are debited. Average value $368 with
      lognormal distribution reflecting typical business transaction patterns. Used with
      credit_amount to maintain balanced journal entries for financial integrity.
    checks:
      - name: not_null
  - name: credit_amount
    type: DOUBLE
    description: |
      Credit amount in USD for this journal entry line (≥ 0, max $100,000). Contains the actual
      monetary value when the account is being credited, otherwise 0. Part of double-entry
      bookkeeping where increases to liabilities/equity/revenue are credited. Mirrors debit_amount
      distribution (average $368) ensuring sum(debits) = sum(credits) across all entries.
      Essential for maintaining accounting equation balance and financial statement accuracy.
    checks:
      - name: not_null
  - name: currency
    type: VARCHAR
    description: |
      ISO currency code for all transaction amounts, consistently "USD" across all 1.9M records.
      Contoso operates as a US-based retailer with all financial reporting in US Dollars.
      Single currency simplifies financial consolidation and reporting. Future expansion
      could introduce multi-currency transactions requiring currency conversion handling
      in downstream financial analytics and reporting systems.
    checks:
      - name: not_null
  - name: source_table
    type: VARCHAR
    description: |
      Source system identifier indicating transaction origin (2 unique values: "sales", "overhead").
      "sales" entries derive from revenue/COGS transactions generated from contoso_raw.sales orders.
      "overhead" entries represent monthly departmental operating expenses like rent, software,
      travel, professional services. Critical for data lineage tracking, source system
      reconciliation, and understanding transaction composition in financial analysis.
    checks:
      - name: not_null
  - name: source_key
    type: INTEGER
    description: |
      Foreign key to the originating transaction record when applicable. For "sales" entries,
      references the order_key from contoso_raw.orders. For "overhead" entries, typically null
      as these represent aggregated monthly expenses rather than individual transactions.
      7,888 null values (~0.4%) align with overhead entries lacking specific source records.
      Enables drill-down from GL to source transactions for detailed analysis and audit.
  - name: extracted_at
    type: TIMESTAMP
    description: |
      UTC timestamp indicating when this data was loaded into the data warehouse, consistently
      2026-03-30 11:38:08.823923 across all records indicating a single batch load. Used for
      data lineage tracking, ETL monitoring, and determining data freshness. Not related to
      business transaction timing (see entry_date for transaction dates). Essential for
      operational monitoring and data quality assurance in the pipeline.
    checks:
      - name: not_null

@bruin"""

import logging
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import load_contoso_keys, load_parquet, seed_all

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

OVERHEAD_ACCOUNTS = [
    ("6300", "Rent & Utilities"),
    ("6400", "Software & Tools"),
    ("6500", "Travel & Entertainment"),
    ("6600", "Professional Services"),
    ("6700", "Office Supplies"),
    ("6800", "Training & Development"),
    ("6900", "Depreciation"),
]


def materialize():
    seed_all(42)
    rng = np.random.default_rng(42)

    logger.info("Generating GL journal entries...")
    records = []
    je_key = 0

    # --- Revenue and COGS from sales (sampled for performance) ---
    sales = load_parquet("sales")
    # Sample ~20% of sales for GL entries to keep size manageable
    sample_idx = rng.choice(len(sales), size=len(sales) // 5, replace=False)
    sales_sample = sales.iloc[sample_idx]

    logger.info("Generating revenue/COGS entries from %d sampled sales...", len(sales_sample))

    for _, row in sales_sample.iterrows():
        revenue_usd = round(float(row["Quantity"] * row["NetPrice"] * row["ExchangeRate"]), 2)
        cogs_usd = round(float(row["Quantity"] * row["UnitCost"] * row["ExchangeRate"]), 2)
        entry_date = pd.Timestamp(row["OrderDate"]).date()
        order_key = int(row["OrderKey"])

        # Revenue entry (debit AR, credit Revenue)
        je_key += 1
        records.append({
            "JournalEntryKey": je_key,
            "EntryDate": entry_date,
            "AccountCode": "1200",
            "AccountName": "Accounts Receivable",
            "DepartmentKey": 2,  # Sales
            "Description": f"Revenue - Order {order_key}",
            "DebitAmount": revenue_usd,
            "CreditAmount": 0.0,
            "Currency": "USD",
            "SourceTable": "sales",
            "SourceKey": order_key,
        })
        je_key += 1
        records.append({
            "JournalEntryKey": je_key,
            "EntryDate": entry_date,
            "AccountCode": "4000",
            "AccountName": "Revenue",
            "DepartmentKey": 2,
            "Description": f"Revenue - Order {order_key}",
            "DebitAmount": 0.0,
            "CreditAmount": revenue_usd,
            "Currency": "USD",
            "SourceTable": "sales",
            "SourceKey": order_key,
        })

        # COGS entry (debit COGS, credit Inventory)
        je_key += 1
        records.append({
            "JournalEntryKey": je_key,
            "EntryDate": entry_date,
            "AccountCode": "5000",
            "AccountName": "Cost of Goods Sold",
            "DepartmentKey": 8,  # Operations
            "Description": f"COGS - Order {order_key}",
            "DebitAmount": cogs_usd,
            "CreditAmount": 0.0,
            "Currency": "USD",
            "SourceTable": "sales",
            "SourceKey": order_key,
        })
        je_key += 1
        records.append({
            "JournalEntryKey": je_key,
            "EntryDate": entry_date,
            "AccountCode": "1400",
            "AccountName": "Inventory",
            "DepartmentKey": 8,
            "Description": f"COGS - Order {order_key}",
            "DebitAmount": 0.0,
            "CreditAmount": cogs_usd,
            "Currency": "USD",
            "SourceTable": "sales",
            "SourceKey": order_key,
        })

    # --- Monthly overhead entries ---
    keys = load_contoso_keys()
    min_date, max_date = keys["date_range"]
    months = pd.date_range(start=min_date, end=max_date, freq="MS")

    logger.info("Generating monthly overhead entries for %d months...", len(months))

    dept_keys = list(range(1, 13))
    for month_start in months:
        entry_date = month_start.date()
        for dept_key in dept_keys:
            # 2-3 overhead entries per department per month
            n_entries = int(rng.integers(2, 4))
            for _ in range(n_entries):
                acc_code, acc_name = OVERHEAD_ACCOUNTS[int(rng.integers(0, len(OVERHEAD_ACCOUNTS)))]
                amount = round(float(rng.lognormal(mean=8, sigma=1.0)), 2)
                amount = min(amount, 100_000)

                # Debit expense, credit cash
                je_key += 1
                records.append({
                    "JournalEntryKey": je_key,
                    "EntryDate": entry_date,
                    "AccountCode": acc_code,
                    "AccountName": acc_name,
                    "DepartmentKey": dept_key,
                    "Description": f"Monthly {acc_name} - Dept {dept_key}",
                    "DebitAmount": amount,
                    "CreditAmount": 0.0,
                    "Currency": "USD",
                    "SourceTable": "overhead",
                    "SourceKey": None,
                })
                je_key += 1
                records.append({
                    "JournalEntryKey": je_key,
                    "EntryDate": entry_date,
                    "AccountCode": "1000",
                    "AccountName": "Cash",
                    "DepartmentKey": dept_key,
                    "Description": f"Monthly {acc_name} - Dept {dept_key}",
                    "DebitAmount": 0.0,
                    "CreditAmount": amount,
                    "Currency": "USD",
                    "SourceTable": "overhead",
                    "SourceKey": None,
                })

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d GL journal entries", len(df))
    return df
