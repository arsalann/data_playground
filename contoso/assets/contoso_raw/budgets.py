"""@bruin

name: contoso_raw.budgets
description: |
  Contoso annual budget planning data across 12 departments and 8 GL account categories for the consumer electronics retailer's operational expense planning.

  This synthetic dataset represents the company's financial planning process, capturing monthly budget allocations
  by department (Executive, Sales, Marketing, Finance, HR, Engineering, Customer Support, Operations, Legal, Product,
  Facilities, Data & Analytics) across 8 operational expense categories. Used as the foundational data source for
  budget vs. actual variance analysis, departmental cost control, and executive financial reporting.

  Key business characteristics:
  - Covers fiscal years ~2015-2025 with seasonal budget variations (Q4 +15%, Q1 -10%)
  - Annual base budgets range from $1.5M (Facilities) to $15M (Sales) with 3-8% year-over-year growth
  - Payroll dominates budget allocation (~55% across all departments)
  - Marketing department gets higher allocation for account 6200 (Marketing expenses)
  - Engineering and Data & Analytics receive larger software/tools budgets (account 6400)
  - Budget amounts exclude capital expenditures and revenue/COGS accounts (focuses on 6xxx OpEx GL codes)
  - Generated deterministically (seed=42) to ensure reproducible cross-departmental analytics

  Links to downstream financial reporting in contoso_staging.financial_summary_monthly for comprehensive
  budget variance analysis and executive KPI dashboards. References contoso_raw.departments for organizational
  hierarchy and cost center mappings.
connection: bruin-playground-eu
tags:
  - domain:finance
  - domain:planning
  - data_type:fact_table
  - data_type:budget_data
  - sensitivity:internal
  - pipeline_role:raw
  - update_pattern:static
  - synthetic_data
  - financial_planning
  - operational_expenses

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: budget_key
    type: INTEGER
    description: |
      Unique sequential identifier for each budget line item (1-12,672). Primary key providing
      granular tracking of individual department/month/account budget allocations. Used for
      data lineage tracking and joining with actual expenses in downstream financial analytics.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: department_key
    type: INTEGER
    description: |
      Foreign key to contoso_raw.departments (1-12) representing Contoso's organizational structure.
      Maps to business units: Executive (1), Sales (2), Marketing (3), Finance (4), HR (5),
      Engineering (6), Customer Support (7), Operations (8), Legal (9), Product (10), Facilities (11),
      Data & Analytics (12). Critical for departmental budget rollups and variance analysis.
    checks:
      - name: not_null
  - name: fiscal_year
    type: INTEGER
    description: |
      Calendar year representing the fiscal period (e.g., 2021). Contoso operates on calendar year
      fiscal periods. Budget data spans approximately 2015-2025 to support multi-year trend analysis
      and long-term financial planning. Used for year-over-year budget variance reporting.
    checks:
      - name: not_null
  - name: fiscal_month
    type: INTEGER
    description: |
      Month number within fiscal year (1-12 representing January-December). Budget allocations
      include seasonal adjustments: Q4 (Oct-Dec) receives ~15% increase for holiday operations,
      Q1 (Jan-Mar) reduced by ~10% for slower business periods. Essential for monthly financial
      reporting and quarterly budget reviews.
    checks:
      - name: not_null
  - name: account_code
    type: VARCHAR
    description: |
      4-digit GL account code following standard chart of accounts for operational expenses
      (6100-6800 series). Categories include: Payroll (6100), Marketing (6200), Rent & Utilities (6300),
      Software & Tools (6400), Travel & Entertainment (6500), Professional Services (6600),
      Office Supplies (6700), Training & Development (6800). Links to GL journal entries for actual
      vs budget variance calculations.
    checks:
      - name: not_null
  - name: account_name
    type: VARCHAR
    description: |
      Human-readable GL account description corresponding to AccountCode. Provides business context
      for operational expense categories during financial reporting and budget analysis. Names range
      from 7-22 characters (e.g., "Payroll", "Marketing", "Training & Development") for clear
      identification in executive dashboards and departmental cost reports.
    checks:
      - name: not_null
  - name: budget_amount
    type: DOUBLE
    description: |
      Planned budget allocation in USD for this department/month/account combination ($2,326-$1,514,765).
      Represents monthly spending targets used for variance analysis against actual expenses.
      Budget amounts incorporate departmental priorities (Payroll ~55% of budgets), seasonal
      adjustments, and annual growth factors. Amounts below $100 filtered out to focus on
      material budget items.
    checks:
      - name: not_null
  - name: currency
    type: VARCHAR
    description: |
      Currency denomination for budget amounts, standardized to "USD" across all records.
      Contoso operates primarily in US markets with USD as the base reporting currency.
      Consistent currency eliminates need for exchange rate conversions in financial analytics
      and ensures accurate budget variance calculations.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: |
      ETL metadata timestamp indicating when this synthetic dataset was generated and loaded
      into the warehouse (UTC). All records share the same extraction time since this is a
      deterministically generated planning dataset that refreshes completely on each pipeline run.
      Critical for data lineage tracking and ensuring downstream models use the latest budget version.
    checks:
      - name: not_null

@bruin"""

import logging
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import load_contoso_keys, seed_all

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

ACCOUNTS = [
    ("6100", "Payroll"),
    ("6200", "Marketing"),
    ("6300", "Rent & Utilities"),
    ("6400", "Software & Tools"),
    ("6500", "Travel & Entertainment"),
    ("6600", "Professional Services"),
    ("6700", "Office Supplies"),
    ("6800", "Training & Development"),
]

# Annual base budget by department (in thousands USD)
DEPT_ANNUAL_BUDGET = {
    1: 2_000,    # Executive
    2: 15_000,   # Sales
    3: 8_000,    # Marketing
    4: 3_000,    # Finance
    5: 2_500,    # Human Resources
    6: 12_000,   # Engineering
    7: 5_000,    # Customer Support
    8: 6_000,    # Operations
    9: 2_000,    # Legal
    10: 4_000,   # Product
    11: 1_500,   # Facilities
    12: 3_000,   # Data & Analytics
}


def materialize():
    seed_all(42)
    rng = np.random.default_rng(42)
    keys = load_contoso_keys()
    min_date, max_date = keys["date_range"]

    start_year = pd.Timestamp(min_date).year
    end_year = pd.Timestamp(max_date).year

    logger.info("Generating budgets for years %d-%d...", start_year, end_year)

    records = []
    budget_key = 0

    for year in range(start_year, end_year + 1):
        # Annual growth factor (3-8% per year from base)
        growth = 1 + (year - start_year) * rng.uniform(0.03, 0.08)

        for dept_key, annual_base in DEPT_ANNUAL_BUDGET.items():
            annual = annual_base * growth * 1000  # convert from thousands

            for month in range(1, 13):
                # Seasonality: Q4 gets ~30% more budget, Q1 gets ~10% less
                if month in (10, 11, 12):
                    seasonal = 1.15
                elif month in (1, 2, 3):
                    seasonal = 0.90
                else:
                    seasonal = 1.0

                monthly_total = annual / 12 * seasonal

                # Split across account categories
                for acc_code, acc_name in ACCOUNTS:
                    # Weight by account type and department
                    if acc_code == "6100":  # Payroll - largest
                        weight = 0.55
                    elif acc_code == "6200" and dept_key == 3:  # Marketing dept
                        weight = 0.30
                    elif acc_code == "6200":
                        weight = 0.05
                    elif acc_code == "6400" and dept_key in (6, 12):  # Software for tech
                        weight = 0.20
                    else:
                        weight = rng.uniform(0.02, 0.08)

                    amount = round(monthly_total * weight * rng.uniform(0.9, 1.1), 2)
                    if amount < 100:
                        continue

                    budget_key += 1
                    records.append({
                        "BudgetKey": budget_key,
                        "DepartmentKey": dept_key,
                        "FiscalYear": year,
                        "FiscalMonth": month,
                        "AccountCode": acc_code,
                        "AccountName": acc_name,
                        "BudgetAmount": amount,
                        "Currency": "USD",
                    })

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d budget records", len(df))
    return df
