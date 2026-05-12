"""@bruin

name: contoso_v2_raw.gl_journal_entries
description: |
  General ledger journal entries for Contoso v2, 2010-01-01 to 2026-05-01.
  Sales-derived revenue/COGS sampled at ~20% of sales lines for performance.
  Overhead entries scale to department headcount and budget growth (so 2024
  is roughly 7x 2010). Quarter-end accruals cluster on the last 3 days of
  March/June/September/December. ~1% reversal entries (negative amounts;
  reversal_of_entry_key references the corrected entry) representing
  accounting corrections. Chart of accounts: 1000 Cash, 1200 AR,
  1400 Inventory, 4000 Revenue, 5000 COGS, 6300-6900 OpEx (Rent, Software,
  Travel, Services, Supplies, Training, Depreciation).
connection: gcp-default
instance: b1.xlarge
tags:
  - finance
  - accounting
  - fact_table

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: journal_entry_key
    type: INTEGER
    primary_key: true
    description: Sparse journal entry line identifier.
    checks:
      - name: not_null
      - name: unique
  - name: entry_date
    type: DATE
    description: Business date of the underlying transaction.
    checks:
      - name: not_null
  - name: account_code
    type: VARCHAR
    description: 4-digit GL account code.
    checks:
      - name: not_null
  - name: account_name
    type: VARCHAR
    description: Account label.
    checks:
      - name: not_null
  - name: department_key
    type: INTEGER
    description: Foreign key to departments.
    checks:
      - name: not_null
  - name: description
    type: VARCHAR
    description: Transaction description.
    checks:
      - name: not_null
  - name: debit_amount
    type: NUMERIC
    description: Debit amount (USD). Negative for reversals.
    checks:
      - name: not_null
  - name: credit_amount
    type: NUMERIC
    description: Credit amount (USD). Negative for reversals.
    checks:
      - name: not_null
  - name: currency
    type: VARCHAR
    description: Always USD.
    checks:
      - name: not_null
  - name: source_table
    type: VARCHAR
    description: sales, overhead, accrual, or reversal.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - sales
          - overhead
          - accrual
          - reversal
  - name: source_key
    type: INTEGER
    description: Foreign key into the source transaction (NULL for overhead).
  - name: reversal_of_entry_key
    type: INTEGER
    description: For reversals, references the entry being corrected.
  - name: extracted_at
    type: TIMESTAMP

@bruin"""

import logging
import os
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import (
    PIPELINE_END, PIPELINE_START, annual_target_headcount,
    build_sales_lines, get_seeded_rng, sparse_keys,
)

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
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
    rng = get_seeded_rng(197)
    sparse_iter = iter(sparse_keys(15_000_000, gap_rate=0.02, start=1, seed=197))

    rows = []

    # ---- Sales-derived revenue/COGS ----
    sales = build_sales_lines()
    n_sales = len(sales)
    sample_idx = rng.choice(n_sales, size=n_sales // 5, replace=False)
    sample_idx.sort()
    logger.info("Generating GL revenue/COGS from %d / %d sales lines",
                len(sample_idx), n_sales)

    qty = sales["quantity"].values
    net_price = sales["net_price"].values
    unit_cost = sales["unit_cost"].values
    xrate = sales["exchange_rate"].values
    order_dates = pd.to_datetime(sales["order_date"]).dt.date.values
    order_keys = sales["order_key"].values

    for ix in sample_idx:
        revenue_usd = round(float(qty[ix] * net_price[ix] * xrate[ix]), 2)
        cogs_usd = round(float(abs(qty[ix]) * unit_cost[ix] * xrate[ix]), 2)
        edate = order_dates[ix]
        ok = int(order_keys[ix])

        # Revenue: DR AR / CR Revenue
        ar_key = int(next(sparse_iter))
        rev_key = int(next(sparse_iter))
        rows.append({
            "journal_entry_key": ar_key,
            "entry_date": edate,
            "account_code": "1200",
            "account_name": "Accounts Receivable",
            "department_key": 2,
            "description": f"Revenue - Order {ok}",
            "debit_amount": revenue_usd,
            "credit_amount": 0.0,
            "currency": "USD",
            "source_table": "sales",
            "source_key": ok,
            "reversal_of_entry_key": None,
        })
        rows.append({
            "journal_entry_key": rev_key,
            "entry_date": edate,
            "account_code": "4000",
            "account_name": "Revenue",
            "department_key": 2,
            "description": f"Revenue - Order {ok}",
            "debit_amount": 0.0,
            "credit_amount": revenue_usd,
            "currency": "USD",
            "source_table": "sales",
            "source_key": ok,
            "reversal_of_entry_key": None,
        })
        # COGS: DR COGS / CR Inventory
        cogs_key = int(next(sparse_iter))
        inv_key = int(next(sparse_iter))
        rows.append({
            "journal_entry_key": cogs_key,
            "entry_date": edate,
            "account_code": "5000",
            "account_name": "Cost of Goods Sold",
            "department_key": 8,
            "description": f"COGS - Order {ok}",
            "debit_amount": cogs_usd,
            "credit_amount": 0.0,
            "currency": "USD",
            "source_table": "sales",
            "source_key": ok,
            "reversal_of_entry_key": None,
        })
        rows.append({
            "journal_entry_key": inv_key,
            "entry_date": edate,
            "account_code": "1400",
            "account_name": "Inventory",
            "department_key": 8,
            "description": f"COGS - Order {ok}",
            "debit_amount": 0.0,
            "credit_amount": cogs_usd,
            "currency": "USD",
            "source_table": "sales",
            "source_key": ok,
            "reversal_of_entry_key": None,
        })

    # ---- Monthly overhead entries ----
    months = pd.date_range(
        start=pd.Timestamp(PIPELINE_START).replace(day=1),
        end=pd.Timestamp(PIPELINE_END),
        freq="MS",
    )
    logger.info("Generating overhead entries for %d months", len(months))

    for ts in months:
        d = ts.date()
        hc_scale = annual_target_headcount(d.year) / annual_target_headcount(2010)
        for dept_key in range(1, 13):
            n_entries = int(rng.integers(2, 5))
            for _ in range(n_entries):
                acc_code, acc_name = OVERHEAD_ACCOUNTS[
                    int(rng.integers(0, len(OVERHEAD_ACCOUNTS)))
                ]
                amount = float(rng.lognormal(mean=8.0, sigma=1.0)) * hc_scale
                amount = min(amount, 200_000)
                amount = round(amount, 2)

                exp_key = int(next(sparse_iter))
                cash_key = int(next(sparse_iter))
                rows.append({
                    "journal_entry_key": exp_key,
                    "entry_date": d,
                    "account_code": acc_code,
                    "account_name": acc_name,
                    "department_key": dept_key,
                    "description": f"Monthly {acc_name} - Dept {dept_key}",
                    "debit_amount": amount,
                    "credit_amount": 0.0,
                    "currency": "USD",
                    "source_table": "overhead",
                    "source_key": None,
                    "reversal_of_entry_key": None,
                })
                rows.append({
                    "journal_entry_key": cash_key,
                    "entry_date": d,
                    "account_code": "1000",
                    "account_name": "Cash",
                    "department_key": dept_key,
                    "description": f"Monthly {acc_name} - Dept {dept_key}",
                    "debit_amount": 0.0,
                    "credit_amount": amount,
                    "currency": "USD",
                    "source_table": "overhead",
                    "source_key": None,
                    "reversal_of_entry_key": None,
                })

        # Quarter-end accruals on last 3 days of Mar/Jun/Sep/Dec
        if d.month in (3, 6, 9, 12):
            for accrual_day in [-3, -2, -1]:
                if d.month == 12:
                    last = date(d.year + 1, 1, 1) - timedelta(days=1)
                else:
                    last = date(d.year, d.month + 1, 1) - timedelta(days=1)
                accrual_date = last + timedelta(days=accrual_day + 1)
                for _ in range(int(rng.integers(8, 16))):
                    dept_key = int(rng.integers(1, 13))
                    acc_code, acc_name = OVERHEAD_ACCOUNTS[
                        int(rng.integers(0, len(OVERHEAD_ACCOUNTS)))
                    ]
                    amount = round(float(rng.lognormal(mean=9.0, sigma=0.8)) * hc_scale, 2)
                    amount = min(amount, 250_000)
                    rows.append({
                        "journal_entry_key": int(next(sparse_iter)),
                        "entry_date": accrual_date,
                        "account_code": acc_code,
                        "account_name": acc_name,
                        "department_key": dept_key,
                        "description": f"Q-end accrual {acc_name} - Dept {dept_key}",
                        "debit_amount": amount,
                        "credit_amount": 0.0,
                        "currency": "USD",
                        "source_table": "accrual",
                        "source_key": None,
                        "reversal_of_entry_key": None,
                    })
                    rows.append({
                        "journal_entry_key": int(next(sparse_iter)),
                        "entry_date": accrual_date,
                        "account_code": "1000",
                        "account_name": "Cash",
                        "department_key": dept_key,
                        "description": f"Q-end accrual {acc_name} - Dept {dept_key}",
                        "debit_amount": 0.0,
                        "credit_amount": amount,
                        "currency": "USD",
                        "source_table": "accrual",
                        "source_key": None,
                        "reversal_of_entry_key": None,
                    })

    df = pd.DataFrame(rows)
    # 1% reversal entries — flip the sign of an existing entry, link via reversal_of_entry_key
    n_rev = max(1, int(len(df) * 0.01))
    rev_idx = rng.choice(len(df), size=n_rev, replace=False)
    rev_rows = df.iloc[rev_idx].copy()
    rev_rows["journal_entry_key"] = [int(next(sparse_iter)) for _ in range(n_rev)]
    rev_rows["debit_amount"] = -rev_rows["debit_amount"]
    rev_rows["credit_amount"] = -rev_rows["credit_amount"]
    rev_rows["source_table"] = "reversal"
    rev_rows["reversal_of_entry_key"] = df.iloc[rev_idx]["journal_entry_key"].values
    rev_rows["description"] = "REVERSAL: " + rev_rows["description"].astype(str)
    df = pd.concat([df, rev_rows], ignore_index=True)

    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d GL entries (%d reversals)", len(df), n_rev)
    return df
