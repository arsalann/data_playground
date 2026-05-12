"""@bruin

name: contoso_v2_raw.budgets
description: |
  Annual departmental budgets for Contoso v2, 2010-2026, broken out by month
  and 8 OpEx GL accounts (6100-6800). Year-over-year growth is regime-driven:
  +8-12%/yr during expansion (2015-2019), 0-3% in 2020 (freeze), tighter in
  2022 inflation, +6-10% in 2023-2024 AI investment, normal in 2025+. Q4
  receives ~+15% lift; Q1 about -10%. Includes a budget_version column —
  baseline plus mid-year reforecasts for 2020 (COVID), 2022 (inflation), and
  2023 (AI) to mirror real-world replanning. Marketing dept gets heavier
  6200 weight; Engineering and Data & Analytics carry larger 6400 software
  budgets.
connection: gcp-default
tags:
  - finance
  - planning
  - fact_table

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: budget_key
    type: INTEGER
    primary_key: true
    description: Sparse budget line identifier.
    checks:
      - name: not_null
      - name: unique
  - name: department_key
    type: INTEGER
    description: Foreign key to departments.
    checks:
      - name: not_null
  - name: fiscal_year
    type: INTEGER
    description: Fiscal year (calendar).
    checks:
      - name: not_null
  - name: fiscal_month
    type: INTEGER
    description: Month 1-12.
    checks:
      - name: not_null
  - name: account_code
    type: VARCHAR
    description: 4-digit GL account code (6100-6800).
    checks:
      - name: not_null
  - name: account_name
    type: VARCHAR
    description: Account label.
    checks:
      - name: not_null
  - name: budget_amount
    type: NUMERIC
    description: Planned monthly budget in USD.
    checks:
      - name: not_null
      - name: non_negative
  - name: currency
    type: VARCHAR
    description: Always USD.
    checks:
      - name: not_null
  - name: budget_version
    type: VARCHAR
    description: baseline or reforecast (mid-year revision).
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - baseline
          - reforecast
  - name: extracted_at
    type: TIMESTAMP

@bruin"""

import logging
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import (
    annual_target_headcount, get_seeded_rng, sparse_keys,
)

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
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

# Annual base budget per department in 2010, in $K
DEPT_BASE_2010 = {
    1: 600, 2: 1_500, 3: 600, 4: 400, 5: 300, 6: 800,
    7: 400, 8: 700, 9: 200, 10: 250, 11: 200, 12: 0,  # Data & Analytics 0 pre-2015
}


def _yoy_growth(year: int) -> float:
    if 2015 <= year <= 2019:
        return 1.10
    if year == 2020:
        return 1.01
    if year == 2021:
        return 1.04
    if year == 2022:
        return 1.06
    if 2023 <= year <= 2024:
        return 1.09
    if year == 2025:
        return 1.04
    return 1.05


def _reforecast_factor(year: int, dept_key: int, account_code: str) -> float:
    # 2020 reforecast cuts, 2022 inflation reforecast on cost-sensitive lines, 2023 AI investment lift
    if year == 2020:
        if account_code in ("6500", "6700", "6800"):
            return 0.50  # heavy travel/training cuts
        return 0.90
    if year == 2022:
        if account_code in ("6300", "6500"):
            return 1.10
        return 1.04
    if year == 2023:
        if dept_key in (6, 12) and account_code == "6400":
            return 1.30
        if dept_key == 3 and account_code == "6200":
            return 1.20
        return 1.05
    return 1.0


def materialize():
    rng = get_seeded_rng(181)
    sparse_iter = iter(sparse_keys(60_000, gap_rate=0.05, start=1, seed=181))

    rows = []
    for year in range(2010, 2027):
        # Compound growth from 2010
        compound = 1.0
        for y in range(2011, year + 1):
            compound *= _yoy_growth(y)

        # Headcount-based scaling for payroll-heavy budgets
        hc_scale = annual_target_headcount(year) / annual_target_headcount(2010)

        for dept_key, base_k in DEPT_BASE_2010.items():
            if base_k == 0 and year < 2015:
                continue
            base_k_eff = base_k if base_k > 0 else 200
            annual_usd = base_k_eff * compound * 1000.0
            # Payroll component scales further with headcount
            payroll_share = 0.55

            for month in range(1, 13):
                if month in (10, 11, 12):
                    seasonal = 1.15
                elif month in (1, 2, 3):
                    seasonal = 0.90
                else:
                    seasonal = 1.0
                monthly = annual_usd / 12.0 * seasonal

                for acc_code, acc_name in ACCOUNTS:
                    if acc_code == "6100":
                        weight = payroll_share * (0.7 + 0.3 * hc_scale / 5.0)
                    elif acc_code == "6200":
                        weight = 0.30 if dept_key == 3 else 0.04
                    elif acc_code == "6400":
                        weight = 0.20 if dept_key in (6, 12) else 0.05
                    elif acc_code == "6300":
                        weight = 0.06 if dept_key == 11 else 0.04
                    else:
                        weight = float(rng.uniform(0.02, 0.06))

                    amount = round(
                        monthly * weight * float(rng.uniform(0.95, 1.05)), 2
                    )
                    if amount < 100:
                        continue

                    # Baseline row
                    rows.append({
                        "budget_key": int(next(sparse_iter)),
                        "department_key": dept_key,
                        "fiscal_year": year,
                        "fiscal_month": month,
                        "account_code": acc_code,
                        "account_name": acc_name,
                        "budget_amount": amount,
                        "currency": "USD",
                        "budget_version": "baseline",
                    })

                    # Reforecast for selected years (mid-year, applies to remaining months)
                    if year in (2020, 2022, 2023) and month >= 4:
                        rf_amount = round(
                            amount * _reforecast_factor(year, dept_key, acc_code), 2
                        )
                        if rf_amount >= 100 and abs(rf_amount - amount) > 1:
                            rows.append({
                                "budget_key": int(next(sparse_iter)),
                                "department_key": dept_key,
                                "fiscal_year": year,
                                "fiscal_month": month,
                                "account_code": acc_code,
                                "account_name": acc_name,
                                "budget_amount": rf_amount,
                                "currency": "USD",
                                "budget_version": "reforecast",
                            })

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d budget records", len(df))
    return df
