"""@bruin

name: contoso_v2_raw.payroll
description: |
  Semi-monthly payroll history for Contoso v2 employees, 2010-01-01 to
  2026-05-01. Each employee has 24 pay periods/year covering hire to
  termination. Mixture distribution: ~90% tight ±3% gross variance, ~8% bonus
  periods (Q1 prior-year, Q4 holiday) with +30-80% lift, ~2% reversal entries
  with negative gross referencing a prior payroll_key (corrections).
  Sign-on bonuses appear in first month after hire; termination payouts on the
  last check before termination_date.
connection: gcp-default
instance: b1.large
tags:
  - fact_table
  - hr
  - finance
  - sensitivity:pii

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: payroll_key
    type: INTEGER
    primary_key: true
    description: Sparse payroll record identifier.
    checks:
      - name: not_null
      - name: unique
  - name: employee_key
    type: INTEGER
    description: Foreign key to employees.
    checks:
      - name: not_null
  - name: pay_period_start
    type: DATE
    description: Start of semi-monthly pay period (1st or 16th).
    checks:
      - name: not_null
  - name: pay_period_end
    type: DATE
    description: End of pay period (15th or end-of-month).
    checks:
      - name: not_null
  - name: gross_pay
    type: NUMERIC
    description: Gross pay in USD. Negative for reversal entries.
    checks:
      - name: not_null
  - name: deductions
    type: NUMERIC
    description: Total deductions in USD.
    checks:
      - name: not_null
  - name: net_pay
    type: NUMERIC
    description: Net pay after deductions in USD.
    checks:
      - name: not_null
  - name: pay_type
    type: VARCHAR
    description: regular, bonus, signon, termination, reversal.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - regular
          - bonus
          - signon
          - termination
          - reversal
  - name: reversal_of_payroll_key
    type: INTEGER
    description: For reversal entries, references the earlier payroll_key being corrected.
  - name: currency
    type: VARCHAR
    description: Always USD.
    checks:
      - name: not_null
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
    PIPELINE_END, build_employees, get_seeded_rng, sparse_keys,
)

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


def _periods(hire_date, term_date, cutoff=PIPELINE_END):
    """Yield semi-monthly pay periods (1-15, 16-EOM)."""
    end = term_date if term_date else cutoff
    if hire_date > end:
        return
    cur = date(hire_date.year, hire_date.month, 1)
    while cur <= end:
        y, m = cur.year, cur.month
        mid = date(y, m, 15)
        if m == 12:
            next_first = date(y + 1, 1, 1)
        else:
            next_first = date(y, m + 1, 1)
        eom = next_first - timedelta(days=1)

        if mid >= hire_date and mid <= end:
            yield (date(y, m, 1) if hire_date.day <= 1 else max(date(y, m, 1), date(hire_date.year, hire_date.month, 1)), mid, "first_half")
        if eom >= hire_date and date(y, m, 16) <= end:
            yield (date(y, m, 16), eom if eom <= end else end, "second_half")
        cur = next_first


def materialize():
    rng = get_seeded_rng(73)
    employees = build_employees()

    n_employees = len(employees)
    logger.info("Generating payroll for %d employees...", n_employees)

    # Estimate total records to pre-size sparse keys
    sparse_iter = iter(sparse_keys(2_000_000, gap_rate=0.05, start=1, seed=73))

    records = []
    hire_dates = pd.to_datetime(employees["hire_date"]).dt.date.tolist()
    term_dates = [
        d.date() if pd.notna(d) else None
        for d in pd.to_datetime(employees["termination_date"])
    ]
    salaries = employees["salary"].tolist()
    emp_keys = employees["employee_key"].tolist()

    for i in range(n_employees):
        emp_key = int(emp_keys[i])
        hd = hire_dates[i]
        td = term_dates[i]
        salary = float(salaries[i])
        semi_gross = salary / 24.0

        first_period = True
        prior_pkey = None
        for period_start, period_end, half in _periods(hd, td):
            pkey = next(sparse_iter)
            # 90% regular, 8% bonus (Q1, Q4 mostly), 2% reversal
            r = rng.random()
            if first_period and rng.random() < 0.6:
                pay_type = "signon"
                first_period = False
                gross = round(semi_gross * float(rng.uniform(1.4, 2.5)), 2)
            elif (period_end == td) if td else False:
                pay_type = "termination"
                gross = round(semi_gross * float(rng.uniform(1.0, 3.0)), 2)
            elif r < 0.90:
                pay_type = "regular"
                gross = round(semi_gross * float(rng.lognormal(0, 0.025)), 2)
            elif r < 0.98:
                pay_type = "bonus"
                # Bonus in Q1 (Feb/Mar) and Q4 (Nov/Dec) primarily
                if period_end.month in (2, 3, 11, 12):
                    boost = float(rng.uniform(1.30, 1.80))
                else:
                    boost = float(rng.uniform(1.10, 1.40))
                gross = round(semi_gross * boost, 2)
            else:
                pay_type = "reversal"
                gross = -round(semi_gross * float(rng.uniform(0.9, 1.1)), 2)

            ded_rate = float(rng.uniform(0.24, 0.36))
            deductions = round(abs(gross) * ded_rate * (1 if gross > 0 else -1), 2)
            net = round(gross - deductions, 2)

            records.append({
                "payroll_key": int(pkey),
                "employee_key": emp_key,
                "pay_period_start": period_start,
                "pay_period_end": period_end,
                "gross_pay": gross,
                "deductions": deductions,
                "net_pay": net,
                "pay_type": pay_type,
                "reversal_of_payroll_key": prior_pkey if pay_type == "reversal" else None,
                "currency": "USD",
            })
            prior_pkey = int(pkey) if pay_type == "regular" else prior_pkey
            first_period = False

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d payroll records", len(df))
    return df
