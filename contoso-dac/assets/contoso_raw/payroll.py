"""@bruin

name: contoso_raw.payroll
description: |
  Comprehensive payroll history for Contoso's consumer electronics retailer spanning
  2008-2025, containing 613K+ synthetic payroll records for 3,000 employees across
  all departments. This dataset simulates realistic payroll patterns with semi-monthly
  pay periods (24 per year), capturing the full employment lifecycle including
  terminated employees (15% turnover rate).

  The data models enterprise payroll operations including:
  - Semi-monthly pay structure with periods 1st-15th and 16th-end of month
  - Salary distributions from $35K-$350K annual (lognormal with slight random variance)
  - Realistic deduction rates of 25-35% covering taxes, benefits, and withholdings
  - Complete employment history from hire date through termination
  - All monetary amounts in USD with no currency fluctuations

  Key business patterns reflected:
  - Pay period alignment ensures consistent 24 payments annually
  - Gross pay includes slight variance (±5-8%) to simulate overtime/bonuses
  - Net pay calculation: gross_pay - deductions (mathematically consistent)
  - Employee tenure spans from recent hires to 17-year veterans
  - Terminated employees have complete payroll history through final pay period

  Data generation is deterministic using seed=42 for reproducible testing and
  cross-departmental analytics. Links to contoso_raw.employees via employee_key
  for workforce analytics, compensation benchmarking, and department-level
  payroll reporting.
connection: contoso-duckdb
instance: b1.large
tags:
  - domain:hr
  - domain:finance
  - data_type:fact_table
  - sensitivity:pii
  - pipeline_role:raw
  - update_pattern:snapshot

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: payroll_key
    type: INTEGER
    description: Unique sequential payroll record identifier (primary key)
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: employee_key
    type: INTEGER
    description: Foreign key to contoso_raw.employees table (1-3000 range)
    checks:
      - name: not_null
      - name: min
        value: 1
  - name: pay_period_start
    type: DATE
    description: Start date of semi-monthly pay period (1st or 16th of month)
    checks:
      - name: not_null
  - name: pay_period_end
    type: DATE
    description: End date of semi-monthly pay period (15th or last day of month)
    checks:
      - name: not_null
  - name: gross_pay
    type: DOUBLE
    description: Gross pay amount in USD for the pay period (includes salary + variance)
    checks:
      - name: not_null
      - name: min
        value: 1000
  - name: deductions
    type: DOUBLE
    description: Total deductions in USD (taxes, benefits, withholdings - typically 25-35% of gross)
    checks:
      - name: not_null
      - name: non_negative
  - name: net_pay
    type: DOUBLE
    description: Net pay amount in USD after deductions (gross_pay - deductions)
    checks:
      - name: not_null
      - name: positive
  - name: currency
    type: VARCHAR
    description: Currency code (always 'USD' for Contoso operations)
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - USD
  - name: extracted_at
    type: TIMESTAMP
    description: Data extraction timestamp in UTC (pipeline execution time)
    checks:
      - name: not_null

@bruin"""

import logging
import os
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd

import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import get_seeded_faker, seed_all

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

NUM_EMPLOYEES = 3000
SALARY_MIN = 35_000
SALARY_MAX = 350_000


def _generate_pay_periods(hire_date, term_date, cutoff=date(2025, 12, 31)):
    """Generate semi-monthly pay periods (1st-15th, 16th-end)."""
    periods = []
    end = term_date if term_date else cutoff
    current = date(hire_date.year, hire_date.month, 1)

    while current <= end:
        y, m = current.year, current.month
        mid = date(y, m, 15)
        if m == 12:
            eom = date(y + 1, 1, 1)
        else:
            eom = date(y, m + 1, 1)
        last_day = eom - pd.Timedelta(days=1)
        last_day = last_day.date() if hasattr(last_day, "date") else date(y, m, last_day.day)

        if mid >= hire_date and mid <= end:
            periods.append((date(y, m, 1), mid))
        if date(y, m, 16) >= hire_date and last_day <= end:
            periods.append((date(y, m, 16), last_day))

        if m == 12:
            current = date(y + 1, 1, 1)
        else:
            current = date(y, m + 1, 1)

    return periods


def materialize():
    seed_all(42)
    rng = np.random.default_rng(42)
    fake = get_seeded_faker(42)

    # Generate employee base data (salary + hire/term dates)
    salaries = rng.lognormal(mean=10.8, sigma=0.5, size=NUM_EMPLOYEES)
    salaries = np.clip(salaries, SALARY_MIN, SALARY_MAX).round(2)

    hire_dates = [
        fake.date_between(start_date=datetime(2008, 1, 1), end_date=datetime(2025, 6, 1))
        for _ in range(NUM_EMPLOYEES)
    ]

    term_dates = []
    for hd in hire_dates:
        if rng.random() < 0.15:
            term_dates.append(fake.date_between(start_date=hd, end_date=datetime(2025, 12, 31)))
        else:
            term_dates.append(None)

    logger.info("Generating payroll for %d employees...", NUM_EMPLOYEES)

    records = []
    payroll_key = 0
    for emp_idx in range(NUM_EMPLOYEES):
        emp_key = emp_idx + 1
        salary = salaries[emp_idx]
        semi_monthly_gross = round(salary / 24, 2)
        periods = _generate_pay_periods(hire_dates[emp_idx], term_dates[emp_idx])

        for period_start, period_end in periods:
            payroll_key += 1
            gross = round(semi_monthly_gross * rng.uniform(0.95, 1.08), 2)
            deduction_rate = rng.uniform(0.25, 0.35)
            deductions = round(gross * deduction_rate, 2)
            net = round(gross - deductions, 2)

            records.append({
                "PayrollKey": payroll_key,
                "EmployeeKey": emp_key,
                "PayPeriodStart": period_start,
                "PayPeriodEnd": period_end,
                "GrossPay": gross,
                "Deductions": deductions,
                "NetPay": net,
                "Currency": "USD",
            })

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d payroll records", len(df))
    return df
