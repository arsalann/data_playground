"""@bruin

name: contoso_v2_raw.dates
description: |
  Calendar dimension table for the Contoso v2 simulated retailer, spanning
  2010-01-01 to 2026-05-01 (5,966 days). Includes hierarchical date breakdowns,
  working-day flags, retail holiday names, and the named operating regime
  active on each date (early_growth, expansion, mature_ops, covid_shock,
  recovery, inflation, ai_boom, stabilization, recent).

  Generated synthetically — no external parquet dependency.
connection: gcp-default
tags:
  - dimension_table
  - reference_data
  - time_dimension
  - retail_simulation

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: date
    type: TIMESTAMP
    description: Primary calendar date (midnight UTC timestamp).
    primary_key: true
    checks:
      - name: not_null
  - name: date_key
    type: VARCHAR
    description: Date in YYYYMMDD string format.
    checks:
      - name: not_null
      - name: unique
  - name: year
    type: INTEGER
    description: Calendar year (2010-2026).
    checks:
      - name: not_null
  - name: year_quarter
    type: VARCHAR
    description: Year and quarter in readable format (e.g. "2022-Q3").
    checks:
      - name: not_null
  - name: year_quarter_number
    type: INTEGER
    description: Year-quarter as integer (e.g. 20223 for 2022-Q3).
    checks:
      - name: not_null
  - name: quarter
    type: VARCHAR
    description: Quarter designation (Q1-Q4).
    checks:
      - name: not_null
  - name: year_month
    type: VARCHAR
    description: Year and full month name (e.g. "2022-March").
    checks:
      - name: not_null
  - name: year_month_short
    type: VARCHAR
    description: Year and abbreviated month (e.g. "2022-Mar").
    checks:
      - name: not_null
  - name: year_month_number
    type: INTEGER
    description: Year-month as integer (e.g. 202203).
    checks:
      - name: not_null
  - name: month
    type: VARCHAR
    description: Full month name.
    checks:
      - name: not_null
  - name: month_short
    type: VARCHAR
    description: Three-letter month abbreviation.
    checks:
      - name: not_null
  - name: month_number
    type: INTEGER
    description: Numeric month (1-12).
    checks:
      - name: not_null
  - name: dayof_week
    type: VARCHAR
    description: Full day name.
    checks:
      - name: not_null
  - name: dayof_week_short
    type: VARCHAR
    description: Three-letter day abbreviation.
    checks:
      - name: not_null
  - name: dayof_week_number
    type: INTEGER
    description: Day of week as integer (1=Monday, 7=Sunday).
    checks:
      - name: not_null
  - name: working_day
    type: INTEGER
    description: Binary working-day flag (1 = business day, 0 = weekend).
    checks:
      - name: not_null
  - name: working_day_number
    type: INTEGER
    description: Sequential counter of working days since pipeline start.
    checks:
      - name: not_null
  - name: is_holiday
    type: BOOLEAN
    description: True if the date is a recognized retail holiday.
  - name: holiday_name
    type: VARCHAR
    description: Name of the holiday on this date, if any.
  - name: is_holiday_window
    type: BOOLEAN
    description: True for the Nov 20 - Dec 31 retail holiday window.
  - name: regime
    type: VARCHAR
    description: |
      Named operating regime active on this date: early_growth, expansion,
      mature_ops, covid_shock, recovery, inflation, ai_boom, stabilization, recent.
    checks:
      - name: not_null
  - name: is_event_window
    type: BOOLEAN
    description: True during covid_shock, inflation, or ai_boom regimes.
  - name: extracted_at
    type: TIMESTAMP
    description: ETL processing timestamp (UTC).

@bruin"""

import logging
import os
from datetime import date, datetime, timedelta, timezone

import pandas as pd

import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import (
    PIPELINE_START, PIPELINE_END, holiday_name, is_holiday_window, regime_for,
)

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)

EVENT_REGIMES = {"covid_shock", "inflation", "ai_boom"}


def materialize():
    logger.info("Generating calendar 2010-01-01 to 2026-05-01...")
    rows = []
    work_counter = 0
    cur = PIPELINE_START
    while cur <= PIPELINE_END:
        is_weekend = cur.weekday() >= 5
        working = 0 if is_weekend else 1
        if working:
            work_counter += 1
        hname = holiday_name(cur)
        rows.append({
            "date": pd.Timestamp(cur),
            "date_key": cur.strftime("%Y%m%d"),
            "year": cur.year,
            "year_quarter": f"{cur.year}-Q{(cur.month - 1) // 3 + 1}",
            "year_quarter_number": cur.year * 10 + ((cur.month - 1) // 3 + 1),
            "quarter": f"Q{(cur.month - 1) // 3 + 1}",
            "year_month": f"{cur.year}-{cur.strftime('%B')}",
            "year_month_short": f"{cur.year}-{cur.strftime('%b')}",
            "year_month_number": cur.year * 100 + cur.month,
            "month": cur.strftime("%B"),
            "month_short": cur.strftime("%b"),
            "month_number": cur.month,
            "dayof_week": cur.strftime("%A"),
            "dayof_week_short": cur.strftime("%a"),
            "dayof_week_number": cur.weekday() + 1,
            "working_day": working,
            "working_day_number": work_counter,
            "is_holiday": hname is not None,
            "holiday_name": hname,
            "is_holiday_window": is_holiday_window(cur),
            "regime": regime_for(cur),
            "is_event_window": regime_for(cur) in EVENT_REGIMES,
        })
        cur += timedelta(days=1)

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d calendar rows", len(df))
    return df
