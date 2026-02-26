"""@bruin
name: raw.stackoverflow_api_monthly
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Supplements the BigQuery public dataset (frozen at Sept 2022) with recent
  monthly question and answer metrics from the Stack Exchange API. Fetches
  total questions and unanswered counts per month, computing answer rates
  from the difference.

  Uses append strategy with BRUIN_START_DATE / BRUIN_END_DATE to fetch only
  the requested interval. Deduplication is handled in staging.

  Data source: https://api.stackexchange.com/2.3
  License: CC BY-SA 4.0

materialization:
  type: table
  strategy: append

columns:
  - name: month
    type: DATE
    description: First day of the month
    primary_key: true
  - name: question_count
    type: INTEGER
    description: Total number of questions posted in this month
  - name: answered_count
    type: INTEGER
    description: Number of questions that received at least one answer
  - name: answer_rate_pct
    type: DOUBLE
    description: Percentage of questions with at least one answer
  - name: source
    type: VARCHAR
    description: Data source identifier (always 'stackexchange_api')
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was fetched from the API

@bruin"""

import calendar
import os
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import requests

SE_API_BASE = "https://api.stackexchange.com/2.3"


def generate_month_range(start_date: str, end_date: str) -> list[tuple[int, int]]:
    start = datetime.strptime(start_date, "%Y-%m-%d").replace(day=1)
    end = datetime.strptime(end_date, "%Y-%m-%d").replace(day=1)
    months = []
    current = start
    while current <= end:
        months.append((current.year, current.month))
        current += relativedelta(months=1)
    return months


def _month_timestamps(year: int, month: int):
    from_ts = int(datetime(year, month, 1).timestamp())
    last_day = calendar.monthrange(year, month)[1]
    to_ts = int(datetime(year, month, last_day, 23, 59, 59).timestamp())
    return from_ts, to_ts


def _api_total(endpoint: str, year: int, month: int) -> int | None:
    from_ts, to_ts = _month_timestamps(year, month)
    params = {
        "site": "stackoverflow",
        "fromdate": from_ts,
        "todate": to_ts,
        "filter": "total",
    }
    for attempt in range(5):
        try:
            resp = requests.get(f"{SE_API_BASE}/{endpoint}", params=params, timeout=30)
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            print(f"    Network error, retrying in {wait}s: {e}")
            time.sleep(wait)
            continue

        if resp.status_code in (400, 429, 502, 503):
            wait = 30 * (attempt + 1)
            print(f"    {resp.status_code} for {endpoint}, backing off {wait}s...")
            time.sleep(wait)
            continue

        resp.raise_for_status()
        data = resp.json()
        if "backoff" in data:
            time.sleep(data["backoff"])
        return data["total"]
    return None


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE")
    end_date = os.environ.get("BRUIN_END_DATE")
    print(f"Interval: {start_date} to {end_date}")

    months = generate_month_range(start_date, end_date)
    print(f"Months to fetch: {len(months)}")

    rows = []
    consecutive_failures = 0
    for year, month in months:
        total = _api_total("questions", year, month)
        time.sleep(0.15)
        if total is None:
            consecutive_failures += 1
            print(f"  {year}-{month:02d}: SKIPPED (API error)")
            if consecutive_failures >= 3:
                print("  Stopping early: likely rate-limited")
                break
            continue

        no_answers = _api_total("questions/no-answers", year, month)
        time.sleep(0.15)
        if no_answers is None:
            consecutive_failures += 1
            print(f"  {year}-{month:02d}: SKIPPED (API error on no-answers)")
            if consecutive_failures >= 3:
                print("  Stopping early: likely rate-limited")
                break
            continue

        consecutive_failures = 0
        answered = total - no_answers
        rate = round(answered / total * 100, 1) if total > 0 else 0.0

        rows.append({
            "month": datetime(year, month, 1).date(),
            "question_count": total,
            "answered_count": answered,
            "answer_rate_pct": rate,
            "source": "stackexchange_api",
        })
        print(f"  {year}-{month:02d}: {total:,} questions, {rate:.1f}% answered")

    if not rows:
        raise RuntimeError("No data fetched at all — API quota may be fully exhausted")

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now()
    print(f"\nTotal: {len(df)} months fetched")
    return df
