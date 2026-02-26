"""@bruin
name: raw.stackoverflow_tags_api_monthly
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Supplements the BigQuery public dataset with per-tag monthly question counts
  from the Stack Exchange API. Fetches data for the top 15 all-time tags within
  the BRUIN_START_DATE / BRUIN_END_DATE interval.

  Uses append strategy so subsequent runs for different intervals accumulate
  data without replacing existing rows. Deduplication is handled in staging.

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
  - name: tag
    type: VARCHAR
    description: Programming language or technology tag
    primary_key: true
  - name: question_count
    type: INTEGER
    description: Number of questions with this tag in the given month
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

SE_API = "https://api.stackexchange.com/2.3/questions"

TOP_TAGS = [
    "javascript", "python", "java", "c#", "php",
    "android", "html", "jquery", "c++", "css",
    "ios", "mysql", "r", "reactjs", "node.js",
]


def generate_month_range(start_date: str, end_date: str) -> list[tuple[int, int]]:
    start = datetime.strptime(start_date, "%Y-%m-%d").replace(day=1)
    end = datetime.strptime(end_date, "%Y-%m-%d").replace(day=1)
    months = []
    current = start
    while current <= end:
        months.append((current.year, current.month))
        current += relativedelta(months=1)
    return months


def fetch_tag_count(tag: str, year: int, month: int) -> int:
    from_ts = int(datetime(year, month, 1).timestamp())
    last_day = calendar.monthrange(year, month)[1]
    to_ts = int(datetime(year, month, last_day, 23, 59, 59).timestamp())

    for attempt in range(5):
        try:
            resp = requests.get(
                SE_API,
                params={
                    "site": "stackoverflow",
                    "tagged": tag,
                    "fromdate": from_ts,
                    "todate": to_ts,
                    "filter": "total",
                },
                timeout=30,
            )
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            print(f"    Network error for {tag}, retrying in {wait}s: {e}")
            time.sleep(wait)
            continue

        if resp.status_code in (400, 429, 502, 503):
            wait = 30 * (attempt + 1)
            print(f"    {resp.status_code} for {tag}, backing off {wait}s...")
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
    print(f"Months to fetch: {len(months)} x {len(TOP_TAGS)} tags = {len(months) * len(TOP_TAGS)} API calls")

    rows = []
    consecutive_failures = 0
    for year, month in months:
        month_ok = 0
        for tag in TOP_TAGS:
            count = fetch_tag_count(tag, year, month)
            if count is None:
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    print(f"  Stopping early: {consecutive_failures} consecutive failures (likely rate-limited)")
                    break
                continue
            consecutive_failures = 0
            rows.append({
                "month": datetime(year, month, 1).date(),
                "tag": tag,
                "question_count": count,
            })
            month_ok += 1
            time.sleep(0.25)

        print(f"  {year}-{month:02d}: fetched {month_ok}/{len(TOP_TAGS)} tags")
        if consecutive_failures >= 3:
            break

    if not rows:
        raise RuntimeError("No data fetched at all — API quota may be fully exhausted")

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now()
    print(f"\nTotal: {len(df)} rows fetched (some months may be partial)")
    return df
