"""@bruin
name: raw.stackoverflow_api_monthly
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Supplements the BigQuery public dataset (frozen at Sept 2022) with recent
  monthly question counts from the Stack Exchange API. Fetches total questions
  per month from October 2022 to present.

  Data source: https://api.stackexchange.com/2.3
  License: CC BY-SA 4.0

materialization:
  type: table
  strategy: create+replace

columns:
  - name: month
    type: DATE
    description: First day of the month
    primary_key: true
  - name: question_count
    type: INTEGER
    description: Total number of questions posted in this month
  - name: source
    type: VARCHAR
    description: Data source identifier (always 'stackexchange_api')
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was fetched from the API

@bruin"""

import calendar
import time
from datetime import datetime, date

import pandas as pd
import requests

SE_API = "https://api.stackexchange.com/2.3/questions"
START_YEAR = 2022
START_MONTH = 10


def fetch_monthly_count(year: int, month: int) -> int:
    from_ts = int(datetime(year, month, 1).timestamp())
    last_day = calendar.monthrange(year, month)[1]
    to_ts = int(datetime(year, month, last_day, 23, 59, 59).timestamp())

    resp = requests.get(
        SE_API,
        params={
            "site": "stackoverflow",
            "fromdate": from_ts,
            "todate": to_ts,
            "filter": "total",
            "pagesize": 0,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["total"]


def materialize():
    today = date.today()
    rows = []

    year, month = START_YEAR, START_MONTH
    while (year, month) <= (today.year, today.month):
        count = fetch_monthly_count(year, month)
        rows.append({
            "month": f"{year}-{month:02d}-01",
            "question_count": count,
            "source": "stackexchange_api",
        })
        print(f"  {year}-{month:02d}: {count:,} questions")

        month += 1
        if month > 12:
            month = 1
            year += 1
        time.sleep(0.25)

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now()
    print(f"\nTotal: {len(df)} months fetched from Stack Exchange API")
    return df
