"""@bruin
name: public.orders
type: python
image: python:3.11
connection: ingestr-cli-v1-pg
description: |
  Synthetic order generator for the ingestr-cli-v1 pipeline. For every calendar
  month whose first day falls within [BRUIN_START_DATE, BRUIN_END_DATE], it
  emits exactly 100,000 rows with timestamps randomly distributed across the
  days of that month, and appends them to the postgres `public.orders` table.
  Running across 10 months therefore yields 1,000,000 rows.

materialization:
  type: table
  strategy: append

columns:
  - name: order_id
    type: VARCHAR
    primary_key: true
    description: Globally unique order identifier (deterministic per month).
  - name: customer_id
    type: INTEGER
    description: Synthetic customer id (1..10_000).
  - name: amount
    type: DOUBLE
    description: Order total in USD.
  - name: status
    type: VARCHAR
    description: Order status (placed, paid, shipped, delivered, cancelled).
  - name: created_at
    type: TIMESTAMP
    description: When the order was created (random instant in its month).
  - name: updated_at
    type: TIMESTAMP
    description: Last-update timestamp; used as the ingestr incremental key.
@bruin"""

import hashlib
import os
import random
from calendar import monthrange
from datetime import datetime, timedelta

import pandas as pd

ROWS_PER_MONTH = 100_000
STATUSES = ["placed", "paid", "shipped", "delivered", "cancelled"]


def _parse(d: str) -> datetime:
    return datetime.strptime(d[:10], "%Y-%m-%d")


def _months_in_range(start: datetime, end: datetime):
    cur = datetime(start.year, start.month, 1)
    while cur <= end:
        yield cur.year, cur.month
        if cur.month == 12:
            cur = datetime(cur.year + 1, 1, 1)
        else:
            cur = datetime(cur.year, cur.month + 1, 1)


def materialize():
    start = _parse(os.environ["BRUIN_START_DATE"])
    end = _parse(os.environ["BRUIN_END_DATE"])
    if end < start:
        end = start

    rows = []
    for year, month in _months_in_range(start, end):
        days_in_month = monthrange(year, month)[1]
        month_start = datetime(year, month, 1)
        span_seconds = days_in_month * 24 * 3600

        seed_src = f"{year:04d}-{month:02d}".encode()
        seed = int(hashlib.sha256(seed_src).hexdigest(), 16) % (2**32)
        rng = random.Random(seed)
        tag = f"{year:04d}{month:02d}"

        for i in range(ROWS_PER_MONTH):
            ts = month_start + timedelta(seconds=rng.randrange(span_seconds))
            rows.append(
                {
                    "order_id": f"ord-{tag}-{i:06d}",
                    "customer_id": rng.randint(1, 10_000),
                    "amount": round(rng.uniform(5.0, 500.0), 2),
                    "status": STATUSES[rng.randrange(len(STATUSES))],
                    "created_at": ts,
                    "updated_at": ts,
                }
            )

    return pd.DataFrame(rows)
