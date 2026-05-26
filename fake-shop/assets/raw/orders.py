"""@bruin
name: raw.orders
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Generates deterministic fake e-commerce orders for testing the self-healing
  pipeline skills. Same date inputs always produce the same data.

  Baseline behavior: 1500-3500 orders per day, distributed across 6 countries,
  6 product categories, with a realistic amount distribution.

  Injected issues (the agents should detect these):

  1. QUALITY-FAIL — duplicate_order_id
     Starting 2026-05-15, ~12 duplicate order_ids appear per day. The asset
     declares order_id as primary_key, so a `unique` check on order_id should
     catch this. Routes to data-quality-investigate.

  2. ANOMALY — country_concentration
     On 2026-05-20, country="TR" gets 10x its normal share. Total daily
     revenue spikes >2x. Single-dimension-driver pattern. Routes to
     anomaly-investigate.

  3. FRESHNESS — stalled_source
     For run dates within the last 2 days (today, today-1), returns an empty
     DataFrame. Simulates a source connector that stopped publishing.
     Routes to freshness-sla-check.

materialization:
  type: table
  strategy: append
  incremental_key: order_date

columns:
  - name: order_id
    type: VARCHAR
    description: Unique order identifier
    primary_key: true
    nullable: false
    checks:
      - name: unique
      - name: not_null
  - name: user_id
    type: VARCHAR
    description: Customer identifier
    nullable: false
  - name: product_id
    type: VARCHAR
    description: Product identifier
    nullable: false
  - name: country
    type: VARCHAR
    description: ISO 3166-1 alpha-2 country code
    nullable: false
  - name: amount_usd
    type: DOUBLE
    description: Order amount in USD
    nullable: false
    checks:
      - name: positive
  - name: created_at
    type: TIMESTAMP
    description: Order creation timestamp (UTC)
    nullable: false
  - name: order_date
    type: DATE
    description: Order date (UTC, derived from created_at)
    nullable: false

custom_checks:
  - name: daily_revenue_within_2x_28d_median
    description: |
      Daily revenue should not exceed 2x the 28-day rolling median.
      Failure on 2026-05-20 is expected (anomaly injection).
    query: |
      WITH daily AS (
        SELECT order_date, SUM(amount_usd) AS revenue
        FROM raw.orders
        GROUP BY 1
      ),
      with_baseline AS (
        SELECT
          d.order_date,
          d.revenue,
          APPROX_QUANTILES(b.revenue, 2)[OFFSET(1)] AS baseline
        FROM daily d
        LEFT JOIN daily b
          ON b.order_date >= DATE_SUB(d.order_date, INTERVAL 28 DAY)
         AND b.order_date < d.order_date
        GROUP BY d.order_date, d.revenue
      )
      SELECT COUNT(*)
      FROM with_baseline
      WHERE baseline IS NOT NULL AND revenue > 2 * baseline
    value: 0

@bruin"""

import hashlib
import os
import random
from datetime import date, datetime, timedelta

import pandas as pd

COUNTRIES = ["US", "GB", "DE", "FR", "TR", "BR"]
COUNTRY_WEIGHTS = [0.40, 0.15, 0.12, 0.10, 0.08, 0.15]
PRODUCT_IDS = [f"P{i:03d}" for i in range(1, 31)]
AMOUNT_BUCKETS = [(10, 30, 0.35), (30, 80, 0.40), (80, 200, 0.20), (200, 600, 0.05)]

ANOMALY_DATE = date(2026, 5, 20)
DUP_START_DATE = date(2026, 5, 15)
ORDER_COLUMNS = [
    "order_id", "user_id", "product_id", "country",
    "amount_usd", "created_at", "order_date",
]


def empty_orders_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "order_id": pd.Series(dtype="string"),
        "user_id": pd.Series(dtype="string"),
        "product_id": pd.Series(dtype="string"),
        "country": pd.Series(dtype="string"),
        "amount_usd": pd.Series(dtype="float64"),
        "created_at": pd.Series(dtype="datetime64[us]"),
        "order_date": pd.Series(dtype="datetime64[ns]"),
    })


def seed_for_date(d: date) -> int:
    return int(hashlib.sha256(d.isoformat().encode()).hexdigest()[:8], 16)


def generate_amount(rng: random.Random) -> float:
    r = rng.random()
    cum = 0.0
    for lo, hi, weight in AMOUNT_BUCKETS:
        cum += weight
        if r < cum:
            return round(rng.uniform(lo, hi), 2)
    return round(rng.uniform(10, 30), 2)


def generate_day(d: date) -> pd.DataFrame:
    rng = random.Random(seed_for_date(d))
    base_count = 1500 + int(2000 * rng.random())

    countries = list(COUNTRIES)
    weights = list(COUNTRY_WEIGHTS)

    if d == ANOMALY_DATE:
        tr_index = countries.index("TR")
        weights[tr_index] = weights[tr_index] * 10
        total = sum(weights)
        weights = [w / total for w in weights]
        base_count = int(base_count * 2.5)

    rows = []
    for i in range(base_count):
        order_id = f"O{d.strftime('%Y%m%d')}{i:05d}"
        country = rng.choices(countries, weights=weights, k=1)[0]
        rows.append({
            "order_id": order_id,
            "user_id": f"U{rng.randint(1, 50000):06d}",
            "product_id": rng.choice(PRODUCT_IDS),
            "country": country,
            "amount_usd": generate_amount(rng),
            "created_at": datetime.combine(d, datetime.min.time())
                          + timedelta(seconds=rng.randint(0, 86399)),
            "order_date": d,
        })

    if d >= DUP_START_DATE:
        dup_count = 12
        for _ in range(dup_count):
            victim = rng.choice(rows)
            rows.append({**victim, "user_id": f"U{rng.randint(1, 50000):06d}"})

    return pd.DataFrame(rows)


def materialize():
    start_str = os.environ.get("BRUIN_START_DATE", "2026-01-01")
    end_str = os.environ.get("BRUIN_END_DATE", date.today().isoformat())

    start = date.fromisoformat(start_str[:10])
    end = date.fromisoformat(end_str[:10])

    today = date.today()
    stale_window_start = today - timedelta(days=1)

    frames = []
    current = start
    while current <= end:
        if current >= stale_window_start:
            print(f"[fake-shop] {current}: simulated source stall — returning 0 rows")
        else:
            frames.append(generate_day(current))
        current += timedelta(days=1)

    if not frames:
        return empty_orders_frame()

    df = pd.concat(frames, ignore_index=True)[ORDER_COLUMNS]
    df["amount_usd"] = df["amount_usd"].astype("float64")
    df["created_at"] = pd.to_datetime(df["created_at"]).astype("datetime64[us]")
    df["order_date"] = pd.to_datetime(df["order_date"])
    print(f"[fake-shop] generated {len(df):,} orders across {df['order_date'].nunique()} days")
    return df
