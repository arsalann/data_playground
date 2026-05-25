"""@bruin
name: raw.pageviews
type: python
image: python:3.11
connection: duckdb-default
description: |
  Generates deterministic fake web pageview events. Baseline: ~40k events per
  day across 6 countries, 4 browsers, 3 devices, 8 page paths.

  Injected issues:

  1. ANOMALY — single_dimension_driver
     On 2026-05-18, country=ID (Indonesia) gets 8x its normal share. Daily
     pageviews spike ~3x. anomaly-investigate should attribute the bulk of
     the delta to ID.

  2. NEW-SEGMENT — browser_arc
     Browser "Arc" did not exist before 2026-05-15. Starting that date it
     appears with ~3% share. schema-drift-check classifies this as
     enum-value-added; anomaly-investigate may also pick it up.

  3. FRESHNESS — multi_day_gap
     For run dates within the last 3 days (today, today-1, today-2), returns
     an empty DataFrame. Routes to freshness-sla-check.

materialization:
  type: table
  strategy: append
  incremental_key: event_date

columns:
  - name: session_id
    type: VARCHAR
    description: Session identifier
    nullable: false
  - name: user_id
    type: VARCHAR
    description: User identifier (anonymous if not logged in)
    nullable: false
  - name: country
    type: VARCHAR
    description: ISO 3166-1 alpha-2 country code
    nullable: false
  - name: browser
    type: VARCHAR
    description: Browser name
    nullable: false
  - name: device
    type: VARCHAR
    description: Device class (desktop, mobile, tablet)
    nullable: false
  - name: page_path
    type: VARCHAR
    description: URL path
    nullable: false
  - name: event_time
    type: TIMESTAMP
    description: Pageview timestamp (UTC)
    nullable: false
  - name: event_date
    type: DATE
    description: Pageview date (UTC, derived)
    nullable: false

@bruin"""

import hashlib
import os
import random
from datetime import date, datetime, timedelta

import pandas as pd

COUNTRIES = ["US", "GB", "DE", "FR", "ID", "BR"]
BASE_COUNTRY_WEIGHTS = [0.35, 0.18, 0.12, 0.10, 0.05, 0.20]
BROWSERS_PRE = ["Chrome", "Safari", "Firefox", "Edge"]
BROWSER_WEIGHTS_PRE = [0.62, 0.20, 0.10, 0.08]
BROWSERS_POST = ["Chrome", "Safari", "Firefox", "Edge", "Arc"]
BROWSER_WEIGHTS_POST = [0.60, 0.19, 0.10, 0.08, 0.03]
DEVICES = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [0.45, 0.50, 0.05]
PAGE_PATHS = ["/", "/products", "/blog", "/pricing", "/about", "/login", "/signup", "/help"]

ANOMALY_DATE = date(2026, 5, 18)
NEW_BROWSER_DATE = date(2026, 5, 15)


def seed_for_date(d: date) -> int:
    return int(hashlib.sha256(d.isoformat().encode()).hexdigest()[:8], 16)


def generate_day(d: date) -> pd.DataFrame:
    rng = random.Random(seed_for_date(d))
    count = 35000 + int(10000 * rng.random())

    countries = list(COUNTRIES)
    weights = list(BASE_COUNTRY_WEIGHTS)

    if d == ANOMALY_DATE:
        id_index = countries.index("ID")
        weights[id_index] = weights[id_index] * 8
        total = sum(weights)
        weights = [w / total for w in weights]
        count = int(count * 2.2)

    if d >= NEW_BROWSER_DATE:
        browsers, browser_weights = BROWSERS_POST, BROWSER_WEIGHTS_POST
    else:
        browsers, browser_weights = BROWSERS_PRE, BROWSER_WEIGHTS_PRE

    rows = []
    for _ in range(count):
        rows.append({
            "session_id": f"S{rng.randint(1, 999999):07d}",
            "user_id": f"U{rng.randint(1, 200000):07d}",
            "country": rng.choices(countries, weights=weights, k=1)[0],
            "browser": rng.choices(browsers, weights=browser_weights, k=1)[0],
            "device": rng.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0],
            "page_path": rng.choice(PAGE_PATHS),
            "event_time": datetime.combine(d, datetime.min.time())
                          + timedelta(seconds=rng.randint(0, 86399)),
            "event_date": d,
        })

    return pd.DataFrame(rows)


def materialize():
    start_str = os.environ.get("BRUIN_START_DATE", "2026-01-01")
    end_str = os.environ.get("BRUIN_END_DATE", date.today().isoformat())

    start = date.fromisoformat(start_str[:10])
    end = date.fromisoformat(end_str[:10])

    today = date.today()
    stale_window_start = today - timedelta(days=2)

    frames = []
    current = start
    while current <= end:
        if current >= stale_window_start:
            print(f"[fake-webevents] {current}: simulated source stall — returning 0 rows")
        else:
            frames.append(generate_day(current))
        current += timedelta(days=1)

    if not frames:
        return pd.DataFrame(columns=[
            "session_id", "user_id", "country", "browser",
            "device", "page_path", "event_time", "event_date",
        ])

    df = pd.concat(frames, ignore_index=True)
    print(f"[fake-webevents] generated {len(df):,} events across {df['event_date'].nunique()} days")
    return df
