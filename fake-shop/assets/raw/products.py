"""@bruin
name: raw.products
type: python
image: python:3.11
connection: duckdb-default
description: |
  Generates a deterministic fake product catalog. Re-emits the full catalog on
  every run (create+replace), so downstream assets always see the current shape.

  Injected issue:

  SCHEMA-DRIFT — column_rename
    Before 2026-04-01 the catalog column is named `category`. From 2026-04-01
    onward, the same data is emitted under `product_category`. The downstream
    staging.daily_revenue asset still references `category`, so any run with
    BRUIN_END_DATE >= 2026-04-01 will surface a "column not found" error.
    Routes to schema-drift-check → maintenance-pr.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: product_id
    type: VARCHAR
    description: Product identifier
    primary_key: true
    nullable: false
  - name: name
    type: VARCHAR
    description: Product display name
    nullable: false
  - name: category
    type: VARCHAR
    description: Product category (pre-drift name; post-drift this column is renamed)
  - name: price_usd
    type: DOUBLE
    description: List price in USD
    nullable: false

@bruin"""

import os
import random
from datetime import date

import pandas as pd

CATEGORIES = ["apparel", "electronics", "home", "books", "outdoors", "beauty"]
DRIFT_DATE = date(2026, 4, 1)


def materialize():
    rng = random.Random(42)
    rows = []
    for i in range(1, 31):
        rows.append({
            "product_id": f"P{i:03d}",
            "name": f"Product {i:03d}",
            "category": rng.choice(CATEGORIES),
            "price_usd": round(rng.uniform(10, 400), 2),
        })

    df = pd.DataFrame(rows)

    end_str = os.environ.get("BRUIN_END_DATE", date.today().isoformat())
    end = date.fromisoformat(end_str[:10])

    if end >= DRIFT_DATE:
        df = df.rename(columns={"category": "product_category"})
        print(f"[fake-shop] schema drift active: 'category' → 'product_category' (end_date={end})")

    print(f"[fake-shop] emitted {len(df)} products with columns {list(df.columns)}")
    return df
