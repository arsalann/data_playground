"""@bruin
name: self_heal_test_raw.products
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Generates a deterministic fake product catalog. Re-emits the full catalog on
  every run with an extraction timestamp. Downstream staging deduplicates to the
  latest row per product so agents can rerun this fake fixture safely.

  Injected issue:

  SCHEMA-DRIFT — column_rename
    Before 2026-05-18 the catalog column is named `category`. From 2026-05-18
    onward, the same data is emitted under `product_category`. The downstream
    self_heal_test_staging.daily_revenue asset still references `category`, so any run with
    BRUIN_END_DATE >= 2026-05-18 will surface a "column not found" error.
    Routes to schema-drift-check → maintenance-pr.

materialization:
  type: table
  strategy: append

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
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this fake catalog row was generated.
    nullable: false

custom_checks:
  - name: no_product_category_drift_column
    description: |
      The live self_heal_test_raw.products table should not contain product_category while the
      declared source contract still expects category. Failure after
      2026-05-18 is expected (schema drift injection).
    query: |
      SELECT COUNT(*)
      FROM self_heal_test_raw.INFORMATION_SCHEMA.COLUMNS
      WHERE table_name = 'products'
        AND column_name = 'product_category'
    value: 0

@bruin"""

import os
import random
from datetime import date, datetime, timezone

import pandas as pd

CATEGORIES = ["apparel", "electronics", "home", "books", "outdoors", "beauty"]
DRIFT_DATE = date(2026, 5, 18)


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
    df["extracted_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    df["extracted_at"] = pd.to_datetime(df["extracted_at"]).astype("datetime64[us]")

    end_str = os.environ.get("BRUIN_END_DATE", date.today().isoformat())
    end = date.fromisoformat(end_str[:10])

    if end >= DRIFT_DATE:
        df = df.rename(columns={"category": "product_category"})
        print(f"[self-heal-shop] schema drift active: 'category' → 'product_category' (end_date={end})")

    print(f"[self-heal-shop] emitted {len(df)} products with columns {list(df.columns)}")
    return df
