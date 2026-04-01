"""@bruin

name: contoso_raw.inventory_snapshots
description: |
  Monthly inventory snapshots for Contoso retail operations (synthetic data).

  Captures point-in-time stock levels across all store-product combinations on the first
  of each month from 2016-2026. Part of Contoso's multi-department simulation representing
  a consumer electronics retailer.

  The dataset reflects realistic inventory management patterns:
  - Automated reordering when stock falls below reorder points
  - Variable stock levels influenced by store size and product popularity
  - Cross-references authentic Contoso V2 store and product keys for department integration

  Generated deterministically with seed=42 using a subset of ~200 top products across
  74 retail locations (~50 products per store average). Actual row count: ~488K records.

  Business applications: stock-out risk analysis, reorder optimization, demand forecasting,
  inventory turnover calculations, and cross-department operational reporting.
connection: bruin-playground-eu
instance: b1.medium
tags:
  - domain:operations
  - data_type:fact_table
  - department:operations
  - sensitivity:internal
  - pipeline_role:raw
  - update_pattern:monthly_snapshot
  - source_type:synthetic

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: snapshot_key
    type: INTEGER
    description: Unique sequential identifier for each inventory snapshot record
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: snapshot_date
    type: DATE
    description: |
      Snapshot date (first day of month only).
      Date range: 2016-01-01 to 2026-12-01 (132 unique months)
    checks:
      - name: not_null
  - name: store_key
    type: INTEGER
    description: |
      Foreign key referencing contoso_raw.stores table.
      Links to one of 74 Contoso retail locations worldwide
    checks:
      - name: not_null
  - name: product_key
    type: INTEGER
    description: |
      Foreign key referencing contoso_raw.products table.
      Subset of ~200 top products from 2,517 total Contoso product catalog
    checks:
      - name: not_null
  - name: quantity_on_hand
    type: INTEGER
    description: |
      Current inventory units physically available in store.
      Range: 0-499 units (randomly distributed)
    checks:
      - name: not_null
      - name: non_negative
  - name: reorder_point
    type: INTEGER
    description: |
      Minimum stock threshold that triggers automatic reordering.
      Range: 10-99 units. When quantity_on_hand falls below this level,
      the system places orders with suppliers
    checks:
      - name: not_null
      - name: positive
  - name: quantity_on_order
    type: INTEGER
    description: |
      Units currently ordered from suppliers but not yet received.
      Range: 0-299 units. Non-zero only when quantity_on_hand < reorder_point
    checks:
      - name: not_null
      - name: non_negative
  - name: extracted_at
    type: TIMESTAMP
    description: |
      UTC timestamp when this synthetic dataset was generated.
      All records share the same extraction timestamp for consistency
    checks:
      - name: not_null

@bruin"""

import logging
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import load_contoso_keys, seed_all

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def materialize():
    seed_all(42)
    rng = np.random.default_rng(42)
    keys = load_contoso_keys()
    store_keys = keys["store_keys"]
    product_keys = keys["product_keys"]
    min_date, max_date = keys["date_range"]

    # Generate monthly snapshot dates
    months = pd.date_range(
        start=pd.Timestamp(min_date).replace(day=1),
        end=pd.Timestamp(max_date),
        freq="MS",
    )

    # Sample a subset of store-product combos (not all 74*2517 = ~186K combos)
    # Use top 200 products and all stores for ~50 products per store on average
    n_products_per_store = 50
    sampled_products = rng.choice(product_keys, size=min(200, len(product_keys)), replace=False)

    logger.info(
        "Generating inventory snapshots for %d stores x %d products x %d months...",
        len(store_keys), len(sampled_products), len(months),
    )

    records = []
    snapshot_key = 0

    for month_date in months:
        for store_key in store_keys:
            # Each store carries a random subset of sampled products
            store_products = rng.choice(
                sampled_products,
                size=min(n_products_per_store, len(sampled_products)),
                replace=False,
            )

            for product_key in store_products:
                snapshot_key += 1
                qty_on_hand = int(rng.integers(0, 500))
                reorder_point = int(rng.integers(10, 100))
                qty_on_order = 0
                if qty_on_hand < reorder_point:
                    qty_on_order = int(rng.integers(50, 300))

                records.append({
                    "snapshot_key": snapshot_key,
                    "snapshot_date": month_date.date(),
                    "store_key": int(store_key),
                    "product_key": int(product_key),
                    "quantity_on_hand": qty_on_hand,
                    "reorder_point": reorder_point,
                    "quantity_on_order": qty_on_order,
                })

    df = pd.DataFrame(records)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d inventory snapshot records", len(df))
    return df
