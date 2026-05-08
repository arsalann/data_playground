"""@bruin

name: contoso_v2_raw.inventory_snapshots
description: |
  Monthly inventory snapshots per store-product, 2010-01-01 to 2026-05-01.
  Captures the top ~150 products carried in each open store on the 1st of
  every month. Quantities follow seasonal patterns: October stock-up (+60%),
  January draw-down (-30%); stockouts spike during 2020-03 to 2020-09 (12%
  vs 2% baseline) and 2022-Q3 supply chain (8%). Reorder points are
  proportional to product velocity (proxy: price / cost ratio plus
  category bias). Quantity-on-order is non-zero only when on-hand <
  reorder point.
connection: gcp-default
instance: b1.medium
tags:
  - operations
  - inventory
  - fact_table

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: snapshot_key
    type: INTEGER
    primary_key: true
    description: Sparse snapshot identifier.
    checks:
      - name: not_null
      - name: unique
  - name: snapshot_date
    type: DATE
    description: Snapshot date (1st of month).
    checks:
      - name: not_null
  - name: store_key
    type: INTEGER
    description: Foreign key to stores.
    checks:
      - name: not_null
  - name: product_key
    type: INTEGER
    description: Foreign key to products.
    checks:
      - name: not_null
  - name: quantity_on_hand
    type: INTEGER
    description: Units on shelf.
    checks:
      - name: not_null
      - name: non_negative
  - name: reorder_point
    type: INTEGER
    description: Reorder threshold.
    checks:
      - name: not_null
      - name: non_negative
  - name: quantity_on_order
    type: INTEGER
    description: Units already ordered from suppliers (non-zero only when below reorder point).
    checks:
      - name: not_null
      - name: non_negative
  - name: is_stockout
    type: BOOLEAN
    description: True when quantity_on_hand == 0.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP

@bruin"""

import logging
import os
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd

import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import (
    PIPELINE_END, PIPELINE_START, build_products, build_stores,
    get_seeded_rng, regime_for, sparse_keys,
)

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


def _stockout_rate(d: date) -> float:
    regime = regime_for(d)
    if regime == "covid_shock":
        return 0.12
    if regime == "recovery" and d <= date(2020, 9, 30):
        return 0.10
    if regime == "inflation" and d.month in (7, 8, 9):
        return 0.08
    return 0.02


def _seasonal_stock_mult(month: int) -> float:
    return {
        1: 0.70, 2: 0.85, 3: 1.00, 4: 1.05, 5: 1.05, 6: 1.00,
        7: 1.05, 8: 1.10, 9: 1.30, 10: 1.60, 11: 1.10, 12: 0.85,
    }[month]


def materialize():
    rng = get_seeded_rng(157)
    stores = build_stores()
    products = build_products()

    # Choose top ~150 products by price (proxy for value-density velocity)
    products_sorted = products.sort_values("price", ascending=False).head(150).copy()
    products_sorted = products_sorted.reset_index(drop=True)

    prod_keys = products_sorted["product_key"].values
    prod_first = pd.to_datetime(products_sorted["first_listed_date"]).dt.date.values
    prod_disc = pd.to_datetime(products_sorted["discontinued_date"]).dt.date.values
    prod_velocity = products_sorted["price"].values / products_sorted["cost"].values

    store_keys = stores["store_key"].values
    store_open = pd.to_datetime(stores["open_date"]).dt.date.values
    store_close = pd.to_datetime(stores["close_date"]).dt.date.values
    store_temp_start = pd.to_datetime(stores["temp_closed_start"]).dt.date.values
    store_temp_end = pd.to_datetime(stores["temp_closed_end"]).dt.date.values

    # Each store carries a deterministic subset (~50 products)
    n_per_store = 50
    store_assortment: dict[int, np.ndarray] = {}
    for sk in store_keys:
        idx = rng.choice(len(prod_keys), size=n_per_store, replace=False)
        store_assortment[int(sk)] = idx

    # Monthly snapshot dates
    months = pd.date_range(
        start=pd.Timestamp(PIPELINE_START).replace(day=1),
        end=pd.Timestamp(PIPELINE_END),
        freq="MS",
    )

    sparse_iter = iter(sparse_keys(3_500_000, gap_rate=0.04, start=1, seed=157))
    rows = []

    for ts in months:
        d = ts.date()
        seasonal = _seasonal_stock_mult(d.month)
        stockout_p = _stockout_rate(d)

        for s_i, sk in enumerate(store_keys):
            sk_int = int(sk)
            # Skip if store not open or already closed
            if store_open[s_i] is None or store_open[s_i] > d:
                continue
            if store_close[s_i] is not None and not pd.isna(store_close[s_i]) and store_close[s_i] < d:
                continue
            # Skip if temporarily closed
            ts_start = store_temp_start[s_i]
            ts_end = store_temp_end[s_i]
            if (ts_start is not None and not pd.isna(ts_start)
                    and ts_end is not None and not pd.isna(ts_end)
                    and ts_start <= d <= ts_end):
                continue

            for p_i in store_assortment[sk_int]:
                # Skip if product not yet listed or discontinued
                if prod_first[p_i] > d:
                    continue
                disc = prod_disc[p_i]
                if disc is not None and not pd.isna(disc) and disc < d:
                    continue

                vel = float(prod_velocity[p_i])
                base_qty = max(20, int(80 * seasonal / max(0.5, vel * 0.4)))
                qty = int(rng.gamma(shape=4.0, scale=base_qty / 4.0))
                if rng.random() < stockout_p:
                    qty = 0

                reorder_point = max(5, int(base_qty * 0.30
                                           * float(rng.uniform(0.7, 1.3))))
                qty_on_order = 0
                if qty < reorder_point and rng.random() < 0.85:
                    qty_on_order = int(rng.integers(reorder_point,
                                                    reorder_point * 4))

                rows.append({
                    "snapshot_key": int(next(sparse_iter)),
                    "snapshot_date": d,
                    "store_key": sk_int,
                    "product_key": int(prod_keys[p_i]),
                    "quantity_on_hand": qty,
                    "reorder_point": reorder_point,
                    "quantity_on_order": qty_on_order,
                    "is_stockout": qty == 0,
                })

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d inventory snapshots", len(df))
    return df
