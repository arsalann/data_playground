"""@bruin

name: contoso_v2_raw.shipments
description: |
  Order fulfillment shipments for Contoso v2, 2010-01-01 to 2026-05-01. One
  shipment per order. Carrier mix shifts over time: FedEx 35→25%, UPS 30→25%,
  DHL 15→10%, USPS 15→25%, Amazon Logistics 0→15% (emerging 2018+). Transit
  days are gamma-distributed (median ~4d, p95 ~9d, p99 ~21d) and stretch
  during the 2020 supply-chain shock (+12-16d) and 2022 holiday crunch (+5-7d).
  Status mix: 88% Delivered baseline, drops to 82% during COVID; 4% Returned,
  1% Lost, 0.3% truly stuck (>90d In Transit).
connection: gcp-default
instance: b1.large
tags:
  - operations
  - logistics
  - fact_table

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: shipment_key
    type: INTEGER
    primary_key: true
    description: Sparse shipment identifier.
    checks:
      - name: not_null
      - name: unique
  - name: order_key
    type: INTEGER
    description: Foreign key to orders (1:1).
    checks:
      - name: not_null
  - name: ship_date
    type: TIMESTAMP
    description: Dispatch timestamp (0-3 days after order_date).
    checks:
      - name: not_null
  - name: delivery_date
    type: TIMESTAMP
    description: Delivery timestamp (NULL if Lost or stuck In Transit).
  - name: carrier
    type: VARCHAR
    description: FedEx, UPS, DHL, USPS, or Amazon Logistics.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - FedEx
          - UPS
          - DHL
          - USPS
          - Amazon Logistics
  - name: tracking_number
    type: VARCHAR
    description: Carrier-prefixed tracking reference.
    checks:
      - name: not_null
  - name: shipment_status
    type: VARCHAR
    description: Delivered, Returned, Lost, or In Transit.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Delivered
          - Returned
          - Lost
          - In Transit
  - name: transit_days
    type: INTEGER
    description: Days from ship_date to delivery_date.
  - name: ship_cost
    type: NUMERIC
    description: Shipping cost in USD.
    checks:
      - name: not_null
      - name: non_negative
  - name: currency
    type: VARCHAR
    description: Always USD.
    checks:
      - name: not_null
  - name: store_key
    type: INTEGER
    description: Foreign key to stores (fulfilling location).
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP

@bruin"""

import logging
import os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import (
    build_orders, event_impact, get_seeded_rng, regime_for, sparse_keys,
)

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


CARRIERS = ["FedEx", "UPS", "DHL", "USPS", "Amazon Logistics"]


def _carrier_weights_for_year(year: int) -> np.ndarray:
    """Linear interp between 2010 and 2024 anchors."""
    # 2010: FedEx 35, UPS 30, DHL 15, USPS 20, Amazon 0
    # 2024: FedEx 25, UPS 25, DHL 10, USPS 25, Amazon 15
    if year < 2018:
        # No Amazon Logistics before 2018
        frac = max(0.0, (year - 2010) / 8.0)
        w = np.array([
            0.35 - 0.03 * frac,
            0.30 - 0.02 * frac,
            0.15 - 0.02 * frac,
            0.20 + 0.07 * frac,
            0.0,
        ])
    else:
        frac = max(0.0, min(1.0, (year - 2018) / 6.0))
        w = np.array([
            0.32 - 0.07 * frac,
            0.28 - 0.03 * frac,
            0.13 - 0.03 * frac,
            0.22 + 0.03 * frac,
            0.05 + 0.10 * frac,
        ])
    return w / w.sum()


def materialize():
    rng = get_seeded_rng(151)
    orders = build_orders()
    n = len(orders)
    logger.info("Generating shipments for %d orders", n)

    order_keys = orders["order_key"].values
    store_keys = orders["store_key"].values
    order_dates = pd.to_datetime(orders["order_date"]).values
    statuses = orders["status"].values

    sparse_iter = iter(sparse_keys(int(n * 1.10), gap_rate=0.06, start=1, seed=151))

    # Pre-compute per-year carrier weights
    years = pd.to_datetime(orders["order_date"]).dt.year.values
    year_to_weights = {y: _carrier_weights_for_year(int(y)) for y in np.unique(years)}

    rows = []
    for i in range(n):
        ok = int(order_keys[i])
        sk = int(store_keys[i])
        odt = pd.Timestamp(order_dates[i]).to_pydatetime()
        order_status = statuses[i]
        d = odt.date()

        # Skip Pending / Cancelled orders — no shipment generated
        if order_status in ("Pending", "Cancelled"):
            continue

        # Ship date 0-3 days after order
        ship_offset = int(rng.integers(0, 4))
        ship_dt = odt + timedelta(days=ship_offset)

        # Carrier mix by year
        carrier = str(rng.choice(CARRIERS, p=year_to_weights[years[i]]))

        # Transit gamma — base shape=2.5 scale=2.0 → median ~4d, p95 ~9d
        transit = float(rng.gamma(shape=2.5, scale=2.0))
        # Regime impact on transit
        transit *= event_impact(d, "transit")
        # Tail: 1% gets long tail
        if rng.random() < 0.01:
            transit *= float(rng.uniform(2.0, 4.0))
        transit_days = max(1, int(round(transit)))

        delivery_dt = ship_dt + timedelta(days=transit_days)

        # Status mix
        regime = regime_for(d)
        delivered_p = 0.82 if regime == "covid_shock" else 0.88
        r = rng.random()
        if order_status == "Returned" or r > delivered_p + 0.10:
            shipment_status = "Lost"
            delivery_dt = None
        elif r > delivered_p + 0.06:
            shipment_status = "In Transit"
            # Some never delivered
            if transit_days > 90:
                delivery_dt = None
        elif r > delivered_p:
            shipment_status = "Returned"
        else:
            shipment_status = "Delivered"

        # Ship cost: base $5-30, +20-40% during COVID
        cost = float(rng.uniform(5.0, 30.0))
        if regime == "covid_shock":
            cost *= float(rng.uniform(1.20, 1.40))
        cost = round(cost, 2)

        # Tracking number
        prefix = "".join(c for c in carrier.upper() if c.isalpha())[:3]
        tracking = f"{prefix}{int(rng.integers(100_000_000, 999_999_999))}"

        rows.append({
            "shipment_key": int(next(sparse_iter)),
            "order_key": ok,
            "ship_date": ship_dt,
            "delivery_date": delivery_dt,
            "carrier": carrier,
            "tracking_number": tracking,
            "shipment_status": shipment_status,
            "transit_days": transit_days if delivery_dt is not None else None,
            "ship_cost": cost,
            "currency": "USD",
            "store_key": sk,
        })

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d shipment records", len(df))
    return df
