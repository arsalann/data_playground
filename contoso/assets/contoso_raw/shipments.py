"""@bruin

name: contoso_raw.shipments
description: |
  Order fulfillment and shipping records for Contoso consumer electronics retailer.

  Contains one-to-one shipping records for customer orders, tracking carrier selection,
  dispatch timing, delivery performance, and shipping costs. Each order generates exactly
  one shipment record with realistic carrier distribution (FedEx 30%, UPS 30%, DHL 25%, USPS 15%)
  and status outcomes (93% delivered, 5% returned, 2% lost).

  Ships are typically dispatched 0-3 days after order placement with 2-14 day transit times.
  Shipping costs range from $5-45 USD based on realistic fulfillment scenarios. All tracking
  numbers follow carrier-specific formats with carrier prefix and 9-digit reference numbers.

  This synthetic dataset supports Operations team analysis of shipping performance, carrier SLA
  monitoring, delivery time optimization, and logistics cost analysis. Links to core sales
  data via OrderKey and fulfillment locations via StoreKey.

  Generated deterministically with seed=42 for reproducible analysis (~980K rows).
connection: gcp-default
tags:
  - operations
  - logistics
  - shipping
  - fulfillment
  - fact_table
  - synthetic
  - daily_batch

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: ShipmentKey
    type: INTEGER
    description: Unique shipment identifier (sequential 1 to N)
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: OrderKey
    type: INTEGER
    description: Foreign key linking to orders table (1:1 relationship) - identifies which customer order is being shipped
    checks:
      - name: not_null
  - name: ShipDate
    type: TIMESTAMP
    description: Date and time when the shipment was dispatched from the fulfillment center (0-3 days after order date)
    checks:
      - name: not_null
  - name: DeliveryDate
    type: TIMESTAMP
    description: Actual date and time when the shipment was delivered to the customer (2-14 days after ship date)
    checks:
      - name: not_null
  - name: Carrier
    type: STRING
    description: Shipping carrier name (FedEx, UPS, DHL, or USPS) - selected based on weighted distribution favoring premium carriers
    checks:
      - name: not_null
      - name: accepted_values
  - name: TrackingNumber
    type: STRING
    description: Carrier-specific tracking reference number (12 characters) - format is 3-letter carrier code + 9-digit number
    checks:
      - name: not_null
      - name: unique
  - name: ShipmentStatus
    type: STRING
    description: Final shipment outcome status - reflects realistic logistics outcomes with 93% delivery success rate
    checks:
      - name: not_null
      - name: accepted_values
  - name: ShipCost
    type: FLOAT64
    description: Shipping cost charged to customer in USD - ranges from $5 to $45 based on realistic fulfillment pricing
    checks:
      - name: not_null
      - name: positive
  - name: Currency
    type: STRING
    description: Currency code for shipping cost (always USD for Contoso operations)
    checks:
      - name: not_null
      - name: accepted_values
  - name: StoreKey
    type: INTEGER
    description: Foreign key to stores table identifying which physical location fulfilled the order - enables geographic analysis of fulfillment patterns
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: ETL metadata timestamp indicating when this record was ingested into the data warehouse (UTC timezone)

@bruin"""

import logging
import os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import load_contoso_keys, seed_all

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

CARRIERS = ["FedEx", "UPS", "DHL", "USPS"]
CARRIER_WEIGHTS = [0.30, 0.30, 0.25, 0.15]


def materialize():
    seed_all(42)
    rng = np.random.default_rng(42)
    keys = load_contoso_keys()

    orders_df = pd.DataFrame({
        "OrderKey": keys["order_keys"],
    })
    # Load full orders for store keys
    from _contoso_helpers import load_parquet
    orders_full = load_parquet("orders")

    logger.info("Generating shipments for %d orders...", len(orders_full))

    order_keys = orders_full["OrderKey"].values
    store_keys = orders_full["StoreKey"].values
    order_dates = pd.to_datetime(orders_full["DT"]).values

    # Vectorized generation for performance
    n = len(order_keys)
    carriers = rng.choice(CARRIERS, size=n, p=CARRIER_WEIGHTS)
    ship_delays = rng.integers(0, 4, size=n)  # 0-3 days to ship
    transit_days = rng.integers(2, 15, size=n)  # 2-14 days in transit
    ship_costs = np.round(rng.uniform(5, 45, size=n), 2)

    # Status: 93% delivered, 5% returned, 2% lost
    status_rolls = rng.random(size=n)
    statuses = np.where(
        status_rolls < 0.93, "Delivered",
        np.where(status_rolls < 0.98, "Returned", "Lost")
    )

    ship_dates = order_dates + pd.to_timedelta(ship_delays, unit="D")
    delivery_dates = ship_dates + pd.to_timedelta(transit_days, unit="D")

    # Generate tracking numbers
    tracking = [
        f"{carriers[i][:3].upper()}{rng.integers(100000000, 999999999)}"
        for i in range(n)
    ]

    df = pd.DataFrame({
        "ShipmentKey": np.arange(1, n + 1),
        "OrderKey": order_keys,
        "ShipDate": ship_dates,
        "DeliveryDate": delivery_dates,
        "Carrier": carriers,
        "TrackingNumber": tracking,
        "ShipmentStatus": statuses,
        "ShipCost": ship_costs,
        "Currency": "USD",
        "StoreKey": store_keys,
    })

    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d shipment records", len(df))
    return df
