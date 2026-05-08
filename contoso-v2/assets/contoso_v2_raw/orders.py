"""@bruin

name: contoso_v2_raw.orders
description: |
  Order-header fact for Contoso v2. ~1.5M synthetic orders spanning 2010-01-01
  to 2026-05-01, with daily volume modulated by long-term growth, seasonality
  (Q4 lift, Black Friday/Cyber Monday spikes, weekday pattern, holidays), and
  named regimes (COVID dip, AI-boom lift). Channel mix evolves from 70% in-store
  in 2010 toward 35% in 2026 with permanent online shift after COVID. Order
  status: ~92% Completed, 4% Cancelled, 2% Returned, 2% Pending. Sparse
  order_key reflects voids/deletes.
connection: gcp-default
instance: b1.large
tags:
  - fact_table
  - transactional_data
  - retail_orders
  - multi_currency
  - time_series

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: order_key
    type: INTEGER
    description: Primary identifier for orders. Sparse with ~10% gaps.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: customer_key
    type: INTEGER
    description: Foreign key to contoso_v2_raw.customers.
    checks:
      - name: not_null
  - name: store_key
    type: INTEGER
    description: Foreign key to contoso_v2_raw.stores.
    checks:
      - name: not_null
  - name: order_date
    type: TIMESTAMP
    description: UTC timestamp the order was placed.
    checks:
      - name: not_null
  - name: delivery_date
    type: TIMESTAMP
    description: UTC timestamp the order was/will be delivered. Affected by COVID supply-chain delays.
    checks:
      - name: not_null
  - name: currency_code
    type: VARCHAR
    description: |
      ISO 4217 transaction currency. Determined by store country: USD, GBP, EUR,
      CAD, AUD, SEK.
    checks:
      - name: not_null
  - name: status
    type: VARCHAR
    description: Order status — Completed (~92%), Cancelled (4%), Returned (2%), Pending (2%).
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Completed
          - Cancelled
          - Returned
          - Pending
  - name: channel
    type: VARCHAR
    description: Sales channel — Retail or Online.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Retail
          - Online
  - name: extracted_at
    type: TIMESTAMP
    description: ETL processing timestamp.

@bruin"""

import logging
import os
from datetime import datetime, timezone

import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import build_orders

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


def materialize():
    df = build_orders().copy()
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Materializing %d order headers", len(df))
    return df
