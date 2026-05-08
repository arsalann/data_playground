"""@bruin

name: contoso_v2_raw.order_rows
description: |
  Alias of contoso_v2_raw.sales — same line-item granularity, kept as a
  separate table for downstream models that joined on `orderrows` in the
  legacy Contoso schema.
connection: gcp-default
instance: b1.large
tags:
  - fact_table
  - sales
  - alias

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: order_key
    type: INTEGER
    primary_key: true
    description: Foreign key to orders.
    checks:
      - name: not_null
  - name: line_number
    type: INTEGER
    primary_key: true
    description: 1-based line number within the order.
    checks:
      - name: not_null
  - name: order_date
    type: TIMESTAMP
    description: Order timestamp.
    checks:
      - name: not_null
  - name: delivery_date
    type: TIMESTAMP
    description: Delivery timestamp.
    checks:
      - name: not_null
  - name: customer_key
    type: INTEGER
    description: Foreign key to customers.
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
  - name: quantity
    type: INTEGER
    description: Units sold (negative for returns).
    checks:
      - name: not_null
  - name: unit_price
    type: NUMERIC
    description: List price per unit in local currency.
    checks:
      - name: not_null
  - name: net_price
    type: NUMERIC
    description: Final price per unit after discount.
    checks:
      - name: not_null
  - name: unit_cost
    type: NUMERIC
    description: Cost basis per unit in local currency.
    checks:
      - name: not_null
  - name: currency_code
    type: VARCHAR
    description: ISO 4217 transaction currency.
    checks:
      - name: not_null
  - name: exchange_rate
    type: NUMERIC
    description: USD per unit of currency_code.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP

@bruin"""

import logging
import os
from datetime import datetime, timezone

import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import build_sales_lines

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


def materialize():
    df = build_sales_lines().copy()
    df["extracted_at"] = datetime.now(timezone.utc)
    return df
