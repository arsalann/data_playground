"""@bruin

name: contoso_v2_raw.sales
description: |
  Sales line items for Contoso v2 — ~1.8M rows linking orders to specific
  products with quantity, pricing in local currency, cost basis, and the
  exchange rate used for USD normalization. Average ~1.2 lines per order.
  Quantity distribution skews to 1-2 units (90%+); 1.5% of lines have negative
  quantity reflecting returns. 2% of lines carry clearance discounts >40%.
  Inflation regime (2022) inflates unit costs ~12%.
connection: gcp-default
instance: b1.large
tags:
  - fact_table
  - sales
  - multi_currency

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: order_key
    type: INTEGER
    description: Foreign key to orders, composite primary with line_number.
    primary_key: true
    checks:
      - name: not_null
  - name: line_number
    type: INTEGER
    description: 1-based line number within the order.
    primary_key: true
    checks:
      - name: not_null
  - name: order_date
    type: TIMESTAMP
    description: Order timestamp (denormalized from orders).
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
    description: Units sold. Negative for returns (~1.5%).
    checks:
      - name: not_null
  - name: unit_price
    type: NUMERIC
    description: List price per unit in local currency.
    checks:
      - name: not_null
      - name: non_negative
  - name: net_price
    type: NUMERIC
    description: Final price per unit after discount in local currency.
    checks:
      - name: not_null
      - name: non_negative
  - name: unit_cost
    type: NUMERIC
    description: Cost basis per unit in local currency.
    checks:
      - name: not_null
      - name: non_negative
  - name: currency_code
    type: VARCHAR
    description: ISO 4217 transaction currency.
    checks:
      - name: not_null
  - name: exchange_rate
    type: NUMERIC
    description: USD per unit of currency_code on the order date.
    checks:
      - name: not_null
      - name: positive
  - name: extracted_at
    type: TIMESTAMP
    description: ETL processing timestamp.

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
    logger.info("Materializing %d sales line items", len(df))
    return df
