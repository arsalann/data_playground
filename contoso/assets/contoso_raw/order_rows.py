"""@bruin

name: contoso_raw.order_rows
description: |
  Retail sales order line items for Contoso consumer electronics retailer.

  Each row represents one product line within a customer order, containing pricing,
  quantity, and cost information. This is the core transactional dataset that drives
  revenue analysis, inventory planning, and margin calculations across the business.

  The data contains 2.3M line items from ~980K orders, with most orders having 1-2
  line items (avg 1.16 lines per order). Order keys span a large range (~23M) suggesting
  this represents a subset of a larger order history.

  Pricing data includes both unit_price (selling price) and net_price (after discounts),
  with net_price typically being lower, indicating systematic discount application.
  Unit costs are included for margin analysis.

  Source: SQLBI Contoso Data Generator V2 (MIT license) - a widely-used sample dataset
  in the BI community representing realistic retail business patterns.
connection: gcp-default
tags:
  - domain:sales
  - domain:finance
  - data_type:fact_table
  - pipeline_role:raw
  - source:contoso_v2
  - business_process:order_fulfillment
  - update_pattern:snapshot
  - sensitivity:internal

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: order_key
    type: INTEGER
    description: Order identifier, foreign key to contoso_raw.orders table
    primary_key: true
    checks:
      - name: not_null
  - name: row_number
    type: INTEGER
    description: Line item sequence number within the order (1, 2, 3, etc.)
    primary_key: true
    checks:
      - name: not_null
      - name: positive
  - name: product_key
    type: INTEGER
    description: Product identifier, foreign key to contoso_raw.products dimension (~2.5K products)
    checks:
      - name: not_null
      - name: positive
  - name: quantity
    type: INTEGER
    description: Number of product units ordered on this line item (avg 3.1 units)
    checks:
      - name: not_null
      - name: positive
  - name: unit_price
    type: NUMERIC
    description: Selling price per unit in local currency (5 decimal precision, varies by market)
    checks:
      - name: not_null
      - name: positive
  - name: net_price
    type: NUMERIC
    description: Net selling price per unit after discounts applied (typically ≤ unit_price)
    checks:
      - name: not_null
      - name: positive
  - name: unit_cost
    type: NUMERIC
    description: Cost per unit in local currency (5 decimal precision, for margin analysis)
    checks:
      - name: not_null
      - name: positive
  - name: extracted_at
    type: TIMESTAMP
    description: ETL batch timestamp when data was loaded (UTC), used for data lineage tracking

@bruin"""

import logging
import os
from datetime import datetime, timezone

import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import load_parquet

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def materialize():
    logger.info("Loading Contoso V2 order rows data...")
    df = load_parquet("order_rows")
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Fetched %d rows", len(df))
    return df
