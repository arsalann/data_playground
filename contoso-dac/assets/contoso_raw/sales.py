"""@bruin

name: contoso_raw.sales
description: |
  Contoso V2 sales fact table containing detailed order line item transactions for a consumer electronics retailer.
  This is the core transactional table with 2.3M+ rows spanning May 2016 to December 2025.

  Each row represents a single line item within an order, containing product quantities, pricing in local currency,
  and foreign key relationships to customers, products, and stores. The data includes multi-currency transactions
  across 5 currencies (USD, EUR, GBP, CAD, AUD) with historical exchange rates.

  Key characteristics:
  - Most orders contain 1-2 line items (avg 1.16 lines per order)
  - Average quantity per line is 3.14 units
  - Contains both historical and some future delivery dates
  - All amounts are in local currency, requiring exchange rate conversion for USD analysis
  - Serves as the foundation for downstream sales analytics and reporting models

  Source: SQLBI Contoso Data Generator V2 (MIT license)
  https://github.com/sql-bi/Contoso-Data-Generator-V2-Data
connection: contoso-duckdb
instance: b1.large
tags:
  - domain:sales
  - data_type:fact_table
  - sensitivity:internal
  - pipeline_role:raw
  - update_pattern:batch
  - currency:multi_currency

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: order_key
    type: INTEGER
    description: Unique order identifier, forms composite primary key with line_number
    primary_key: true
    checks:
      - name: not_null
  - name: line_number
    type: INTEGER
    description: Line item sequence number within the order (1-based), typically 1-2 items per order
    primary_key: true
    checks:
      - name: not_null
      - name: non_negative
  - name: order_date
    type: TIMESTAMP
    description: Date and time when the order was placed by the customer
    checks:
      - name: not_null
  - name: delivery_date
    type: TIMESTAMP
    description: |
      Date when the order was delivered to the customer.
      Note: Contains some future dates extending to January 2026 for recent orders.
    checks:
      - name: not_null
  - name: customer_key
    type: INTEGER
    description: Foreign key to customers dimension table, identifies the purchasing customer
    checks:
      - name: not_null
  - name: store_key
    type: INTEGER
    description: Foreign key to stores dimension table, identifies the selling store location
    checks:
      - name: not_null
  - name: product_key
    type: INTEGER
    description: Foreign key to products dimension table, identifies the specific product sold
    checks:
      - name: not_null
  - name: quantity
    type: INTEGER
    description: Number of units of this product ordered (average 3.14 units per line)
    checks:
      - name: not_null
      - name: positive
        value: '>= 1'
  - name: unit_price
    type: NUMERIC(9,5)
    description: Selling price per unit in local currency before any discounts
    checks:
      - name: not_null
      - name: non_negative
        value: '>= 0'
  - name: net_price
    type: NUMERIC(9,5)
    description: |
      Final selling price per unit in local currency after applying discounts.
      Used for revenue calculations. Typically slightly lower than unit_price.
    checks:
      - name: not_null
      - name: non_negative
        value: '>= 0'
  - name: unit_cost
    type: NUMERIC(9,5)
    description: Cost basis per unit in local currency, used for profit margin calculations
    checks:
      - name: not_null
      - name: non_negative
        value: '>= 0'
  - name: currency_code
    type: STRING
    description: |
      ISO 4217 currency code for the transaction.
      Values: USD, EUR, GBP, CAD, AUD (exactly 3 characters)
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - USD
          - EUR
          - GBP
          - CAD
          - AUD
  - name: exchange_rate
    type: NUMERIC(6,5)
    description: |
      Exchange rate from local currency to USD at the time of sale.
      Multiply local currency amounts by this rate to get USD equivalent.
    checks:
      - name: not_null
      - name: positive
        value: '> 0'
  - name: extracted_at
    type: TIMESTAMP
    description: |
      Timestamp when this row was loaded into the data warehouse (UTC).
      All current rows share the same extraction timestamp indicating batch load.

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
    logger.info("Loading Contoso V2 sales data...")
    df = load_parquet("sales")
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Fetched %d rows", len(df))
    return df
