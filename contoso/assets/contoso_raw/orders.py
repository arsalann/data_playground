"""@bruin

name: contoso_raw.orders
description: |
  Order header fact table for Contoso Electronics - the central transactional record tracking
  all customer orders placed through retail stores across 8 countries from 2016-2025.

  This table contains 980K order records representing the business-critical transaction headers
  that link customers to stores and establish order timing. Each order represents a single
  shopping session where a customer made one or more purchases at a physical store location.

  Key business characteristics:
  - Multi-national operations across US, Canada, Europe (UK, France, Germany, Italy, Netherlands) and Australia
  - Orders span nearly a decade (2016-2025) providing rich historical transaction patterns
  - All monetary values are stored in local store currency requiring currency conversion for analysis
  - Delivery dates may extend beyond order dates indicating fulfillment lag or backorder scenarios
  - Serves as the foundation for sales analysis, customer lifetime value, and store performance metrics

  Data lineage and relationships:
  - Links to contoso_raw.customers via customer_key for demographic and geographic analysis
  - Links to contoso_raw.stores via store_key for location-based performance metrics
  - Currency values join to contoso_raw.currency_exchange for USD normalization
  - Individual line items are stored separately in contoso_raw.order_rows table
  - Orders without delivery dates likely indicate unfulfilled or cancelled transactions

  Source: SQLBI Contoso Data Generator V2 (MIT license)
  Documentation: https://github.com/sql-bi/Contoso-Data-Generator-V2-Data
connection: bruin-playground-eu
instance: b1.large
tags:
  - fact_table
  - transactional_data
  - retail_orders
  - multi_currency
  - cross_border
  - customer_transactions
  - store_operations
  - time_series
  - historical_data

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: order_key
    type: INTEGER
    description: |
      Primary identifier for order records. Unique across the entire order history spanning
      2016-2025. This is the business key used to link order headers to line items, payments,
      and fulfillment records. Range: millions of sequential order numbers.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: customer_key
    type: INTEGER
    description: |
      Foreign key reference to contoso_raw.customers dimension table. Links this order
      to customer demographic and geographic profile. Essential for customer segmentation,
      lifetime value analysis, and geographic sales distribution. All orders have valid
      customer associations.
    checks:
      - name: not_null
  - name: store_key
    type: INTEGER
    description: |
      Foreign key reference to contoso_raw.stores dimension table. Identifies the physical
      retail location where this order was placed. Critical for store performance analysis,
      regional sales reporting, and catchment area studies. All orders originate from
      valid store locations across 8 countries.
    checks:
      - name: not_null
  - name: order_date
    type: TIMESTAMP
    description: |
      UTC timestamp when the order was initially placed by the customer at the store.
      Primary temporal dimension for time-series analysis, seasonality studies, and
      business period reporting. Date range spans 2016-05-18 to 2025-12-31.
    checks:
      - name: not_null
  - name: delivery_date
    type: TIMESTAMP
    description: |
      UTC timestamp when the order was delivered to the customer. May be same day for
      in-stock items or future dates for backordered/special order items. Essential for
      fulfillment performance analysis and customer satisfaction metrics. Date range
      extends to 2026-01-08 indicating some orders have future delivery commitments.
    checks:
      - name: not_null
  - name: currency_code
    type: STRING
    description: |
      ISO 4217 three-character currency code indicating the local currency used for this
      transaction. Values: USD (United States), EUR (Eurozone), CAD (Canada),
      GBP (United Kingdom), AUD (Australia). Critical for multi-currency revenue
      reporting and requires conversion to USD for consolidated financial analysis.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: |
      System-generated UTC timestamp indicating when this order record was loaded into
      the data warehouse. Used for data lineage tracking, incremental processing logic,
      and debugging data pipeline issues. All records share the same extraction timestamp
      indicating full refresh strategy.
    checks:
      - name: not_null

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
    logger.info("Loading Contoso V2 orders data...")
    df = load_parquet("orders")
    df = df.rename(columns={"DT": "OrderDate"})
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Fetched %d rows", len(df))
    return df
