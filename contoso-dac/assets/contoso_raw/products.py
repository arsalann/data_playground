"""@bruin

name: contoso_raw.products
description: |
  Product master dimension table for Contoso's consumer electronics catalog.
  Contains detailed product information including pricing, physical attributes, and hierarchical categorization.

  This dataset represents ~2,500 consumer electronics products from the SQLBI Contoso V2 data generator.
  Products span 8 major categories (Audio, Computers, etc.) and 32 subcategories, with full pricing and
  cost information. Used as the primary product dimension for sales analysis and inventory management.

  Key characteristics:
  - All products have unique codes and names (high cardinality)
  - 11 manufacturers and 11 brands represented
  - Weight information missing for ~11% of products (likely digital/software items)
  - Pricing includes both cost (wholesale) and retail price in USD
  - Categories follow a two-level hierarchy (category -> subcategory)

  Source: SQLBI Contoso Data Generator V2 (MIT license)
connection: contoso-duckdb
tags:
  - domain:sales
  - data_type:dimension_table
  - source:contoso_v2
  - update_pattern:snapshot
  - sensitivity:internal

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: product_key
    type: INTEGER
    description: Unique product identifier used for dimensional joins across the data warehouse
    checks:
      - name: not_null
      - name: unique
  - name: product_code
    type: VARCHAR
    description: Product SKU code - standardized 7-character alphanumeric identifier
    checks:
      - name: not_null
      - name: unique
  - name: product_name
    type: VARCHAR
    description: Full descriptive product name as displayed to customers (19-83 characters)
    checks:
      - name: not_null
  - name: manufacturer
    type: VARCHAR
    description: Manufacturing company name (11 distinct manufacturers including major electronics brands)
    checks:
      - name: not_null
  - name: brand
    type: VARCHAR
    description: Brand name under which the product is marketed (11 distinct brands)
    checks:
      - name: not_null
  - name: color
    type: VARCHAR
    description: Primary product color (17 distinct colors including standard and specialty colors)
    checks:
      - name: not_null
  - name: weight_unit
    type: VARCHAR
    description: Unit of measurement for product weight (e.g., 'lbs', 'kg', 'oz', 'g'). Empty for ~9% of products, likely digital/software items
  - name: weight
    type: NUMERIC
    description: Product weight in the specified unit. Null for ~11% of products (284/2517), typically non-physical items
  - name: cost
    type: NUMERIC
    description: Unit wholesale cost in USD - used for margin calculations and profitability analysis
    checks:
      - name: not_null
  - name: price
    type: NUMERIC
    description: Unit retail price in USD - customer-facing price before discounts
    checks:
      - name: not_null
  - name: category_key
    type: INTEGER
    description: Foreign key to product category dimension (8 distinct categories)
    checks:
      - name: not_null
  - name: category_name
    type: VARCHAR
    description: Product category name (e.g., 'Audio', 'Computers', 'Cameras and camcorders'). Top level of product hierarchy
    checks:
      - name: not_null
  - name: sub_category_key
    type: INTEGER
    description: Foreign key to product subcategory dimension (32 distinct subcategories)
    checks:
      - name: not_null
  - name: sub_category_name
    type: VARCHAR
    description: Product subcategory name providing granular classification within each category
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: Data extraction timestamp in UTC - indicates when this snapshot was loaded from source system

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
    logger.info("Loading Contoso V2 products data...")
    df = load_parquet("products")
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Fetched %d rows", len(df))
    return df
