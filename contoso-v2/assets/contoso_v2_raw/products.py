"""@bruin

name: contoso_v2_raw.products
description: |
  Product catalog for Contoso v2. ~2,500 SKUs across 8 categories, 32 subcategories,
  15 brands. Each product has a `first_listed_date` distributed across 2010-2024 and
  ~3% have a `discontinued_date` clustered around the COVID 2020 catalog rationalization
  and 2022 inflation cull. AI-themed brands (Synapse AI, Halcyon AI, Forge Robotics)
  only appear from 2023 onward.
connection: gcp-default
tags:
  - dimension_table
  - reference_data
  - product_catalog

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: product_key
    type: INTEGER
    description: Primary key (sparse integers).
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: product_name
    type: VARCHAR
    description: Product display name (brand + subcategory + model).
    checks:
      - name: not_null
  - name: brand
    type: VARCHAR
    description: Brand name.
    checks:
      - name: not_null
  - name: category_name
    type: VARCHAR
    description: High-level category.
    checks:
      - name: not_null
  - name: sub_category_name
    type: VARCHAR
    description: Detailed subcategory.
    checks:
      - name: not_null
  - name: color
    type: VARCHAR
    description: Product color.
  - name: cost
    type: NUMERIC
    description: Cost basis per unit in USD.
    checks:
      - name: not_null
      - name: non_negative
  - name: price
    type: NUMERIC
    description: List price per unit in USD (cost × markup).
    checks:
      - name: not_null
      - name: non_negative
  - name: first_listed_date
    type: DATE
    description: Date the product was first listed for sale.
    checks:
      - name: not_null
  - name: discontinued_date
    type: DATE
    description: Date the product was discontinued (NULL if still active).
  - name: extracted_at
    type: TIMESTAMP
    description: ETL processing timestamp.

@bruin"""

import logging
import os
from datetime import datetime, timezone

import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import build_products

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


def materialize():
    df = build_products().copy()
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d products", len(df))
    return df
