"""@bruin

name: contoso_v2_raw.stores
description: |
  Store dimension for Contoso v2. 75 retail and online locations across 10 countries.
  Open dates span 2010-2024 reflecting the company's geographic expansion. 16 stores
  closed at various points (4 pre-2014 consolidation, 4 permanently during COVID 2020,
  2 in 2023). 25 brick-and-mortar stores have COVID temporary closures (Mar-Aug 2020).
connection: gcp-default
tags:
  - dimension_table
  - reference_data
  - retail_locations

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: store_key
    type: INTEGER
    description: Primary key (sparse integers).
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: store_name
    type: VARCHAR
    description: Store display name.
    checks:
      - name: not_null
  - name: city
    type: VARCHAR
    description: City where the store is located.
    checks:
      - name: not_null
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2 country code.
    checks:
      - name: not_null
  - name: country_name
    type: VARCHAR
    description: Country full name.
    checks:
      - name: not_null
  - name: currency_code
    type: VARCHAR
    description: Local currency for transactions at this store (ISO 4217).
    checks:
      - name: not_null
  - name: channel
    type: VARCHAR
    description: Retail or Online.
    checks:
      - name: not_null
  - name: open_date
    type: DATE
    description: Date the store opened.
    checks:
      - name: not_null
  - name: close_date
    type: DATE
    description: Date the store permanently closed (NULL if still operating).
  - name: temp_closed_start
    type: DATE
    description: Start of any temporary closure (e.g. COVID 2020).
  - name: temp_closed_end
    type: DATE
    description: End of any temporary closure.
  - name: square_meters
    type: INTEGER
    description: Floor area for retail stores (NULL for online).
  - name: extracted_at
    type: TIMESTAMP
    description: ETL processing timestamp.

@bruin"""

import logging
import os
from datetime import datetime, timezone

import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import build_stores

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


def materialize():
    df = build_stores().copy()
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d stores", len(df))
    return df
