"""@bruin

name: contoso_raw.stores
description: |
  Store dimension table from Contoso V2 retail simulation dataset. Contains 74 retail locations
  across 9 countries (US, GB, CA, IT, FR, DE, NL, AU, plus online operations). This table serves
  as the geographical and operational foundation for sales analysis, representing physical store
  locations with their opening/closing history from 2005-2019.

  The dataset includes both active and closed stores, with 16 stores having closed between
  2013-2019. Store sizes range from small outlets (245 sq m) to large flagship stores (3500 sq m).
  Some stores have undergone restructuring rather than permanent closure.

  Key relationships: Links to sales transactions via store_key, connects to geographic dimensions
  via geo_area_key. Used for analyzing sales performance by location, regional trends, and
  store operational efficiency.

  Source: SQLBI Contoso Data Generator V2 (MIT license) - retail electronics simulation data.
connection: bruin-playground-eu
tags:
  - domain:retail
  - data_type:dimension_table
  - source:contoso_v2
  - sensitivity:internal
  - pipeline_role:raw
  - department:sales

materialization:
  type: table
  strategy: create+replace
image: python:3.11

secrets:
  - key: bruin-playground-eu
    inject_as: bruin-playground-eu

columns:
  - name: store_key
    type: INTEGER
    description: Unique store identifier. Primary key for joining with sales transactions and other store-related data.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: store_code
    type: INTEGER
    description: Operational store code number used for internal identification. Typically assigned sequentially but not guaranteed to be unique across all stores.
  - name: geo_area_key
    type: INTEGER
    description: Foreign key linking to geographic area dimension. Used for regional sales analysis and territorial management.
    checks:
      - name: not_null
  - name: country_code
    type: VARCHAR
    description: ISO 2-character country code (US, GB, CA, IT, FR, DE, NL, AU). Includes '--' for online/virtual operations.
    checks:
      - name: not_null
  - name: country_name
    type: VARCHAR
    description: Full country name corresponding to country_code. 'Online' designation used for digital-only operations.
    checks:
      - name: not_null
  - name: state
    type: VARCHAR
    description: State, province, or regional subdivision within the country. High cardinality field (67 distinct values) indicating broad geographic spread.
    checks:
      - name: not_null
  - name: open_date
    type: TIMESTAMP
    description: Date when the store first opened for business. Ranges from 2005-03-04 to 2019-03-05, showing historical expansion timeline.
    checks:
      - name: not_null
  - name: close_date
    type: TIMESTAMP
    description: Date when the store permanently closed (null for active stores). 58 stores remain active, 16 closed between 2013-12-05 and 2019-11-03.
  - name: description
    type: VARCHAR
    description: Human-readable store description, often including location details or distinguishing characteristics. Typically 12-42 characters in length.
    checks:
      - name: not_null
  - name: square_meters
    type: FLOAT
    description: Total floor area of the store in square meters. Ranges from 245 to 3500 sq m, indicating store size from small outlets to large flagship locations.
  - name: status
    type: VARCHAR
    description: Current operational status of the store. Values include empty string (active), 'Closed', or 'Restructured'. Many active stores have empty status field.
  - name: extracted_at
    type: TIMESTAMP
    description: ETL metadata timestamp indicating when this record was loaded into the data warehouse (UTC). Used for data lineage and refresh tracking.
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
    logger.info("Loading Contoso V2 stores data...")
    df = load_parquet("stores")
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Fetched %d rows", len(df))
    return df
