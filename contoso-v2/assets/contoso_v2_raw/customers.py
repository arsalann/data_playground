"""@bruin

name: contoso_v2_raw.customers
description: |
  Customer dimension for Contoso v2. ~140K customers across 9 countries with
  signup_date spread across 2010-01-01 to 2026-04-30, weighted by the long-term
  growth curve (more new signups during expansion 2015-2017 and AI boom 2023-2024).
  1.5% of records have data-quality issues (NULL birth_year or empty email)
  reflecting realistic CRM noise. Segment classification: New (<90d), Returning,
  Lapsed (>3y), VIP (5%).
connection: gcp-default
instance: b1.large
tags:
  - dimension_table
  - master_data
  - sensitivity:pii

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: customer_key
    type: INTEGER
    description: Primary key (sparse integers).
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: given_name
    type: VARCHAR
    description: Customer first name.
    checks:
      - name: not_null
  - name: surname
    type: VARCHAR
    description: Customer last name.
    checks:
      - name: not_null
  - name: gender
    type: VARCHAR
    description: male or female.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - male
          - female
  - name: birth_year
    type: INTEGER
    description: Year of birth (1.5% of records may be NULL — data quality).
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2 country code.
    checks:
      - name: not_null
  - name: country_full
    type: VARCHAR
    description: Full country name.
    checks:
      - name: not_null
  - name: currency_code
    type: VARCHAR
    description: Local currency for the customer's market.
    checks:
      - name: not_null
  - name: occupation
    type: VARCHAR
    description: Customer occupation (free-text).
  - name: signup_date
    type: DATE
    description: Date customer was added to the database.
    checks:
      - name: not_null
  - name: email
    type: VARCHAR
    description: Customer email (occasional empty strings reflect CRM data quality).
  - name: segment
    type: VARCHAR
    description: New / Returning / Lapsed / VIP.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - New
          - Returning
          - Lapsed
          - VIP
  - name: extracted_at
    type: TIMESTAMP
    description: ETL processing timestamp.

@bruin"""

import logging
import os
from datetime import datetime, timezone

import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contoso_helpers import build_customers

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


def materialize():
    df = build_customers().copy()
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d customers", len(df))
    return df
