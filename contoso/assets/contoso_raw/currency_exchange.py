"""@bruin

name: contoso_raw.currency_exchange
description: |
  Historical daily currency exchange rates for Contoso's global retail operations.

  Contains exchange rates between the five primary currencies used across Contoso's international
  stores: USD, EUR, GBP, CAD, and AUD. This dataset supports currency normalization for sales
  transactions, financial reporting, and multi-currency analytics across the organization.

  The data spans from 2016-2026 with ~100K daily exchange rate observations across all currency
  pairs. Rates are provided with 5-decimal precision and are used downstream for converting local
  currency sales amounts to USD equivalents in financial analysis.

  Key characteristics:
  - 25 bidirectional currency pairs (5 currencies × 5 currencies)
  - Daily frequency but excludes weekends and major holidays (~4,018 unique dates over 11 years)
  - Exchange rates stored with high precision (5 decimal places) for accurate financial calculations
  - All rates are complete (no missing values despite nullable schema definition)
  - Extracted as a single batch snapshot from the Contoso V2 dataset

  Used by downstream processes including sales_fact table currency conversions, multi-currency
  financial reporting, and international performance analysis.

  Source: SQLBI Contoso Data Generator V2 (MIT license)
connection: gcp-default
tags:
  - domain:sales
  - domain:finance
  - data_type:fact_table
  - source:contoso_v2
  - update_pattern:snapshot
  - sensitivity:internal
  - pipeline_role:raw

materialization:
  type: table
  strategy: create+replace
image: python:3.11


columns:
  - name: date
    type: TIMESTAMP
    description: |
      Exchange rate effective date (timestamp format).
      Daily rates spanning 2016-2026, excluding weekends and holidays.
      Forms part of composite primary key with currency pair.
    checks:
      - name: not_null
  - name: from_currency
    type: STRING
    description: |
      Source currency code in ISO 4217 format (3-character).
      One of: USD, EUR, GBP, CAD, AUD representing Contoso's operating markets.
      Forms part of composite primary key.
    checks:
      - name: not_null
  - name: to_currency
    type: STRING
    description: |
      Target currency code in ISO 4217 format (3-character).
      One of: USD, EUR, GBP, CAD, AUD representing Contoso's operating markets.
      Forms part of composite primary key.
    checks:
      - name: not_null
  - name: exchange
    type: NUMERIC
    description: |
      Exchange rate from source to target currency with 5-decimal precision.
      Represents how many units of target currency equal 1 unit of source currency.
      Used for converting transaction amounts between currencies in financial analysis.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - '>0'
  - name: extracted_at
    type: TIMESTAMP
    description: |-
      Timestamp when this data was extracted from source system (UTC).
      Single extraction timestamp for the entire dataset batch load.

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
    logger.info("Loading Contoso V2 currency exchange data...")
    df = load_parquet("currency_exchange")
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Fetched %d rows", len(df))
    return df
