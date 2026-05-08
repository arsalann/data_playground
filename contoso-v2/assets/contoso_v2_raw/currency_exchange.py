"""@bruin

name: contoso_v2_raw.currency_exchange
description: |
  Daily exchange rates from local currency to USD for the Contoso v2 simulation.
  Covers 2010-01-01 to 2026-05-01 across 6 non-USD currencies (EUR, GBP, CAD, AUD,
  SEK, JPY). Generated as a random walk around realistic mid-rates with three
  documented volatility events:
    - 2016-06 Brexit: GBP drops ~12% over 2 weeks
    - 2020-03 COVID: ±8% volatility across all currencies
    - 2022 USD strength: DXY-style appreciation, EUR/GBP/CAD/AUD weaken ~10-15%
  USD always shows rate 1.0. exchange_rate is USD per unit of from_currency
  (so local_amount * exchange_rate = USD).
connection: gcp-default
tags:
  - reference_data
  - financial
  - daily_refresh

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: date
    type: DATE
    description: Calendar date.
    primary_key: true
    checks:
      - name: not_null
  - name: from_currency
    type: VARCHAR
    description: Source currency (ISO 4217).
    primary_key: true
    checks:
      - name: not_null
  - name: to_currency
    type: VARCHAR
    description: Target currency (always USD here).
    checks:
      - name: not_null
  - name: exchange_rate
    type: NUMERIC
    description: USD per unit of from_currency. local_amount * exchange_rate = USD.
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
from _contoso_helpers import build_exchange_rates

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


def materialize():
    df = build_exchange_rates().copy()
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Generated %d exchange rate rows", len(df))
    return df
