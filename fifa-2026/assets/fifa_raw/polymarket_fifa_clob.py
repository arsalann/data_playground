"""@bruin

name: fifa_raw.polymarket_fifa_clob
description: |
  CLOB (Central Limit Order Book) tick-level price history for FIFA-2026
  prediction-market outcome tokens. One row per (token_id, ts_utc) tick.

  Status: deferred in this build. The Polymarket Gamma API (used by
  `polymarket_fifa_markets`) already exposes the latest `outcome_prices`
  array per market, which is sufficient to compute team-by-team
  market_implied_prob in `team_market_implied_prob`. Iterating CLOB
  prices-history per token added 30+ minutes of API time without changing
  the H3 conclusions; this stub keeps the table contract while skipping the
  slow path.

  When re-enabled this asset would pull
  https://clob.polymarket.com/prices-history per token_id with retry+backoff
  and `last_trade_at` / `volume_24h` carry-forward.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - prediction_markets
  - external_source
  - raw_data
  - timeseries

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_raw.polymarket_fifa_markets
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: token_id
    type: VARCHAR
  - name: ts_utc
    type: TIMESTAMP
  - name: extracted_at
    type: TIMESTAMP
  - name: market_id
    type: VARCHAR
  - name: condition_id
    type: VARCHAR
  - name: yes_price
    type: DOUBLE

@bruin"""

import logging
import os

import pandas as pd

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def materialize():
    logger.info("polymarket_fifa_clob is currently a stub — H3 uses outcome_prices from the markets table")
    return pd.DataFrame({
        "token_id":     pd.Series(dtype="string"),
        "ts_utc":       pd.Series(dtype="datetime64[ns, UTC]"),
        "extracted_at": pd.Series(dtype="datetime64[ns, UTC]"),
        "market_id":    pd.Series(dtype="string"),
        "condition_id": pd.Series(dtype="string"),
        "yes_price":    pd.Series(dtype="float64"),
    })
