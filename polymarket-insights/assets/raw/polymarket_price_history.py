"""@bruin

name: raw.polymarket_price_history
description: |
  Historical price timeseries data for prediction market outcomes on Polymarket, the largest
  decentralized prediction platform. Contains granular price movements over time for the most
  liquid markets, enabling volatility analysis, price discovery research, and prediction accuracy studies.

  Each record represents a single price observation for a specific market outcome at a point in time.
  Prices reflect the crowd-sourced probability estimates that traders assign to events, creating
  a rich dataset for understanding how market sentiment evolves around real-world events.

  Data characteristics and insights:
  - Focuses on top ~200 markets by trading volume to ensure meaningful liquidity and price discovery
  - Binary outcome structure dominates (~2 outcomes per market: Yes/No)
  - Price range 0.0005-0.9995 represents implied probabilities with 0.05% minimum tick size
  - Average price hovers around 0.50, indicating well-balanced markets without strong directional bias
  - Historical depth spans from 2024-01-05 with ~1,813 unique timestamps across all markets
  - Category field is consistently empty - legacy field from older API versions
  - Token IDs are 76-78 character hexadecimal strings (blockchain addresses)
  - Condition IDs are 66-character strings linking to market metadata in raw.polymarket_markets

  Fetched via CLOB API with retry logic for rate limiting and configured for maximum historical fidelity.
  Essential foundation for downstream price modeling, volatility analysis, and market efficiency studies.

  Source: https://clob.polymarket.com/prices-history
connection: bruin-playground-arsalan
tags:
  - domain:finance
  - data_type:fact_table
  - source:external_api
  - pipeline_role:raw
  - update_pattern:append_only
  - sensitivity:public
  - market_data:prediction_markets
  - time_series:price_history

materialization:
  type: table
  strategy: append
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: condition_id
    type: VARCHAR
    description: |
      Market condition identifier linking to raw.polymarket_markets metadata. 66-character
      hexadecimal string representing the unique market condition on the blockchain.
      Foreign key relationship to markets table for enrichment with questions, categories, and trading metrics.
    checks:
      - name: not_null
  - name: token_id
    type: VARCHAR
    description: |
      CLOB token identifier for the specific market outcome being priced. 76-78 character
      hexadecimal string representing the tradeable token contract address. Combined with
      timestamp forms the grain of this table. High cardinality (~100 distinct tokens from top markets).
    primary_key: true
    checks:
      - name: not_null
  - name: outcome_label
    type: VARCHAR
    description: |
      Human-readable label for the market outcome (semantic type: dimension). Predominantly
      binary outcomes with values like "Yes", "No" but can include multi-outcome labels.
      Average length 2.5 characters with only 2 distinct values across current dataset.
    checks:
      - name: not_null
  - name: question
    type: VARCHAR
    description: |
      The prediction question text for context (semantic type: dimension). Truncated to
      2000 characters in ingestion logic. Varies from 26-77 characters with average ~55 chars.
      Enables human interpretation of what probability the price represents.
    checks:
      - name: not_null
  - name: category
    type: VARCHAR
    description: |
      Market category classification - LEGACY FIELD that is consistently empty across all records.
      Retained for backward compatibility but contains no meaningful data. Consider excluding from analysis.
  - name: timestamp
    type: TIMESTAMP
    description: |
      UTC timestamp when the price observation was recorded (semantic type: event_time).
      Forms part of the composite primary key with token_id. Spans 2024-01-05 to present
      with ~1,813 unique timestamps, indicating irregular but frequent price updates.
    primary_key: true
    checks:
      - name: not_null
  - name: price
    type: DOUBLE
    description: |
      Market price representing implied probability of the outcome occurring (semantic type: metric).
      Range 0.0005 to 0.9995 (0.05% to 99.95%) with ~0.05% tick size. Values near 0.5 indicate
      uncertain outcomes while values near extremes suggest strong market consensus. Critical for
      probability modeling and market sentiment analysis.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: |
      System timestamp when the record was fetched from the Polymarket API (semantic type: ingestion_timestamp).
      All current records share the same extraction timestamp (2026-04-02 09:05:09), indicating batch processing.
      Used for data lineage tracking and debugging data quality issues.
    checks:
      - name: not_null

@bruin"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
CLOB_BASE_URL = "https://clob.polymarket.com"
MAX_RETRIES = 5
TOP_MARKETS_LIMIT = int(os.environ.get("POLYMARKET_MARKET_LIMIT", "200"))


def fetch_with_retry(url, params=None, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code in (429, 502, 503):
                wait = 15 * (attempt + 1)
                logger.warning("HTTP %d, retrying in %ds", resp.status_code, wait)
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            logger.warning("Request error (attempt %d/%d): %s", attempt + 1, retries, e)
            time.sleep(wait)
    logger.error("All %d retries exhausted for %s", retries, url)
    return None


def fetch_top_markets(limit):
    """Fetch top markets by volume from Gamma API."""
    markets = []
    offset = 0
    page_size = 100

    while len(markets) < limit:
        fetch_limit = min(page_size, limit - len(markets))
        params = {
            "limit": fetch_limit,
            "offset": offset,
            "order": "volumeNum",
            "ascending": "false",
        }

        logger.info("Fetching top markets offset=%d (have %d/%d)", offset, len(markets), limit)
        data = fetch_with_retry(f"{GAMMA_BASE_URL}/markets", params)

        if not data:
            break

        for m in data:
            token_ids_raw = m.get("clobTokenIds", "[]")
            try:
                token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
            except json.JSONDecodeError:
                token_ids = []

            outcomes_raw = m.get("outcomes", "[]")
            try:
                outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            except json.JSONDecodeError:
                outcomes = []

            if token_ids:
                markets.append({
                    "condition_id": m.get("conditionId", ""),
                    "question": m.get("question", ""),
                    "category": m.get("category", ""),
                    "token_ids": token_ids,
                    "outcomes": outcomes,
                    "volume": float(m.get("volumeNum", 0) or 0),
                })

        if len(data) < fetch_limit:
            break

        offset += fetch_limit
        time.sleep(0.5)

    logger.info("Found %d markets with token IDs", len(markets))
    return markets[:limit]


def fetch_price_history(token_id):
    """Fetch full price history for a single token."""
    params = {
        "market": token_id,
        "interval": "max",
        "fidelity": 2000,
    }

    data = fetch_with_retry(f"{CLOB_BASE_URL}/prices-history", params)

    if not data or "history" not in data:
        return []

    return data["history"]


def materialize():
    logger.info("Starting Polymarket price history ingestion (top %d markets)", TOP_MARKETS_LIMIT)

    markets = fetch_top_markets(TOP_MARKETS_LIMIT)

    all_rows = []
    fetched_count = 0
    failed_count = 0

    for i, market in enumerate(markets):
        token_ids = market["token_ids"]
        outcomes = market["outcomes"]

        for j, token_id in enumerate(token_ids):
            outcome_label = outcomes[j] if j < len(outcomes) else f"Outcome_{j}"

            history = fetch_price_history(token_id)

            if history is None:
                failed_count += 1
                continue

            for point in history:
                all_rows.append({
                    "condition_id": market["condition_id"],
                    "token_id": token_id,
                    "outcome_label": outcome_label,
                    "question": market["question"][:2000],
                    "category": market["category"],
                    "timestamp": datetime.fromtimestamp(point["t"], tz=timezone.utc),
                    "price": float(point["p"]),
                })

            fetched_count += 1
            time.sleep(0.3)

        if (i + 1) % 25 == 0:
            logger.info(
                "Progress: %d/%d markets, %d price points, %d failures",
                i + 1, len(markets), len(all_rows), failed_count,
            )

    if not all_rows:
        logger.warning("No price history fetched")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "Done: %d tokens fetched, %d failures, %d total price points",
        fetched_count, failed_count, len(df),
    )

    return df
