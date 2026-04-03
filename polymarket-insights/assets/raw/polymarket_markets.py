"""@bruin

name: raw.polymarket_markets
description: |
  Comprehensive prediction market dataset from Polymarket, the largest decentralized prediction platform.
  Each record represents an individual prediction market where users trade on event outcomes.

  Markets span diverse topics including politics, sports, economics, crypto, and current events.
  Data includes market metadata (questions, resolution criteria), trading metrics (volume, liquidity),
  real-time pricing (bid/ask spreads, implied probabilities), and on-chain identifiers for
  smart contract integration.

  Notable characteristics:
  - Most markets are binary (Yes/No) with ~40% featuring negative-risk multi-outcome structures
  - Category field is sparsely populated (~0.4% of records) - likely a legacy field
  - Volume metrics have varying availability (24h volume only present for ~8% of markets)
  - Markets date back to 2020 but creation accelerated significantly in recent years
  - All current records show as "active" but ~93% are "closed" (resolved)

  Data fetched via paginated API calls ordered by trading volume to prioritize liquid markets.
  Essential raw layer for downstream market analysis, price modeling, and prediction accuracy studies.

  Source: https://gamma-api.polymarket.com
connection: bruin-playground-arsalan
tags:
  - domain:finance
  - data_type:fact_table
  - source:external_api
  - pipeline_role:raw
  - update_pattern:snapshot
  - sensitivity:public

materialization:
  type: table
  strategy: append
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: market_id
    type: VARCHAR
    description: Unique Polymarket market identifier - primary key with high cardinality (10k unique values)
    primary_key: true
  - name: question
    type: VARCHAR
    description: The prediction question being traded on - human-readable market premise (avg ~42 chars, high variability 8-123)
  - name: slug
    type: VARCHAR
    description: URL-friendly market identifier - hyphenated lowercase version used in Polymarket URLs (unique per market)
  - name: description
    type: VARCHAR
    description: Detailed market description including resolution criteria and sources - can be quite lengthy (avg ~670 chars, up to 3.3k)
  - name: category
    type: VARCHAR
    description: Market category classification - mostly unpopulated legacy field (only ~0.4% have values, 5 distinct categories when present)
  - name: end_date
    type: TIMESTAMP
    description: Market resolution/expiry date when outcome will be determined - ranges from 2020 to 2028, nullable for some markets (~1.8% null)
  - name: active
    type: BOOLEAN
    description: Whether market accepts new trades - currently all records show true (legacy field showing trading state)
  - name: closed
    type: BOOLEAN
    description: Whether market outcome has been resolved - ~93% are closed indicating historical resolution status
  - name: archived
    type: BOOLEAN
    description: Whether market has been archived from active display - currently all records show false
  - name: outcomes
    type: VARCHAR
    description: JSON array of possible outcomes (typically ["Yes", "No"] for binary markets, longer for multi-outcome)
  - name: outcome_prices
    type: VARCHAR
    description: JSON array of current implied probability prices for each outcome (0.0-1.0 representing market consensus)
  - name: volume_total
    type: DOUBLE
    description: Lifetime trading volume in USD - ranges from ~$700k to $1.5B, non-nullable core liquidity metric
  - name: volume_24h
    type: DOUBLE
    description: Trading volume in last 24 hours USD - only available for ~8% of markets (recent/active trading)
  - name: volume_1w
    type: DOUBLE
    description: Trading volume in last 7 days USD - available for ~96% of markets
  - name: volume_1m
    type: DOUBLE
    description: Trading volume in last 30 days USD - available for ~96% of markets
  - name: liquidity
    type: DOUBLE
    description: Current order book depth in USD - only available for ~10% of markets (active order books)
  - name: best_bid
    type: DOUBLE
    description: Highest current buy order price - probability scale 0-1, nullable for ~60% (inactive markets)
  - name: best_ask
    type: DOUBLE
    description: Lowest current sell order price - probability scale 0-1, represents market maker ask
  - name: spread
    type: DOUBLE
    description: Current bid-ask spread - measure of market liquidity/efficiency (lower = more efficient)
  - name: last_trade_price
    type: DOUBLE
    description: Most recent transaction price - probability scale 0-1 representing last market consensus
  - name: price_change_1d
    type: DOUBLE
    description: Price movement over last 24 hours - fractional change, nullable for ~24% (inactive markets)
  - name: price_change_1w
    type: DOUBLE
    description: Price movement over last 7 days - fractional change, nullable for ~41% (less recent activity)
  - name: price_change_1m
    type: DOUBLE
    description: Price movement over last 30 days - fractional change, nullable for ~61% (limited historical activity)
  - name: competitive
    type: DOUBLE
    description: Market competitiveness/efficiency score 0-1 - proprietary Polymarket metric, only for ~10% of markets
  - name: neg_risk
    type: BOOLEAN
    description: Multi-outcome market flag - true for ~40% indicating non-binary prediction structures
  - name: event_slug
    type: VARCHAR
    description: Parent event grouping identifier - links related markets (e.g., multiple election questions)
  - name: clob_token_ids
    type: VARCHAR
    description: JSON array of Central Limit Order Book token identifiers - blockchain trading contract references
  - name: condition_id
    type: VARCHAR
    description: Ethereum smart contract condition identifier (66 char hex string) - links to on-chain resolution logic
  - name: created_at
    type: TIMESTAMP
    description: Market creation timestamp on Polymarket platform - spans from 2020 to present with recent acceleration
  - name: extracted_at
    type: TIMESTAMP
    description: Data extraction timestamp from API - batch processing watermark for change detection

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
PAGE_SIZE = 100
MAX_RETRIES = 5


def fetch_with_retry(url, params, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code in (429, 502, 503):
                wait = 15 * (attempt + 1)
                logger.warning("HTTP %d, retrying in %ds", resp.status_code, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            logger.warning("Request error (attempt %d/%d): %s", attempt + 1, retries, e)
            time.sleep(wait)
    logger.error("All %d retries exhausted for %s", retries, url)
    return None


def safe_float(val):
    if val is None or val == "" or val == "null":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_bool(val):
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() == "true"
    return False


def fetch_all_markets():
    all_markets = []
    offset = 0
    max_markets = int(os.environ.get("POLYMARKET_MAX_MARKETS", "50000"))

    while len(all_markets) < max_markets:
        params = {
            "limit": PAGE_SIZE,
            "offset": offset,
            "order": "volumeNum",
            "ascending": "false",
        }

        logger.info("Fetching markets offset=%d", offset)
        data = fetch_with_retry(f"{GAMMA_BASE_URL}/markets", params)

        if data is None:
            logger.warning("Failed to fetch at offset=%d, returning partial results", offset)
            break

        if not data:
            logger.info("No more markets at offset=%d", offset)
            break

        for m in data:
            event_slug = None
            if m.get("events") and len(m["events"]) > 0:
                event_slug = m["events"][0].get("slug")

            row = {
                "market_id": m.get("id", ""),
                "question": m.get("question", ""),
                "slug": m.get("slug", ""),
                "description": (m.get("description", "") or "")[:8000],
                "category": m.get("category", ""),
                "end_date": m.get("endDate"),
                "active": safe_bool(m.get("active")),
                "closed": safe_bool(m.get("closed")),
                "archived": safe_bool(m.get("archived")),
                "outcomes": m.get("outcomes", "[]"),
                "outcome_prices": m.get("outcomePrices", "[]"),
                "volume_total": safe_float(m.get("volumeNum")),
                "volume_24h": safe_float(m.get("volume24hr")),
                "volume_1w": safe_float(m.get("volume1wk")),
                "volume_1m": safe_float(m.get("volume1mo")),
                "liquidity": safe_float(m.get("liquidityNum")),
                "best_bid": safe_float(m.get("bestBid")),
                "best_ask": safe_float(m.get("bestAsk")),
                "spread": safe_float(m.get("spread")),
                "last_trade_price": safe_float(m.get("lastTradePrice")),
                "price_change_1d": safe_float(m.get("oneDayPriceChange")),
                "price_change_1w": safe_float(m.get("oneWeekPriceChange")),
                "price_change_1m": safe_float(m.get("oneMonthPriceChange")),
                "competitive": safe_float(m.get("competitive")),
                "neg_risk": safe_bool(m.get("negRisk")),
                "event_slug": event_slug,
                "clob_token_ids": m.get("clobTokenIds", "[]"),
                "condition_id": m.get("conditionId", ""),
                "created_at": m.get("createdAt"),
            }
            all_markets.append(row)

        logger.info("Fetched %d markets (total so far: %d)", len(data), len(all_markets))

        if len(data) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(0.5)

    return all_markets


def materialize():
    logger.info("Starting Polymarket markets ingestion")

    rows = fetch_all_markets()

    if not rows:
        logger.warning("No markets fetched")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce", utc=True)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Total markets fetched: %d", len(df))
    logger.info("Active: %d, Closed: %d", df["active"].sum(), df["closed"].sum())
    logger.info("Categories: %s", df["category"].value_counts().head(10).to_dict())

    return df
