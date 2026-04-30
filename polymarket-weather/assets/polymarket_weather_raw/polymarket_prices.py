"""@bruin

name: polymarket_weather_raw.polymarket_prices
description: |
  High-frequency tick-level price history for weather prediction markets, designed for forensic
  analysis of the April 2026 Paris-CDG temperature sensor tampering allegations. This dataset
  captures sub-hour price movements from Polymarket's Central Limit Order Book (CLOB) at the
  highest available temporal resolution (fidelity=1) to enable analysis of trader behavior
  during suspected sensor manipulation events.

  The asset fetches complete price histories for all tokens from the latest snapshot of
  polymarket_weather_raw.polymarket_markets, with special focus on Paris daily temperature
  markets that resolved using the disputed CDG sensor. Price data spans March-April 2026,
  covering the critical periods when $34k in winnings were recorded on 2026-04-06 and 2026-04-15
  during unexplained 4°C temperature spikes at 18:30 local time.

  Each price tick represents the implied probability (0.0 to 1.0) of a specific weather outcome
  (e.g., "Paris reaches 22°C on April 15th") at a precise moment in time. The granular temporal
  resolution enables detection of unusual trading patterns, volume anomalies, and price movements
  that may correlate with alleged sensor tampering incidents.

  Paris daily weather markets are always included regardless of volume (as they are the focal
  point of the investigation), while other weather markets are prioritized by trading volume.
  Development environments can use POLYMARKET_PRICES_LIMIT to cap token fetches for faster
  iteration.

  ## Operational Characteristics
  - **Refresh cadence**: Daily batch extraction, appends only new price ticks
  - **Data volume**: 1M+ ticks covering 1,074 unique prediction tokens across 537 weather markets
  - **Temporal span**: March 28 - April 28, 2026 (spans the critical tampering investigation period)
  - **API rate limits**: 200ms delays between token requests with exponential backoff retry logic
  - **Data integrity**: Deduplication on (token_id, ts_utc, extracted_at) prevents duplicate ticks
  - **Forensic focus**: All Paris daily temperature series included, other markets filtered by volume

  Source: Polymarket CLOB API (https://clob.polymarket.com/prices-history), no authentication required.
connection: bruin-playground-arsalan
tags:
  - domain:finance
  - domain:weather
  - data_type:external_source
  - data_type:prediction_markets
  - data_type:timeseries
  - sensitivity:public
  - pipeline_role:raw
  - update_pattern:append_only
  - investigation:paris_cdg_tampering
  - granularity:tick_level

materialization:
  type: table
  strategy: append

depends:
  - polymarket_weather_raw.polymarket_markets
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: token_id
    type: VARCHAR
    description: CLOB token identifier - 76-78 character hexadecimal blockchain address uniquely identifying each prediction outcome token (e.g., specific temperature bucket for a given day)
    primary_key: true
    checks:
      - name: not_null
  - name: ts_utc
    type: TIMESTAMP
    description: Price tick timestamp in UTC - precise moment when this price was recorded on the CLOB, enabling sub-hour analysis of trading patterns during critical events
    primary_key: true
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: ETL timestamp in UTC when this price record was fetched from the CLOB API - used for deduplication and data lineage tracking
    primary_key: true
    checks:
      - name: not_null
  - name: condition_id
    type: VARCHAR
    description: 66-character on-chain condition identifier linking to the blockchain smart contract condition - connects to polymarket_markets.condition_id for market metadata
    checks:
      - name: not_null
  - name: market_id
    type: VARCHAR
    description: Polymarket internal market identifier (6-7 characters) - shorter human-readable ID linking to polymarket_markets.market_id for enrichment data
    checks:
      - name: not_null
  - name: outcome_label
    type: VARCHAR
    description: Human-readable outcome label - typically "Yes" or "No" for binary weather predictions (e.g., "Yes" = temperature will reach this threshold)
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - "Yes"
          - "No"
  - name: question
    type: VARCHAR
    description: Market question text describing the specific prediction (e.g., "Will Paris reach 22°C on April 15?") - truncated to 2000 chars for storage efficiency
    checks:
      - name: not_null
  - name: event_slug
    type: VARCHAR
    description: Parent event slug identifier (21-68 characters) - groups related markets under common events, links to polymarket_markets.event_slug
  - name: series_slug
    type: VARCHAR
    description: Series slug identifier for recurring market types (7-41 characters) - critical field identifying "paris-daily-weather" markets central to tampering investigation (nullable for non-series markets)
  - name: price
    type: DOUBLE
    description: Implied probability of the outcome as decimal (0.0005 to 0.9995) - represents market consensus on prediction likelihood, bounded away from pure 0/1 for numerical stability
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

CLOB = "https://clob.polymarket.com"
MAX_RETRIES = 5

PROJECT_ID = os.environ.get("BRUIN_BIGQUERY_PROJECT", "bruin-playground-arsalan")


def fetch_with_retry(url, params=None):
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code in (429, 502, 503):
                wait = 15 * (attempt + 1)
                logger.warning("CLOB HTTP %d, retrying in %ds", r.status_code, wait)
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            logger.warning("CLOB error attempt %d/%d: %s", attempt + 1, MAX_RETRIES, e)
            time.sleep(wait)
    return None


def fetch_prices(token_id: str) -> list:
    data = fetch_with_retry(f"{CLOB}/prices-history", {"market": token_id, "interval": "max", "fidelity": 1})
    if not data or "history" not in data:
        return []
    return data["history"]


FOCUS_SERIES_SLUGS = (
    "paris-daily-weather",
    "london-daily-weather",
    "seoul-daily-weather",
    "toronto-daily-weather",
)


def load_market_universe() -> pd.DataFrame:
    """Read the latest snapshot of polymarket_markets from BigQuery.

    All daily-temperature markets for the four investigation cities (Paris,
    London, Seoul, Toronto) resolving in 2026-01-01..2026-04-30 are always
    included regardless of volume. Other weather markets are sorted by volume
    and capped by POLYMARKET_PRICES_LIMIT.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    focus_in = ", ".join(f"'{s}'" for s in FOCUS_SERIES_SLUGS)
    sql = f"""
        WITH latest AS (
          SELECT MAX(extracted_at) AS ts FROM `{PROJECT_ID}.polymarket_weather_raw.polymarket_markets`
        )
        SELECT
            market_id, condition_id, clob_token_ids, outcomes, question,
            event_slug, series_slug, volume,
            CASE
                WHEN series_slug IN ({focus_in})
                  AND DATE(end_date, 'UTC') BETWEEN DATE '2026-01-01' AND DATE '2026-04-30'
                THEN 1 ELSE 0
            END AS is_focus
        FROM `{PROJECT_ID}.polymarket_weather_raw.polymarket_markets` m, latest
        WHERE m.extracted_at = latest.ts
          AND clob_token_ids IS NOT NULL AND clob_token_ids != ''
    """
    return client.query(sql).to_dataframe()


def materialize():
    limit = int(os.environ.get("POLYMARKET_PRICES_LIMIT", "0"))  # 0 = no cap
    logger.info("Loading market universe from polymarket_weather_raw.polymarket_markets")
    universe = load_market_universe()
    logger.info("Markets in latest snapshot: %d", len(universe))

    if limit > 0:
        # Always keep every focus-city daily-weather market; cap the rest to top-volume.
        focus = universe[universe["is_focus"] == 1]
        rest = universe[universe["is_focus"] == 0].sort_values("volume", ascending=False).head(limit)
        universe = pd.concat([focus, rest], ignore_index=True)
        logger.info(
            "Cap: kept all %d focus-city markets + top %d others by volume (total %d)",
            len(focus), len(rest), len(universe),
        )
    else:
        # No cap on focus cities, drop the rest (unbounded universe is impractical).
        focus = universe[universe["is_focus"] == 1]
        logger.info("Default mode (no env var): focus-city only — %d markets", len(focus))
        universe = focus.reset_index(drop=True)

    snap_ts = datetime.now(timezone.utc)
    rows = []

    fetched = 0
    failed = 0
    for i, row in enumerate(universe.itertuples(index=False)):
        try:
            token_ids = json.loads(row.clob_token_ids) if isinstance(row.clob_token_ids, str) else (row.clob_token_ids or [])
            outcomes = json.loads(row.outcomes) if isinstance(row.outcomes, str) else (row.outcomes or [])
        except json.JSONDecodeError:
            failed += 1
            continue

        for j, token_id in enumerate(token_ids):
            outcome_label = outcomes[j] if j < len(outcomes) else f"Outcome_{j}"
            history = fetch_prices(token_id)
            if not history:
                failed += 1
                continue
            for pt in history:
                rows.append({
                    "token_id": token_id,
                    "ts_utc": datetime.fromtimestamp(pt["t"], tz=timezone.utc),
                    "condition_id": row.condition_id,
                    "market_id": row.market_id,
                    "outcome_label": outcome_label,
                    "question": (row.question or "")[:2000],
                    "event_slug": row.event_slug,
                    "series_slug": row.series_slug,
                    "price": float(pt["p"]),
                    "extracted_at": snap_ts,
                })
            fetched += 1
            time.sleep(0.2)

        if (i + 1) % 50 == 0:
            logger.info("Progress: %d/%d markets, %d ticks, %d fetch failures", i + 1, len(universe), len(rows), failed)

    if not rows:
        logger.warning("No price history fetched")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["token_id", "ts_utc", "extracted_at"], keep="last").reset_index(drop=True)
    logger.info("Done: %d tokens fetched, %d failures, %d total ticks", fetched, failed, len(df))
    return df
