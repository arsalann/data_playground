"""@bruin

name: polymarket_weather_raw.polymarket_markets
description: |
  Polymarket weather-related prediction markets dataset, designed specifically for investigating
  the April 2026 Paris-CDG temperature sensor tampering allegations and broader weather betting
  analysis. This asset captures fresh data independently from legacy polymarket tables.

  Data is sourced via two complementary API passes:
    1. Events tagged with `tag_slug=weather` (Polymarket's curated weather category covering
       daily city temperature series, monthly global temperature anomalies, hurricane season
       counts, snowfall/precipitation markets, extreme weather events)
    2. Events from known city daily-weather series (29 global cities including Paris, NYC,
       London, Tokyo, etc.) to ensure complete coverage of temperature prediction markets

  Each parent event (e.g., "Highest temperature in Paris on April 15?") is exploded into
  individual market records representing specific temperature buckets ("21°C", "22°C", "≥24°C").
  This granular structure enables analysis of bucket-level trading behavior, resolution patterns,
  and price movements during alleged tampering incidents.

  The dataset spans 2024-2026 weather coverage with ~34,500 markets across ~3,700 events,
  capturing the complete ecosystem of weather prediction markets including the Paris daily
  series that resolved using Paris-CDG (LFPG) as the temperature source during the disputed
  April 2026 periods.

  Resolution sources are primarily Wunderground weather stations identified by ICAO codes,
  enabling cross-validation against independent meteorological data for anomaly detection.

  ## Operational Characteristics
  - **Refresh cadence**: Daily batch extraction via pipeline schedule
  - **Data size**: ~35K markets across ~4K events (growing with new weather series)
  - **Extraction strategy**: Two-pass API calls (weather-tagged + city series) with 200 events per page
  - **Error handling**: Retry logic with exponential backoff for rate limits and server errors
  - **Data patterns**: 96.5% markets closed, ~15% volume nulls, ~95% liquidity nulls (expected for historical markets)
  - **Blockchain integration**: All markets linked to on-chain conditions for settlement verification
  - **Quality assurance**: Deduplication by (market_id, extracted_at) in staging layer

  Source: Polymarket Gamma API (https://gamma-api.polymarket.com/events), no authentication required.
connection: bruin-playground-arsalan
tags:
  - domain:finance
  - domain:weather
  - data_type:external_source
  - data_type:prediction_markets
  - sensitivity:public
  - pipeline_role:raw
  - update_pattern:append_only
  - investigation:paris_cdg_tampering

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
    description: Polymarket inner-market identifier (6-7 characters) — unique primary key for one temperature bucket within an event. Essential for linking to CLOB price history and blockchain settlement data.
    primary_key: true
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this snapshot row was fetched from Gamma API. Used for versioning and incremental data loads. All records share the same extraction timestamp per pipeline run.
    primary_key: true
    checks:
      - name: not_null
  - name: event_id
    type: VARCHAR
    description: Parent event identifier (5-6 characters) — groups inner markets sharing a question stem. One event typically contains 8-15 temperature bucket markets. Critical for aggregating event-level trading metrics and resolution analysis.
    checks:
      - name: not_null
  - name: event_slug
    type: VARCHAR
    description: URL-friendly parent event slug (20-68 characters). Used in Polymarket URLs and human-readable references. Contains date and location information in standardized format.
    checks:
      - name: not_null
  - name: event_title
    type: VARCHAR
    description: Human-readable event title (21-69 characters). Examples include "Highest temperature in Paris on April 15?" Contains the prediction question's core context including city, date, and metric type.
    checks:
      - name: not_null
  - name: series_slug
    type: VARCHAR
    description: First series slug attached to the event (7-41 characters). Identifies recurring market series like "paris-daily-weather". Critical for filtering city-specific data in the investigation. ~141 records have null values.
  - name: tags_csv
    type: VARCHAR
    description: Comma-separated list of all tag slugs on the event (7-125 characters). Always includes "weather" tag. Used for market categorization and filtering. Contains hierarchical tags like location, timeframe, and weather phenomenon type.
    checks:
      - name: not_null
  - name: question
    type: VARCHAR
    description: Inner-market prediction question text (21-100 characters). The specific temperature bucket or range being predicted (e.g., "Will it be 21°C?", "Will it be ≥24°C?"). Nearly unique per market with ~23 duplicates across 34,500 records.
    checks:
      - name: not_null
  - name: slug
    type: VARCHAR
    description: URL-friendly inner-market slug (20-110 characters). Completely unique identifier used in Polymarket URLs. Contains encoded question text with temperature values for programmatic parsing.
    checks:
      - name: not_null
  - name: description
    type: VARCHAR
    description: Inner-market description truncated to 4000 characters (555-1597 characters observed). Contains detailed market rules, resolution criteria, and source specifications. Critical for understanding resolution methodology changes during the Paris tampering incident.
    checks:
      - name: not_null
  - name: resolution_source
    type: VARCHAR
    description: Free-text resolution source at market level (0-94 characters). References specific weather stations by name and ICAO code (e.g., "Wunderground LFPG"). Empty for ~10% of markets. Key field for tracking resolution source changes in Paris markets.
  - name: end_date
    type: TIMESTAMP
    description: Resolution/expiry timestamp for the inner market. When the market closes and temperature is measured for settlement. Range spans 2025-2027 with ~14 null values. Critical for temporal analysis of trading patterns.
  - name: start_date
    type: TIMESTAMP
    description: Trading-open timestamp for the inner market. When betting becomes available. Nearly unique timestamps indicate staggered market creation. One null value observed.
  - name: created_at
    type: TIMESTAMP
    description: Inner-market creation timestamp. Completely unique across all markets, indicating precise creation sequencing. Spans 2025-2026 period covering the investigation timeframe.
    checks:
      - name: not_null
  - name: outcomes
    type: VARCHAR
    description: JSON-encoded array of outcome labels. All markets use binary outcomes ["Yes", "No"] based on observed data. Length consistently 13 characters across all records.
    checks:
      - name: not_null
  - name: outcome_prices
    type: VARCHAR
    description: JSON-encoded array of final/current prices per outcome (0.0-1.0 range). For resolved markets, represents final settlement prices. For active markets, shows current bid/offer. 363 distinct price combinations observed.
    checks:
      - name: not_null
  - name: condition_id
    type: VARCHAR
    description: 66-character blockchain condition identifier linking to Ethereum smart contract state. Enables verification of on-chain settlement. Completely unique per market, critical for blockchain audit trail.
    checks:
      - name: not_null
  - name: clob_token_ids
    type: VARCHAR
    description: JSON-encoded array of CLOB (Central Limit Order Book) token addresses for Yes/No outcomes. Links to detailed price history via CLOB API. Completely unique per market (157-164 characters).
    checks:
      - name: not_null
  - name: volume
    type: DOUBLE
    description: Total all-time inner-market volume in USD. Ranges $0 to $6.5M with average $13.7K. Missing for ~15% of markets (likely inactive/new markets). Critical metric for identifying high-impact anomalous trading.
  - name: liquidity
    type: DOUBLE
    description: Current inner-market order-book depth in USD. Available for only ~5% of markets (likely active/recent markets only). Ranges $0 to $201K. Low coverage suggests most markets are closed or illiquid.
  - name: active
    type: BOOLEAN
    description: Whether the inner market is still accepting orders. Only 1 inactive market observed out of 34,561, indicating dataset captures primarily live/recently-closed markets.
    checks:
      - name: not_null
  - name: closed
    type: BOOLEAN
    description: Whether the inner market has been resolved. 96.5% of markets are closed (33,342 out of 34,561), indicating comprehensive historical coverage including resolved temperature predictions.
    checks:
      - name: not_null
  - name: archived
    type: BOOLEAN
    description: Whether the inner market is archived. All markets are currently unarchived (all False values). Field reserved for future lifecycle management.
    checks:
      - name: not_null
  - name: event_volume
    type: DOUBLE
    description: Total all-time event-level volume in USD aggregated across all temperature buckets. Ranges $5 to $7.6M with average $120K. Missing for ~10% of events. Higher values than market-level volume indicate multi-bucket trading activity.
  - name: event_liquidity
    type: DOUBLE
    description: Current event-level liquidity in USD aggregated across active buckets. Available for only ~5% of events. Ranges $0 to $606K. Low coverage aligns with most events being historically resolved.
  - name: event_resolution_source
    type: VARCHAR
    description: Free-text resolution source at the event level (0-101 characters). Often more detailed than market-level field. Contains full Wunderground URLs with ICAO codes. ~20% empty values. Essential for tracking Paris resolution source transitions.

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

GAMMA = "https://gamma-api.polymarket.com"
PAGE = 200
MAX_RETRIES = 5

CITY_SERIES = [
    "paris-daily-weather",
    "nyc-daily-weather",
    "london-daily-weather",
    "dallas-daily-weather",
    "toronto-daily-weather",
    "atlanta-daily-weather",
    "seoul-daily-weather",
    "buenos-aires-daily-weather",
    "seattle-daily-weather",
    "miami-daily-weather",
    "chicago-daily-weather",
    "ankara-daily-weather",
    "wellington-daily-weather",
    "sao-paulo-daily-weather",
    "lucknow-daily-weather",
    "munich-daily-weather",
    "tel-aviv-daily-weather",
    "tokyo-daily-weather",
    "shanghai-daily-weather",
    "singapore-daily-weather",
    "hong-kong-daily-weather",
    "milan-daily-weather",
    "madrid-daily-weather",
    "warsaw-daily-weather",
    "taipei-daily-weather",
    "chongqing-daily-weather",
    "beijing-daily-weather",
    "wuhan-daily-weather",
    "shenzhen-daily-weather",
]


def fetch_with_retry(url: str, params: dict):
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code in (429, 502, 503):
                wait = 15 * (attempt + 1)
                logger.warning("Gamma HTTP %d, retrying in %ds", r.status_code, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            logger.warning("Gamma error attempt %d/%d: %s", attempt + 1, MAX_RETRIES, e)
            time.sleep(wait)
    return None


def fetch_events_paginated(base_params: dict, max_events: int) -> list:
    out = []
    offset = 0
    while len(out) < max_events:
        page = fetch_with_retry(f"{GAMMA}/events", {**base_params, "limit": PAGE, "offset": offset})
        if not page:
            break
        out.extend(page)
        if len(page) < PAGE:
            break
        offset += PAGE
        time.sleep(0.4)
    return out[:max_events]


def fetch_weather_events(max_events: int) -> list:
    seen = {}

    # Pass 1: tag_slug=weather, both open and closed
    for closed_flag in (None, True, False):
        params = {"tag_slug": "weather", "ascending": "false", "order": "endDate"}
        if closed_flag is not None:
            params["closed"] = str(closed_flag).lower()
        events = fetch_events_paginated(params, max_events)
        logger.info("tag=weather closed=%s: %d events", closed_flag, len(events))
        for e in events:
            seen[e["id"]] = e

    # Pass 2: each city-daily-weather series
    for series in CITY_SERIES:
        events = fetch_events_paginated({"series_slug": series}, max_events)
        logger.info("series=%s: %d events", series, len(events))
        for e in events:
            seen[e["id"]] = e

    return list(seen.values())


def safe_float(v):
    if v is None or v == "" or v == "null":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def safe_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() == "true"
    return False


def explode_to_market_rows(event: dict, snap_ts) -> list:
    rows = []
    series_slug = None
    if event.get("series"):
        series_slug = event["series"][0].get("slug")
    tag_slugs = []
    for t in event.get("tags") or []:
        s = t.get("slug")
        if s:
            tag_slugs.append(s)

    for m in event.get("markets") or []:
        rows.append({
            "market_id": str(m.get("id", "")),
            "event_id": str(event.get("id", "")),
            "event_slug": event.get("slug"),
            "event_title": event.get("title"),
            "series_slug": series_slug,
            "tags_csv": ",".join(tag_slugs) if tag_slugs else None,
            "question": m.get("question"),
            "slug": m.get("slug"),
            "description": (m.get("description") or "")[:4000],
            "resolution_source": m.get("resolutionSource"),
            "end_date": m.get("endDate"),
            "start_date": m.get("startDate"),
            "created_at": m.get("createdAt"),
            "outcomes": m.get("outcomes") if isinstance(m.get("outcomes"), str) else json.dumps(m.get("outcomes")),
            "outcome_prices": m.get("outcomePrices") if isinstance(m.get("outcomePrices"), str) else json.dumps(m.get("outcomePrices")),
            "condition_id": m.get("conditionId"),
            "clob_token_ids": m.get("clobTokenIds") if isinstance(m.get("clobTokenIds"), str) else json.dumps(m.get("clobTokenIds")),
            "volume": safe_float(m.get("volume")),
            "liquidity": safe_float(m.get("liquidity")),
            "active": safe_bool(m.get("active")),
            "closed": safe_bool(m.get("closed")),
            "archived": safe_bool(m.get("archived")),
            "event_volume": safe_float(event.get("volume")),
            "event_liquidity": safe_float(event.get("liquidity")),
            "event_resolution_source": event.get("resolutionSource"),
            "extracted_at": snap_ts,
        })
    return rows


def materialize():
    max_events = int(os.environ.get("POLYMARKET_MAX_EVENTS", "8000"))
    logger.info("Fetching up to %d weather events", max_events)

    events = fetch_weather_events(max_events)
    logger.info("Total unique events: %d", len(events))

    snap_ts = datetime.now(timezone.utc)
    rows = []
    for e in events:
        rows.extend(explode_to_market_rows(e, snap_ts))

    if not rows:
        logger.warning("No markets fetched")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    for col in ("end_date", "start_date", "created_at"):
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    df = df.drop_duplicates(subset=["market_id", "extracted_at"], keep="last").reset_index(drop=True)
    df = df[df["market_id"] != ""].reset_index(drop=True)

    logger.info("Total inner markets: %d across %d events", len(df), df["event_id"].nunique())
    if "series_slug" in df:
        logger.info("Top series: %s", df["series_slug"].value_counts().head(15).to_dict())
    return df
