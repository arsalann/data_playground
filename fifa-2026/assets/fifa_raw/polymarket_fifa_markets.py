"""@bruin

name: fifa_raw.polymarket_fifa_markets
description: |
  Polymarket prediction-market metadata for the 2026 FIFA World Cup.

  Two-pass extraction from the Polymarket Gamma API:
    1. Events with `tag_slug=fifa-world-cup` and `tag_slug=fifa` (curated FIFA tag).
    2. Events from known FIFA-2026 series slugs (winner, top-scorer, golden-boot, group-winner).

  Each parent event is exploded into per-outcome market rows. For the
  "FIFA 2026 Winner" market, each row corresponds to one team's "Yes" outcome
  with its own CLOB token id, used downstream to fetch tick-level price history.

  Source: Polymarket Gamma API (https://gamma-api.polymarket.com/events) — no
  authentication required.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - prediction_markets
  - external_source
  - raw_data

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
    description: Polymarket inner-market id (one outcome per market).
    primary_key: true
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: Snapshot timestamp.
    primary_key: true
    checks:
      - name: not_null
  - name: event_id
    type: VARCHAR
    checks:
      - name: not_null
  - name: event_slug
    type: VARCHAR
  - name: event_title
    type: VARCHAR
  - name: series_slug
    type: VARCHAR
  - name: tags_csv
    type: VARCHAR
  - name: question
    type: VARCHAR
  - name: slug
    type: VARCHAR
  - name: description
    type: VARCHAR
  - name: end_date
    type: TIMESTAMP
  - name: start_date
    type: TIMESTAMP
  - name: created_at
    type: TIMESTAMP
  - name: outcomes
    type: VARCHAR
  - name: outcome_prices
    type: VARCHAR
  - name: condition_id
    type: VARCHAR
  - name: clob_token_ids
    type: VARCHAR
  - name: volume
    type: DOUBLE
  - name: liquidity
    type: DOUBLE
  - name: active
    type: BOOLEAN
  - name: closed
    type: BOOLEAN
  - name: archived
    type: BOOLEAN
  - name: event_volume
    type: DOUBLE
  - name: event_liquidity
    type: DOUBLE

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

FIFA_TAGS = ["fifa-world-cup", "fifa", "world-cup", "soccer", "football"]
FIFA_SERIES = [
    "fifa-world-cup-2026-winner",
    "fifa-world-cup-2026-top-scorer",
    "fifa-world-cup-2026-golden-boot",
    "fifa-world-cup-2026-group-winner",
    "fifa-world-cup-2026",
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


def fetch_fifa_events(max_events: int) -> list:
    seen = {}
    for tag in FIFA_TAGS:
        for closed_flag in (None, True, False):
            params = {"tag_slug": tag, "ascending": "false", "order": "endDate"}
            if closed_flag is not None:
                params["closed"] = str(closed_flag).lower()
            events = fetch_events_paginated(params, max_events)
            logger.info("tag=%s closed=%s: %d events", tag, closed_flag, len(events))
            for e in events:
                seen[e["id"]] = e
    for series in FIFA_SERIES:
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
    tag_slugs = [t.get("slug") for t in (event.get("tags") or []) if t.get("slug")]
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
            "extracted_at": snap_ts,
        })
    return rows


def materialize():
    max_events = int(os.environ.get("POLYMARKET_MAX_EVENTS", "4000"))
    events = fetch_fifa_events(max_events)
    logger.info("Total unique FIFA events: %d", len(events))

    snap_ts = datetime.now(timezone.utc)
    rows = []
    for e in events:
        rows.extend(explode_to_market_rows(e, snap_ts))

    if not rows:
        logger.warning("No FIFA markets fetched")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    for col in ("end_date", "start_date", "created_at"):
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    df = df.drop_duplicates(subset=["market_id", "extracted_at"], keep="last").reset_index(drop=True)
    df = df[df["market_id"] != ""].reset_index(drop=True)

    logger.info("FIFA markets: %d across %d events", len(df), df["event_id"].nunique())
    if "series_slug" in df:
        logger.info("Top series: %s", df["series_slug"].value_counts().head(15).to_dict())
    return df
