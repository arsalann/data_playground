"""@bruin
name: epias_raw.fred_tryusd
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches the CCUSMA02TRM618N series (monthly average TRY per USD) from FRED.
  Uses create+replace strategy so the table is fully rebuilt each run.

  Data source: https://fred.stlouisfed.org/series/CCUSMA02TRM618N
  License: Public domain (OECD via FRED)

materialization:
  type: table
  strategy: create+replace

secrets:
  - key: fred_api_key

columns:
  - name: observation_date
    type: DATE
    description: First day of month for the exchange rate observation
    primary_key: true
  - name: value
    type: DOUBLE
    description: Monthly average Turkish Lira per US Dollar
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was fetched from the FRED API

@bruin"""

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

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
SERIES_ID = "CCUSMA02TRM618N"


def fetch_series(api_key: str, start_date: str, end_date: str) -> list[dict]:
    """Fetch observations for the DEXTUUS series with retry logic."""
    params = {
        "series_id": SERIES_ID,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date,
    }

    for attempt in range(5):
        try:
            resp = requests.get(FRED_BASE_URL, params=params, timeout=30)
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            logger.warning("Network error for %s, retrying in %ds: %s", SERIES_ID, wait, e)
            time.sleep(wait)
            continue

        if resp.status_code == 400:
            logger.warning("Bad request for %s — series may not exist or date range invalid", SERIES_ID)
            return []

        if resp.status_code == 429:
            wait = 30 * (attempt + 1)
            logger.warning("Rate limited on %s, backing off %ds", SERIES_ID, wait)
            time.sleep(wait)
            continue

        if resp.status_code in (500, 502, 503):
            wait = 15 * (attempt + 1)
            logger.warning("Server error %d for %s, retrying in %ds", resp.status_code, SERIES_ID, wait)
            time.sleep(wait)
            continue

        resp.raise_for_status()
        data = resp.json()
        return data.get("observations", [])

    logger.error("Failed to fetch %s after 5 attempts", SERIES_ID)
    return []


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2015-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2026-04-16")
    api_key = os.environ.get("fred_api_key")

    if not api_key:
        raise RuntimeError("fred_api_key secret is not set")

    logger.info("Interval: %s to %s", start_date, end_date)
    logger.info("Fetching FRED series %s (TRY/USD exchange rate)", SERIES_ID)

    observations = fetch_series(api_key, start_date, end_date)

    rows = []
    for obs in observations:
        if obs["value"] == ".":
            continue
        rows.append({
            "observation_date": obs["date"],
            "value": float(obs["value"]),
        })

    if not rows:
        raise RuntimeError("No data fetched — check API key and date range")

    df = pd.DataFrame(rows)
    df["observation_date"] = pd.to_datetime(df["observation_date"]).dt.date
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Total: %d observations for %s", len(df), SERIES_ID)
    return df
