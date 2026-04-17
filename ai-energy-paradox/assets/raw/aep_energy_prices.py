"""@bruin
name: raw.aep_energy_prices
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches historical data for 6 FRED series covering energy prices
  and electricity cost indices. Uses append strategy with
  BRUIN_START_DATE / BRUIN_END_DATE. Deduplication handled in staging.

  Data source: https://fred.stlouisfed.org
  License: Public domain (US government data)

materialization:
  type: table
  strategy: append

secrets:
  - key: fred_api_key

columns:
  - name: observation_date
    type: DATE
    description: Date of the price observation
    primary_key: true
  - name: series_id
    type: VARCHAR
    description: "FRED series identifier (e.g. DCOILBRENTEU)"
    primary_key: true
  - name: series_name
    type: VARCHAR
    description: Human-readable name of the series
  - name: value
    type: DOUBLE
    description: Price or index value (units vary by series)
  - name: unit
    type: VARCHAR
    description: "Unit of measurement (USD/barrel, USD/gallon, USD/MMBtu, index)"
  - name: frequency
    type: VARCHAR
    description: "Observation frequency (daily, weekly, monthly)"
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

SERIES = [
    {"series_id": "DCOILBRENTEU", "series_name": "Brent Crude Oil", "unit": "USD/barrel", "frequency": "daily"},
    {"series_id": "DCOILWTICO", "series_name": "WTI Crude Oil", "unit": "USD/barrel", "frequency": "daily"},
    {"series_id": "GASREGW", "series_name": "US Regular Gasoline", "unit": "USD/gallon", "frequency": "weekly"},
    {"series_id": "DHHNGSP", "series_name": "Henry Hub Natural Gas", "unit": "USD/MMBtu", "frequency": "daily"},
    {"series_id": "CPIENGSL", "series_name": "CPI Energy", "unit": "index", "frequency": "monthly"},
    {"series_id": "PCU2211--2211--", "series_name": "Electric Power Price Index", "unit": "index", "frequency": "monthly"},
]


def fetch_series(series_id: str, api_key: str, start_date: str, end_date: str) -> list[dict]:
    """Fetch observations for a single FRED series with retry logic."""
    params = {
        "series_id": series_id,
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
            logger.warning("Network error for %s, retrying in %ds: %s", series_id, wait, e)
            time.sleep(wait)
            continue

        if resp.status_code == 400:
            logger.warning("Bad request for %s — series may not exist or date range invalid", series_id)
            return []

        if resp.status_code == 429:
            wait = 30 * (attempt + 1)
            logger.warning("Rate limited on %s, backing off %ds", series_id, wait)
            time.sleep(wait)
            continue

        if resp.status_code in (500, 502, 503):
            wait = 15 * (attempt + 1)
            logger.warning("Server error %d for %s, retrying in %ds", resp.status_code, series_id, wait)
            time.sleep(wait)
            continue

        resp.raise_for_status()
        data = resp.json()
        return data.get("observations", [])

    logger.error("Failed to fetch %s after 5 attempts", series_id)
    return []


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2015-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2026-04-15")
    api_key = os.environ.get("fred_api_key")

    if not api_key:
        raise RuntimeError("fred_api_key secret is not set")

    logger.info("Interval: %s to %s", start_date, end_date)
    logger.info("Fetching %d FRED series", len(SERIES))

    all_rows = []

    for series_info in SERIES:
        sid = series_info["series_id"]
        logger.info("Fetching %s (%s)...", sid, series_info["series_name"])

        observations = fetch_series(sid, api_key, start_date, end_date)

        count = 0
        for obs in observations:
            if obs["value"] == ".":
                continue
            all_rows.append({
                "observation_date": obs["date"],
                "series_id": sid,
                "series_name": series_info["series_name"],
                "value": float(obs["value"]),
                "unit": series_info["unit"],
                "frequency": series_info["frequency"],
            })
            count += 1

        logger.info("  %s: %d observations", sid, count)
        time.sleep(0.5)

    if not all_rows:
        raise RuntimeError("No data fetched from any series — check API key and date range")

    df = pd.DataFrame(all_rows)
    df["observation_date"] = pd.to_datetime(df["observation_date"]).dt.date
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Total: %d rows across %d series", len(df), len(SERIES))
    return df
