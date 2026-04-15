"""@bruin

name: raw.worldbank_urban
description: |
  Raw World Bank development indicators for analyzing global urbanization
  patterns and decoding urban form. Fetches 6 core indicators from the World Bank
  Open Data API spanning demographics, economics, and urban development across
  262 countries/regions (2000-2024).

  This dataset provides the temporal dimension for city-pulse: country-level
  urbanization trajectories over time that complement the city-level GHSL
  snapshot data and street network analysis. Enables analysis of urbanization
  stages (rural <30%, transitioning 30-60%, urban 60-80%, hyper-urban >80%),
  urban primacy patterns, and development correlations.

  Contains both sovereign countries (~217) and World Bank regional/income
  group aggregates (filtered out in staging). Data is ingested in 10-year
  chunks with retry logic to handle API rate limits and server instability.

  Source: World Bank Open Data API (https://data.worldbank.org)
  License: Creative Commons Attribution 4.0 (CC BY 4.0)
  No authentication required. Full refresh strategy due to occasional data revisions.
connection: bruin-playground-arsalan
tags:
  - domain:urban_planning
  - data_type:external_source
  - pipeline_role:raw
  - update_pattern:full_refresh
  - geography:global
  - temporal:time_series
  - sensitivity:public

materialization:
  type: table
  strategy: create+replace
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: country_code
    type: VARCHAR
    description: |
      World Bank country/region identifier. Mix of ISO 3166-1 alpha-3 country codes
      (USA, DEU, JPN) and World Bank regional/income group aggregates (AFE, EAS, HIC).
      262 distinct codes total. Empty strings indicate missing country mapping from API.
      Staging layer filters to retain only 3-character sovereign country codes.
    primary_key: true
    checks:
      - name: not_null
  - name: indicator_code
    type: VARCHAR
    description: |
      World Bank indicator identifier (6 distinct values). Core urbanization indicators:
      SP.URB.TOTL.IN.ZS (urban %), SP.URB.GROW (urban growth), EN.URB.LCTY.UR.ZS
      (largest city %), NY.GDP.PCAP.CD (GDP per capita), SP.POP.TOTL (population),
      EN.POP.DNST (density). Length varies 11-17 characters.
    primary_key: true
    checks:
      - name: not_null
  - name: year
    type: INTEGER
    description: |
      Calendar year of observation (2000-2024). Currently includes 2020-2024 data
      with some historical backfill. Average year ~2022 indicates recent data focus.
    primary_key: true
    checks:
      - name: not_null
  - name: country_name
    type: VARCHAR
    description: |
      Full country or region name as reported by World Bank (265 distinct values).
      Includes sovereign countries and regional aggregates like "Africa Eastern and Southern",
      "European Union". Length varies 4-73 characters. Used for display in dashboards.
  - name: indicator_name
    type: VARCHAR
    description: |
      Human-readable indicator description (6 distinct values). Examples: "Urban population
      (% of total)", "GDP per capita (current US$)", "Population density (people per sq km
      of land area)". Used for chart labels and data interpretation.
  - name: value
    type: DOUBLE
    description: |
      Observed indicator value with units varying by indicator. Ranges from -10.9 to 8.1B:
      percentages (0-100 for urbanization), annual % (-10.9 to ~20 for growth rates),
      current US$ (0-130K for GDP per capita), counts (8K to 1.4B for population),
      people/sq km (0.4 to 26K for density). Null values filtered out during ingestion.
  - name: extracted_at
    type: TIMESTAMP
    description: |
      UTC timestamp when this batch was fetched from World Bank API. Single timestamp
      per batch indicating full refresh strategy. Used for deduplication in staging
      when append strategy creates overlapping runs.
    checks:
      - name: not_null

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

WB_BASE_URL = "https://api.worldbank.org/v2/country/all/indicator"

INDICATORS = [
    {"code": "SP.URB.TOTL.IN.ZS", "name": "Urban population (% of total)"},
    {"code": "SP.URB.GROW", "name": "Urban population growth (annual %)"},
    {"code": "EN.URB.LCTY.UR.ZS", "name": "Population in largest city (% of urban pop)"},
    {"code": "NY.GDP.PCAP.CD", "name": "GDP per capita (current US$)"},
    {"code": "SP.POP.TOTL", "name": "Population, total"},
    {"code": "EN.POP.DNST", "name": "Population density (people per sq km of land area)"},
]

def fetch_chunk(indicator_code: str, start_year: int, end_year: int) -> list[dict]:
    """Fetch one year-range chunk for a single indicator (single page, high per_page)."""
    url = f"{WB_BASE_URL}/{indicator_code}"
    params = {
        "format": "json",
        "per_page": 20000,
        "date": f"{start_year}:{end_year}",
    }

    for attempt in range(5):
        try:
            resp = requests.get(url, params=params, timeout=60)
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            logger.warning(
                "Network error for %s (%d-%d), retrying in %ds: %s",
                indicator_code, start_year, end_year, wait, e,
            )
            time.sleep(wait)
            continue

        if resp.status_code == 429:
            wait = 30 * (attempt + 1)
            logger.warning("Rate limited on %s, backing off %ds", indicator_code, wait)
            time.sleep(wait)
            continue

        if resp.status_code in (400, 500, 502, 503):
            wait = 15 * (attempt + 1)
            logger.warning(
                "Server error %d for %s (%d-%d), retrying in %ds",
                resp.status_code, indicator_code, start_year, end_year, wait,
            )
            time.sleep(wait)
            continue

        resp.raise_for_status()
        break
    else:
        logger.warning("Failed to fetch %s (%d-%d) after 5 attempts, skipping chunk", indicator_code, start_year, end_year)
        return []

    payload = resp.json()

    if not isinstance(payload, list) or len(payload) < 2:
        logger.warning("Unexpected response format for %s (%d-%d)", indicator_code, start_year, end_year)
        return []

    records = payload[1]
    if records is None:
        return []

    rows = []
    for rec in records:
        if rec.get("value") is not None:
            rows.append({
                "country_code": rec["countryiso3code"],
                "country_name": rec["country"]["value"],
                "indicator_code": rec["indicator"]["id"],
                "indicator_name": rec["indicator"]["value"],
                "year": int(rec["date"]),
                "value": float(rec["value"]),
            })
    return rows

def fetch_indicator(indicator_code: str, start_year: int, end_year: int) -> list[dict]:
    """Fetch all country-year observations in 10-year chunks for reliability."""
    all_records = []
    chunk_start = start_year

    while chunk_start <= end_year:
        chunk_end = min(chunk_start + 9, end_year)
        logger.info("  Chunk %d-%d...", chunk_start, chunk_end)

        rows = fetch_chunk(indicator_code, chunk_start, chunk_end)
        all_records.extend(rows)

        logger.info("    %d observations", len(rows))
        chunk_start = chunk_end + 1
        time.sleep(0.5)

    return all_records

def materialize():
    # Always fetch the full historical range — this is reference data
    start_year = 2000
    end_year = 2024

    logger.info("Interval: %d to %d", start_year, end_year)
    logger.info("Fetching %d World Bank indicators", len(INDICATORS))

    all_rows = []

    for ind in INDICATORS:
        code = ind["code"]
        logger.info("Fetching %s (%s)...", code, ind["name"])

        records = fetch_indicator(code, start_year, end_year)
        all_rows.extend(records)

        logger.info("  %s: %d observations", code, len(records))
        time.sleep(0.5)

    if not all_rows:
        raise RuntimeError("No data fetched from any indicator — check connectivity")

    df = pd.DataFrame(all_rows)
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Total: %d rows across %d indicators", len(df), len(INDICATORS))
    return df
