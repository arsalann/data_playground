"""@bruin

name: raw.worldbank_indicators
description: |
  Raw World Bank development indicators for analyzing global fertility decline
  patterns and their correlation with economic development. Fetches 10 carefully
  selected indicators from the World Bank Open Data API spanning demographics,
  economics, education, and health for 200+ countries and regional aggregations.

  This dataset supports the "baby-bust" analysis showing how fertility rates
  decline universally as countries develop economically. Includes both individual
  countries and World Bank regional/income group aggregations (identifiable by
  3-character codes like AFE, ARB, etc).

  Key indicators: fertility rate (dependent variable), GDP per capita PPP,
  urbanization, female education/labor participation, life expectancy, infant
  mortality, health expenditure, inflation, and GNI per capita.

  Data spans 1960-2024 but actual coverage varies by indicator and country.
  Some indicators (health expenditure) begin only in 2000; others have 2-year
  publication lag. Missing values are excluded during ingestion.

  Source: World Bank Open Data API (https://data.worldbank.org)
  License: Creative Commons Attribution 4.0 (CC BY 4.0)
  No authentication required.
connection: bruin-playground-arsalan
tags:
  - domain:demographics
  - domain:economics
  - domain:development
  - data_type:external_source
  - data_type:fact_table
  - pipeline_role:raw
  - sensitivity:public
  - update_pattern:append_only
  - source:worldbank_api

materialization:
  type: table
  strategy: append
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: country_code
    type: VARCHAR
    description: |
      World Bank country/region code. Mix of ISO 3166-1 alpha-3 country codes
      (USA, DEU, JPN) and World Bank regional/income group codes (AFE=Africa Eastern
      & Southern, ARB=Arab World, HPC=Heavily Indebted Poor Countries, etc).
      Empty strings indicate data quality issues in source.
    primary_key: true
    checks:
      - name: not_null
  - name: indicator_code
    type: VARCHAR
    description: |
      World Bank indicator identifier. Always one of 10 predefined indicators:
      SP.DYN.TFRT.IN (fertility rate), SP.DYN.LE00.IN (life expectancy),
      NY.GDP.PCAP.PP.CD (GDP per capita PPP), SP.URB.TOTL.IN.ZS (urbanization),
      SE.TER.ENRR.FE (female tertiary enrollment), SL.TLF.CACT.FE.ZS (female labor),
      SP.DYN.IMRT.IN (infant mortality), SH.XPD.CHEX.GD.ZS (health expenditure),
      FP.CPI.TOTL (consumer price index), NY.GNP.PCAP.CD (GNI per capita).
    primary_key: true
    checks:
      - name: not_null
  - name: year
    type: INTEGER
    description: |
      Calendar year of observation (1960-2024). Actual data availability varies
      by indicator and country. Most recent data typically has 1-2 year
      publication lag.
    primary_key: true
    checks:
      - name: not_null
  - name: country_name
    type: VARCHAR
    description: |
      Full country or region name as reported by World Bank. Includes both
      sovereign countries and regional/income group aggregations. Examples:
      "United States", "Germany", "Africa Eastern and Southern", "Euro area",
      "High income". Max length 73 characters.
  - name: indicator_name
    type: VARCHAR
    description: |
      Human-readable indicator description. Always one of 10 predefined names
      corresponding to the indicator codes. Used for display in analysis but
      indicator_code is the stable identifier.
  - name: value
    type: DOUBLE
    description: |
      Observed indicator value. Units vary by indicator: births per woman
      (fertility), years (life expectancy), current international $ PPP (GDP),
      percentage (urbanization, enrollment, labor participation), per 1000 births
      (infant mortality), % of GDP (health expenditure), index 2010=100 (CPI),
      current US$ (GNI). Only non-null values are included during ingestion.
  - name: extracted_at
    type: TIMESTAMP
    description: |
      UTC timestamp when this batch was fetched from World Bank API. Used for
      deduplication in staging layer. All rows in a single pipeline run have
      identical timestamp.
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
    {"code": "SP.DYN.TFRT.IN", "name": "Fertility rate (births per woman)"},
    {"code": "SP.DYN.LE00.IN", "name": "Life expectancy at birth (years)"},
    {"code": "NY.GDP.PCAP.PP.CD", "name": "GDP per capita, PPP (current intl $)"},
    {"code": "SP.URB.TOTL.IN.ZS", "name": "Urban population (% of total)"},
    {"code": "SE.TER.ENRR.FE", "name": "Female tertiary enrollment (% gross)"},
    {"code": "SL.TLF.CACT.FE.ZS", "name": "Female labor force participation (%)"},
    {"code": "SP.DYN.IMRT.IN", "name": "Infant mortality rate (per 1,000 live births)"},
    {"code": "SH.XPD.CHEX.GD.ZS", "name": "Health expenditure (% of GDP)"},
    {"code": "FP.CPI.TOTL", "name": "Consumer price index (2010 = 100)"},
    {"code": "NY.GNP.PCAP.CD", "name": "GNI per capita (current US$)"},
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

        if resp.status_code in (500, 502, 503):
            wait = 15 * (attempt + 1)
            logger.warning(
                "Server error %d for %s, retrying in %ds",
                resp.status_code, indicator_code, wait,
            )
            time.sleep(wait)
            continue

        resp.raise_for_status()
        break
    else:
        logger.error("Failed to fetch %s (%d-%d) after 5 attempts", indicator_code, start_year, end_year)
        return []

    payload = resp.json()

    # World Bank API returns [metadata, data] — two-element list
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
    start_date = os.environ.get("BRUIN_START_DATE", "1960-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2024-12-31")

    start_year = int(start_date[:4])
    end_year = int(end_date[:4])

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
