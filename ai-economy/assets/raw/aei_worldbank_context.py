"""@bruin

name: raw.aei_worldbank_context
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  World Bank Open Data indicators for country-level enrichment of AEI geographic
  adoption metrics. Three indicators × 2020-2024.

  - NY.GDP.PCAP.CD  — GDP per capita (current US$)
  - SL.TLF.CACT.ZS  — Labor force participation rate (% of total pop 15+)
  - SP.POP.TOTL     — Population, total

  Ingested in 10-year chunks with 5 retries and exponential backoff, reusing the
  pattern from `baby-bust/assets/raw/worldbank_indicators.py`.

  Append strategy — deduplicate downstream in staging with QUALIFY ROW_NUMBER().

  Source: https://api.worldbank.org/v2/country/all/indicator/<code>?format=json
  License: CC BY 4.0

materialization:
  type: table
  strategy: append

columns:
  - name: country_code
    type: VARCHAR
    description: World Bank country/region code (ISO 3166-1 alpha-3 for sovereign countries; aggregate codes like `WLD`, `HIC` for groupings).
    primary_key: true
  - name: indicator_code
    type: VARCHAR
    description: World Bank indicator identifier.
    primary_key: true
  - name: year
    type: INTEGER
    description: Calendar year of observation (2020-2024).
    primary_key: true
  - name: country_name
    type: VARCHAR
    description: Full country or region name as reported by World Bank.
  - name: indicator_name
    type: VARCHAR
    description: Human-readable indicator description.
  - name: value
    type: DOUBLE
    description: Indicator value (units vary — see indicator_code).
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this batch was fetched.

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
    {"code": "NY.GDP.PCAP.CD", "name": "GDP per capita (current US$)"},
    {"code": "SL.TLF.CACT.ZS", "name": "Labor force participation rate, total (% of total population ages 15+)"},
    {"code": "SP.POP.TOTL", "name": "Population, total"},
]


def fetch_chunk(indicator_code: str, start_year: int, end_year: int) -> list[dict]:
    url = f"{WB_BASE_URL}/{indicator_code}"
    params = {"format": "json", "per_page": 20000, "date": f"{start_year}:{end_year}"}

    for attempt in range(5):
        try:
            resp = requests.get(url, params=params, timeout=60)
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            logger.warning("Network error %s: retry in %ds", e, wait)
            time.sleep(wait)
            continue

        if resp.status_code == 429:
            wait = 30 * (attempt + 1)
            logger.warning("Rate limited: backing off %ds", wait)
            time.sleep(wait)
            continue
        if resp.status_code in (500, 502, 503):
            wait = 15 * (attempt + 1)
            logger.warning("Server error %d: retry in %ds", resp.status_code, wait)
            time.sleep(wait)
            continue

        resp.raise_for_status()
        break
    else:
        logger.error("Failed %s %d-%d after 5 attempts", indicator_code, start_year, end_year)
        return []

    payload = resp.json()
    if not isinstance(payload, list) or len(payload) < 2 or payload[1] is None:
        return []

    rows = []
    for rec in payload[1]:
        if rec.get("value") is None:
            continue
        rows.append({
            "country_code": rec.get("countryiso3code") or "",
            "country_name": rec["country"]["value"],
            "indicator_code": rec["indicator"]["id"],
            "indicator_name": rec["indicator"]["value"],
            "year": int(rec["date"]),
            "value": float(rec["value"]),
        })
    return rows


def fetch_indicator(indicator_code: str, start_year: int, end_year: int) -> list[dict]:
    all_records = []
    chunk_start = start_year
    while chunk_start <= end_year:
        chunk_end = min(chunk_start + 9, end_year)
        logger.info("  Chunk %d-%d...", chunk_start, chunk_end)
        rows = fetch_chunk(indicator_code, chunk_start, chunk_end)
        logger.info("    %d observations", len(rows))
        all_records.extend(rows)
        chunk_start = chunk_end + 1
        time.sleep(0.5)
    return all_records


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2020-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2024-12-31")
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])

    logger.info("World Bank: %d-%d, %d indicators", start_year, end_year, len(INDICATORS))

    all_rows = []
    for ind in INDICATORS:
        logger.info("Indicator %s (%s)", ind["code"], ind["name"])
        records = fetch_indicator(ind["code"], start_year, end_year)
        logger.info("  %s: %d", ind["code"], len(records))
        all_rows.extend(records)
        time.sleep(0.5)

    if not all_rows:
        raise RuntimeError("No World Bank data fetched — check connectivity")

    df = pd.DataFrame(all_rows)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Total: %d rows", len(df))
    return df
