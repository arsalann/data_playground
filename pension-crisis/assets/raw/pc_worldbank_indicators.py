"""@bruin

name: raw.pc_worldbank_indicators
description: |
  Contextual economic indicators from the World Bank WDI API for the 38 OECD countries.
  Used to enrich pension analysis with GDP per capita PPP, fertility rate, and labour
  force participation of older workers (55-64).

  Indicators:
  - NY.GDP.PCAP.PP.CD  GDP per capita, PPP (current international $)
  - SP.DYN.TFRT.IN     Total fertility rate (births per woman)
  - SL.TLF.ACTI.ZS     Labor force participation rate, 15+, total (%)
  - SL.TLF.ACTI.1524.ZS Labor force participation 15-24 (control series)

  Why the World Bank (not OECD) for these: the WB WDI is the canonical cross-country
  reference for macro-economic indicators, and we only need it for contextual panels
  where OECD doesn't publish directly.

  Source: World Bank Open Data API (https://api.worldbank.org/v2). CC BY 4.0. No auth.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: append
image: python:3.11

columns:
  - name: iso3_code
    type: VARCHAR
    description: ISO 3166-1 alpha-3 country code.
    primary_key: true
  - name: indicator_code
    type: VARCHAR
    description: World Bank indicator code.
    primary_key: true
  - name: year
    type: INTEGER
    description: Observation year.
    primary_key: true
  - name: country_name
    type: VARCHAR
    description: WB-reported country name.
  - name: indicator_name
    type: VARCHAR
    description: Human-readable indicator description.
  - name: value
    type: DOUBLE
    description: Indicator value (units vary).
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp of ingestion.

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
    {"code": "NY.GDP.PCAP.PP.CD", "name": "GDP per capita, PPP (current intl $)"},
    {"code": "SP.DYN.TFRT.IN", "name": "Fertility rate (births per woman)"},
    {"code": "SL.TLF.ACTI.ZS", "name": "Labor force participation 15+ (%)"},
]

OECD_ISO3 = {
    "AUS", "AUT", "BEL", "CAN", "CHL", "COL", "CRI", "CZE", "DNK", "EST",
    "FIN", "FRA", "DEU", "GRC", "HUN", "ISL", "IRL", "ISR", "ITA", "JPN",
    "KOR", "LVA", "LTU", "LUX", "MEX", "NLD", "NZL", "NOR", "POL", "PRT",
    "SVK", "SVN", "ESP", "SWE", "CHE", "TUR", "GBR", "USA",
}


def fetch_chunk(indicator_code: str, start_year: int, end_year: int) -> list[dict]:
    url = f"{WB_BASE_URL}/{indicator_code}"
    params = {"format": "json", "per_page": 20000, "date": f"{start_year}:{end_year}"}

    for attempt in range(5):
        try:
            resp = requests.get(url, params=params, timeout=60)
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            logger.warning("Network error: retrying in %ds (%s)", wait, e)
            time.sleep(wait)
            continue

        if resp.status_code == 429:
            wait = 30 * (attempt + 1)
            logger.warning("Rate limited, backing off %ds", wait)
            time.sleep(wait)
            continue

        if resp.status_code in (500, 502, 503):
            wait = 15 * (attempt + 1)
            logger.warning("Server error %d, retrying in %ds", resp.status_code, wait)
            time.sleep(wait)
            continue

        resp.raise_for_status()
        break
    else:
        logger.error("Failed %s (%d-%d) after 5 attempts", indicator_code, start_year, end_year)
        return []

    payload = resp.json()
    if not isinstance(payload, list) or len(payload) < 2 or payload[1] is None:
        return []

    rows = []
    for rec in payload[1]:
        if rec.get("value") is None:
            continue
        iso3 = rec.get("countryiso3code", "")
        if iso3 not in OECD_ISO3:
            continue
        rows.append({
            "iso3_code": iso3,
            "country_name": rec["country"]["value"],
            "indicator_code": rec["indicator"]["id"],
            "indicator_name": rec["indicator"]["value"],
            "year": int(rec["date"]),
            "value": float(rec["value"]),
        })
    return rows


def fetch_indicator(indicator_code: str, start_year: int, end_year: int) -> list[dict]:
    rows = []
    chunk_start = start_year
    while chunk_start <= end_year:
        chunk_end = min(chunk_start + 9, end_year)
        logger.info("  chunk %d-%d", chunk_start, chunk_end)
        rows.extend(fetch_chunk(indicator_code, chunk_start, chunk_end))
        chunk_start = chunk_end + 1
        time.sleep(0.5)
    return rows


def materialize():
    start_year = int(os.environ.get("WB_START_YEAR", "1990"))
    end_year = int(os.environ.get("WB_END_YEAR", "2025"))
    logger.info("Interval: %d-%d", start_year, end_year)

    all_rows = []
    for ind in INDICATORS:
        logger.info("Fetching %s (%s)", ind["code"], ind["name"])
        records = fetch_indicator(ind["code"], start_year, end_year)
        logger.info("  %s: %d OECD observations", ind["code"], len(records))
        all_rows.extend(records)
        time.sleep(0.5)

    if not all_rows:
        raise RuntimeError("No data returned from WB API — check connectivity")

    df = pd.DataFrame(all_rows)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Total: %d rows", len(df))
    return df
