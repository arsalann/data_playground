"""@bruin
name: raw.oecd_tl2_income
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  OECD TL2 regional household income observations fetched from DBnomics'
  mirror of the OECD Data Explorer regional economy table
  DSD_REG_ECO@DF_INC. Keeps annual TL2 primary income (B5N) and net
  disposable income (B6N), expressed as current-price USD PPP per person.

  Source: OECD Regional Economy, Income - Regions
  URL: https://data-explorer.oecd.org/vis?df[ds]=dsDisseminateFinalDMZ&df[id]=DSD_REG_ECO@DF_INC
  API mirror: https://api.db.nomics.world/v22/series/OECD/DSD_REG_ECO%40DF_INC
  License: OECD data terms; DBnomics provides API access to the OECD dataset.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: region_code
    type: VARCHAR
    description: OECD TL2 reference area code, e.g. DE1 for Baden-Wuerttemberg or CA35 for Ontario.
    primary_key: true
  - name: year
    type: INTEGER
    description: Calendar year of the annual OECD observation.
    primary_key: true
  - name: measure_code
    type: VARCHAR
    description: OECD income measure code; B5N is net primary income and B6N is net disposable income.
    primary_key: true
  - name: region_name
    type: VARCHAR
    description: Human-readable OECD region name from the REF_AREA dimension labels.
  - name: country_prefix
    type: VARCHAR
    description: OECD country prefix derived from the TL2 region code and normalized for grouping.
  - name: territorial_level
    type: VARCHAR
    description: OECD territorial level; this asset is filtered to TL2.
  - name: territorial_type
    type: VARCHAR
    description: OECD territorial typology code for the region, if populated.
  - name: price_base
    type: VARCHAR
    description: OECD price-base code; V means current prices.
  - name: unit_measure
    type: VARCHAR
    description: OECD unit code; USD_PPP_PS means US dollars, PPP converted, per person.
  - name: value
    type: DOUBLE
    description: Income value in current-price USD PPP per person.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when the DBnomics API response was extracted.

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

BASE_URL = "https://api.db.nomics.world/v22/series/OECD/DSD_REG_ECO%40DF_INC"
DIMENSIONS = {
    "FREQ": ["A"],
    "TERRITORIAL_LEVEL": ["TL2"],
    "MEASURE": ["B5N", "B6N"],
    "ACTIVITY": ["_T"],
    "PRICES": ["V"],
    "UNIT_MEASURE": ["USD_PPP_PS"],
}


def country_prefix(region_code: str) -> str:
    if region_code.startswith("ME"):
        return "ME"
    if region_code.startswith("JP"):
        return "JP"
    return region_code[:2]


def fetch_page(offset: int, limit: int = 1000) -> dict:
    params = {
        "dimensions": json.dumps(DIMENSIONS),
        "limit": limit,
        "offset": offset,
        "observations": 1,
    }

    for attempt in range(5):
        try:
            response = requests.get(BASE_URL, params=params, timeout=90)
            if response.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"retryable HTTP {response.status_code}")
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            wait = 2 ** attempt
            logger.warning("DBnomics income page offset=%d failed, retrying in %ss: %s", offset, wait, exc)
            time.sleep(wait)

    raise RuntimeError(f"Failed to fetch OECD income page at offset {offset}")


def fetch_income() -> pd.DataFrame:
    rows: list[dict] = []
    offset = 0
    total = None
    extracted_at = datetime.now(timezone.utc)

    while total is None or offset < total:
        payload = fetch_page(offset)
        series = payload["series"]
        total = series["num_found"]
        docs = series["docs"]
        labels = payload["dataset"]["dimensions_values_labels"].get("REF_AREA", {})

        logger.info("Fetched income series %d-%d of %d", offset + 1, offset + len(docs), total)

        for item in docs:
            dims = item["dimensions"]
            region_code = dims["REF_AREA"]
            periods = item.get("period", [])
            values = item.get("value", [])
            for period, value in zip(periods, values):
                if value is None:
                    continue
                rows.append(
                    {
                        "region_code": region_code,
                        "year": int(period),
                        "measure_code": dims["MEASURE"],
                        "region_name": labels.get(region_code, region_code),
                        "country_prefix": country_prefix(region_code),
                        "territorial_level": dims["TERRITORIAL_LEVEL"],
                        "territorial_type": dims.get("TERRITORIAL_TYPE"),
                        "price_base": dims["PRICES"],
                        "unit_measure": dims["UNIT_MEASURE"],
                        "value": float(value),
                        "extracted_at": extracted_at,
                    }
                )

        offset += len(docs)
        time.sleep(0.5)

    return pd.DataFrame(rows)


def materialize():
    start_year = int(os.environ.get("BRUIN_START_DATE", "1995")[:4])
    end_year = int(os.environ.get("BRUIN_END_DATE", str(datetime.now(timezone.utc).year))[:4])
    logger.info("Interval: %s to %s", start_year, end_year)

    df = fetch_income()
    df = df[(df["year"] >= start_year) & (df["year"] <= end_year)].copy()
    logger.info("Materializing %d OECD TL2 income observations", len(df))
    return df
