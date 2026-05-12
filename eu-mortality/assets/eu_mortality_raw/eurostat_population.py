"""@bruin

name: eu_mortality_raw.eurostat_population
description: |
  Population on 1 January by age group and NUTS3 region for the EU-27, 2015-2025.

  Source: Eurostat DEMO_R_PJANGRP3 -- "Population on 1 January by age group, sex
  and NUTS 3 region". Pulled for total-sex and a curated age subset:
    - TOTAL: full population (denominator).
    - Y65-69, Y70-74, Y75-79, Y_GE80: heat-vulnerable age groups (summed to "65+"
      in staging).

  Yearly granularity (1 January). One request per year (each request returns
  ~6,000 cells, well below Eurostat's ~50k limit).

  https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/DEMO_R_PJANGRP3
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: nuts_id
    type: VARCHAR
    description: NUTS3 region code.
    primary_key: true
    checks:
      - name: not_null
  - name: ref_year
    type: INTEGER
    description: Reference year (1 January).
    primary_key: true
    checks:
      - name: not_null
  - name: age_group
    type: VARCHAR
    description: Eurostat age class code (TOTAL, Y65-69, Y70-74, Y75-79, Y_GE80).
    primary_key: true
  - name: population
    type: DOUBLE
    description: Population count for the (NUTS3, year, age_group, sex=Total) combination.
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

EUROSTAT_API = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/DEMO_R_PJANGRP3"
AGES = ["TOTAL", "Y65-69", "Y70-74", "Y75-79", "Y_GE80"]
MAX_RETRIES = 5


def fetch_year(year: int) -> dict:
    params = [
        ("format", "JSON"),
        ("lang", "EN"),
        ("sex", "T"),
        ("time", str(year)),
    ] + [("age", a) for a in AGES]

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(EUROSTAT_API, params=params, timeout=180)
            if r.status_code in (429, 502, 503):
                wait = 15 * (attempt + 1)
                logger.warning("HTTP %d retry in %ds", r.status_code, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            wait = 15 * (attempt + 1)
            logger.warning("Network error attempt %d: %s", attempt + 1, e)
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch population year {year}")


def parse_year(payload: dict, year: int) -> pd.DataFrame:
    dims = payload.get("dimension", {})
    size = payload.get("size", [])
    ids = payload.get("id", [])
    values = payload.get("value", {})

    if not values:
        return pd.DataFrame()

    inv = {dim_id: {idx: code for code, idx in dims[dim_id]["category"]["index"].items()} for dim_id in ids}

    strides = [1] * len(size)
    for i in range(len(size) - 2, -1, -1):
        strides[i] = strides[i + 1] * size[i + 1]

    geo_idx = ids.index("geo")
    age_idx = ids.index("age")

    rows = []
    for k, v in values.items():
        if v is None:
            continue
        flat = int(k)
        coords = []
        for s in strides:
            coords.append(flat // s)
            flat %= s
        rows.append({
            "nuts_id": inv["geo"][coords[geo_idx]],
            "ref_year": year,
            "age_group": inv["age"][coords[age_idx]],
            "population": float(v),
        })
    return pd.DataFrame(rows)


def materialize():
    start_year = int(os.environ.get("EM_POP_START_YEAR", "2015"))
    end_year = int(os.environ.get("EM_POP_END_YEAR", "2025"))
    logger.info("Eurostat population range: %d-%d", start_year, end_year)

    pieces = []
    for year in range(start_year, end_year + 1):
        logger.info("Fetching population year %d", year)
        payload = fetch_year(year)
        df = parse_year(payload, year)
        logger.info("  year=%d rows=%d", year, len(df))
        if not df.empty:
            pieces.append(df)
        time.sleep(0.5)

    if not pieces:
        raise RuntimeError("No Eurostat population data returned")

    out = pd.concat(pieces, ignore_index=True)
    out["extracted_at"] = datetime.now(timezone.utc)
    out = out.drop_duplicates(subset=["nuts_id", "ref_year", "age_group"], keep="last")

    n_nuts = out["nuts_id"].nunique()
    logger.info("Population rows: %d (NUTS3=%d, years=%d-%d)",
                len(out), n_nuts, start_year, end_year)
    return out
