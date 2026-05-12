"""@bruin

name: eu_mortality_raw.eurostat_weekly_deaths
description: |
  Weekly total deaths by NUTS3 region for the EU-27, 2015-2025. Contains ~750K rows covering 1,506 regions across all EU member states.

  Source: Eurostat DEMO_R_MWK3_T -- "Deaths by week and NUTS 3 region" (total, both
  sexes, all ages combined). The age- and sex-disaggregated companion DEMO_R_MWK3_05
  is much larger; this pipeline uses totals for the excess-mortality model and
  multiplies by population age structure to attribute vulnerability downstream.

  Primary use cases:
    - Baseline calculation for excess mortality detection
    - Heat attribution analysis when joined with temperature data
    - Time series analysis of mortality patterns across EU regions

  Fetched via the Eurostat REST statistics API (version 1.0). One request per year
  to stay well below the ~50k-cell response limit.

  API:
    https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/DEMO_R_MWK3_T

  Coverage notes:
    - Weekly granularity (ISO 8601 week, Monday start).
    - Series begin around 2000 for some countries but the harmonised NUTS3 panel is
      practical from 2015 onward; the excess-mortality baseline uses 2015-2019.
    - 2026 data is published quarterly with a lag of 1-2 quarters.
    - Some observations carry a status flag (p=provisional, e=estimated).

  Operational characteristics:
    - Refreshed weekly via pipeline schedule
    - Full replace materialization strategy (~10 minute runtime)
    - No incremental updates - complete historical rebuild each run
connection: bruin-playground-arsalan
tags:
  - eu-27
  - mortality
  - raw
  - eurostat
  - weekly
  - excess_mortality
  - demographic_data
  - time_series
  - append_only

materialization:
  type: table
  strategy: create+replace
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: nuts_id
    type: VARCHAR
    description: NUTS3 region code (geo dimension value). Covers 1,506 distinct regions across EU-27 member states. Format varies by country (e.g., FR101, DE111, IT102) with 2-9 character length.
    primary_key: true
    checks:
      - name: not_null
  - name: iso_year
    type: INTEGER
    description: ISO 8601 calendar year of the reporting week. Range spans 2015-2025 with focus on 2015-2019 baseline period for excess mortality calculations.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - 2015
          - 2016
          - 2017
          - 2018
          - 2019
          - 2020
          - 2021
          - 2022
          - 2023
          - 2024
          - 2025
  - name: iso_week
    type: INTEGER
    description: ISO 8601 week number (1-53). Week starts on Monday following ISO standard.
    checks:
      - name: not_null
  - name: time_period
    type: VARCHAR
    description: Eurostat-formatted period code following pattern "YYYY-WNN" (e.g., "2024-W30"). Fixed 8-character length. Used for time series identification and joins.
    primary_key: true
    checks:
      - name: not_null
  - name: deaths_total
    type: DOUBLE
    description: Total deaths in the NUTS3 region during the reporting week (all ages, both sexes). Count ranges from 0 to ~127,500 with most regions reporting <1,000 weekly deaths. Used for excess mortality analysis and heat attribution modeling.
    checks:
      - name: not_null
      - name: positive
  - name: obs_status
    type: VARCHAR
    description: Eurostat observation-status flag indicating data quality. Values include "p" (provisional) for recent quarters with 1-2 quarter lag, "e" (estimated), or null for final confirmed data. Most observations (~69%) have final status (null).
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp of ingestion from Eurostat API. Single extraction timestamp per pipeline run indicates create+replace materialization strategy.
    checks:
      - name: not_null

@bruin"""

import logging
import os
import re
import time
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

EUROSTAT_API = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/DEMO_R_MWK3_T"
WEEK_RE = re.compile(r"^(\d{4})-W(\d{1,2})$")


def fetch_year(year: int, retries: int = 5) -> dict:
    weeks = [f"{year}-W{w:02d}" for w in range(1, 54)]
    params = [("format", "JSON"), ("lang", "EN")] + [("time", w) for w in weeks]

    for attempt in range(retries):
        try:
            r = requests.get(EUROSTAT_API, params=params, timeout=180)
            if r.status_code in (429, 502, 503):
                wait = 15 * (attempt + 1)
                logger.warning("HTTP %d, retrying in %ds", r.status_code, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            wait = 15 * (attempt + 1)
            logger.warning("Network error attempt %d: %s", attempt + 1, e)
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch year {year} after {retries} attempts")


def parse_year(payload: dict, year: int) -> pd.DataFrame:
    dims = payload.get("dimension", {})
    size = payload.get("size", [])
    ids = payload.get("id", [])
    values = payload.get("value", {})
    statuses = payload.get("status", {}) or {}

    if not values:
        logger.info("  year=%d: empty response", year)
        return pd.DataFrame()

    cat = {dim_id: dims[dim_id]["category"]["index"] for dim_id in ids}
    inv = {dim_id: {idx: code for code, idx in cat[dim_id].items()} for dim_id in ids}

    strides = [1] * len(size)
    for i in range(len(size) - 2, -1, -1):
        strides[i] = strides[i + 1] * size[i + 1]

    rows = []
    for k, v in values.items():
        if v is None:
            continue
        flat = int(k)
        coords = []
        for s in strides:
            coords.append(flat // s)
            flat %= s
        try:
            geo_idx = ids.index("geo")
            time_idx = ids.index("time")
        except ValueError:
            continue
        nuts_id = inv["geo"][coords[geo_idx]]
        time_period = inv["time"][coords[time_idx]]
        m = WEEK_RE.match(time_period)
        if not m:
            continue
        iso_year = int(m.group(1))
        iso_week = int(m.group(2))
        rows.append({
            "nuts_id": nuts_id,
            "iso_year": iso_year,
            "iso_week": iso_week,
            "time_period": time_period,
            "deaths_total": float(v),
            "obs_status": statuses.get(k),
        })

    return pd.DataFrame(rows)


def materialize():
    start_year = int(os.environ.get("EM_START_YEAR", "2015"))
    end_year = int(os.environ.get("EM_END_YEAR", "2025"))
    logger.info("Eurostat weekly mortality range: %d-%d", start_year, end_year)

    pieces = []
    for year in range(start_year, end_year + 1):
        logger.info("Fetching year %d", year)
        payload = fetch_year(year)
        df = parse_year(payload, year)
        logger.info("  year=%d rows=%d", year, len(df))
        if not df.empty:
            pieces.append(df)
        time.sleep(0.5)

    if not pieces:
        raise RuntimeError("No data returned from Eurostat -- check connectivity / API URL")

    out = pd.concat(pieces, ignore_index=True)
    out["extracted_at"] = datetime.now(timezone.utc)
    out = out.drop_duplicates(subset=["nuts_id", "time_period"], keep="last")
    out = out.sort_values(["nuts_id", "iso_year", "iso_week"]).reset_index(drop=True)

    logger.info("Total weekly mortality rows: %d, NUTS3=%d, years=%s-%s",
                len(out), out["nuts_id"].nunique(), out["iso_year"].min(), out["iso_year"].max())
    return out
