"""@bruin

name: fifa_raw.city_demographics
description: |
  Metro-area population and approximate GDP per capita for each of the 16 host
  cities. Used by H5 as an input to the demand-score heuristic.

  Source: `tournament_manifest.yml.city_demographics_seed`, in turn seeded from
  Wikipedia metro-area population articles (CC BY-SA) and IMF / World Bank /
  OECD per-capita GDP aggregates. Numbers are rounded; precision is not required
  for a relative ranking.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - reference
  - raw_data
  - manifest_backed

materialization:
  type: table
  strategy: create+replace
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: city
    type: VARCHAR
    description: Host city name (matches host_venues.city).
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: country
    type: VARCHAR
    checks:
      - name: not_null
  - name: metro_pop_m
    type: DOUBLE
    description: Metropolitan-area population in millions.
    checks:
      - name: not_null
  - name: gdp_per_capita_usd
    type: DOUBLE
    description: Approximate GDP per capita in USD.
  - name: extracted_at
    type: TIMESTAMP
    checks:
      - name: not_null

@bruin"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

MANIFEST_PATH = Path(__file__).parent / "tournament_manifest.yml"


def materialize():
    with open(MANIFEST_PATH, "r") as f:
        manifest = yaml.safe_load(f)
    snap_ts = datetime.now(timezone.utc)
    rows = [
        {
            "city": r["city"],
            "country": r["country"],
            "metro_pop_m": float(r["metro_pop_m"]),
            "gdp_per_capita_usd": float(r["gdp_per_capita_usd"]),
            "extracted_at": snap_ts,
        }
        for r in manifest["city_demographics_seed"]
    ]
    df = pd.DataFrame(rows)
    logger.info("city_demographics rows: %d", len(df))
    return df
