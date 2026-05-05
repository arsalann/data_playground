"""@bruin

name: fifa_raw.historical_wc_matches
description: |
  Historical FIFA World Cup match results from the 2010, 2014, 2018, and 2022
  tournaments, with venue elevation in metres (the H4 altitude-effect panel
  joins on this column). One row per match, sampled across high-altitude and
  sea-level venues to support the goals-by-altitude-band analysis.

  Source: `tournament_manifest.yml.historical_wc_matches_seed`, which is in turn
  seeded from Wikipedia per-tournament results pages (CC BY-SA). The seed is
  representative, not exhaustive — sufficient to demonstrate altitude-band
  comparison but not to support tournament-level inference.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - historical
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
  - name: tournament
    type: INT64
    description: Tournament year (2010, 2014, 2018, 2022).
    primary_key: true
    checks:
      - name: not_null
  - name: host
    type: VARCHAR
    description: ISO-3 host country code.
    checks:
      - name: not_null
  - name: venue_city
    type: VARCHAR
    description: Host city.
    primary_key: true
    checks:
      - name: not_null
  - name: home
    type: VARCHAR
    description: Home (or first-listed) team FIFA code.
    primary_key: true
    checks:
      - name: not_null
  - name: away
    type: VARCHAR
    description: Away (or second-listed) team FIFA code.
    primary_key: true
    checks:
      - name: not_null
  - name: stage
    type: VARCHAR
    description: G (group) / R16 / QF / SF / 3P / F.
    checks:
      - name: not_null
  - name: elevation_m
    type: DOUBLE
    description: Approximate venue elevation in metres above sea level.
    checks:
      - name: not_null
  - name: home_goals
    type: INT64
    checks:
      - name: not_null
  - name: away_goals
    type: INT64
    checks:
      - name: not_null
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
    rows = []
    for m in manifest["historical_wc_matches_seed"]:
        rows.append({
            "tournament": int(m["tournament"]),
            "host": m["host"],
            "venue_city": m["venue_city"],
            "home": m["home"],
            "away": m["away"],
            "stage": m["stage"],
            "elevation_m": float(m["elevation_m"]),
            "home_goals": int(m["home_goals"]),
            "away_goals": int(m["away_goals"]),
            "extracted_at": snap_ts,
        })
    df = pd.DataFrame(rows)
    logger.info("historical_wc_matches rows: %d", len(df))
    return df
