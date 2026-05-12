"""@bruin

name: fifa_raw.qualified_teams
description: |
  48 qualified national teams for the 2026 FIFA World Cup, with FIFA code, full
  name, confederation, draw pot, and capital-city home coordinates (used by H2 to
  compute travel-distance segments).

  The home_lat / home_lon pair is the team's capital-city centroid, not the
  national-federation training base. This is a deliberate simplification flagged
  in the H2 dashboard footnote.

  Source: `tournament_manifest.yml` (seeded from FIFA confederation qualifying
  summaries, April 2026).
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
  - name: fifa_code
    type: VARCHAR
    description: 3-letter FIFA country code (e.g., USA, MEX, ARG).
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: name
    type: VARCHAR
    description: Full country name.
    checks:
      - name: not_null
  - name: confederation
    type: VARCHAR
    description: One of CONCACAF / CONMEBOL / UEFA / AFC / CAF / OFC.
    checks:
      - name: not_null
      - name: accepted_values
        value: [CONCACAF, CONMEBOL, UEFA, AFC, CAF, OFC]
  - name: pot
    type: INT64
    description: Draw pot (1-4); pot 1 contains hosts plus highest-ranked teams.
  - name: home_lat
    type: DOUBLE
    description: Capital-city latitude — proxy for team home location used in H2 travel calculations.
    checks:
      - name: not_null
  - name: home_lon
    type: DOUBLE
    description: Capital-city longitude — proxy for team home location used in H2 travel calculations.
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
    for t in manifest["teams"]:
        rows.append({
            "fifa_code": t["fifa_code"],
            "name": t["name"],
            "confederation": t["confederation"],
            "pot": int(t.get("pot")) if t.get("pot") is not None else None,
            "home_lat": float(t["home_lat"]),
            "home_lon": float(t["home_lon"]),
            "extracted_at": snap_ts,
        })
    df = pd.DataFrame(rows)
    logger.info("qualified_teams rows: %d, confederations: %s",
                len(df), df["confederation"].value_counts().to_dict())
    return df
