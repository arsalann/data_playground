"""@bruin

name: fifa_raw.fifa_schedule
description: |
  104-match schedule for the 2026 FIFA World Cup: 72 group-stage fixtures
  (12 groups x 6 each) plus 32 knockout fixtures (16 R32, 8 R16, 4 QF, 2 SF,
  1 third-place, 1 final).

  Each row carries `match_id`, `stage`, `group_id` (NULL for KO), `slot`
  (e.g., A1_v_A2 in group stage; W73_v_W74 in KO), `venue_id`, `kickoff_local`,
  and `kickoff_local_tz`. UTC kickoff is computed downstream in
  `matches_enriched` after joining to `host_venues.timezone`.

  Knockout-stage participants are TBD bracket slots — they're resolved into
  actual teams once group results settle. For H1/H2 analysis we only need the
  venue + kickoff hour, both of which are fixed.

  Source: `tournament_manifest.yml` (seeded from FIFA's published match
  calendar). Group-stage matchday 3 fixtures kick off simultaneously per FIFA
  convention.
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
  - name: match_id
    type: VARCHAR
    description: Internal match identifier (M001..M104).
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: stage
    type: VARCHAR
    description: Tournament stage (G group / R32 / R16 / QF / SF / 3P / F).
    checks:
      - name: not_null
      - name: accepted_values
        value: [G, R32, R16, QF, SF, 3P, F]
  - name: group_id
    type: VARCHAR
    description: Group letter A-L for group-stage matches; NULL for knockouts.
  - name: slot
    type: VARCHAR
    description: Fixture slot label (e.g., A1_v_A2 for groups, W73_v_W74 for KO).
    checks:
      - name: not_null
  - name: venue_id
    type: VARCHAR
    description: Foreign key to host_venues.venue_id.
    checks:
      - name: not_null
  - name: kickoff_local
    type: TIMESTAMP
    description: Scheduled kickoff in venue local time (no tz).
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
    for m in manifest["group_stage_matches"]:
        rows.append({
            "match_id": m["match_id"],
            "stage": "G",
            "group_id": m["group"],
            "slot": m["slot"],
            "venue_id": m["venue_id"],
            "kickoff_local": pd.to_datetime(m["kickoff_local"]),
            "extracted_at": snap_ts,
        })
    for m in manifest["knockout_matches"]:
        rows.append({
            "match_id": m["match_id"],
            "stage": m["stage"],
            "group_id": None,
            "slot": m["slot"],
            "venue_id": m["venue_id"],
            "kickoff_local": pd.to_datetime(m["kickoff_local"]),
            "extracted_at": snap_ts,
        })
    df = pd.DataFrame(rows)
    logger.info("fifa_schedule rows: %d (expected 104)", len(df))
    logger.info("Stage breakdown: %s", df["stage"].value_counts().to_dict())
    return df
