"""@bruin

name: fifa_raw.groups
description: |
  Long-form group draw: one row per (group, fifa_code) pair, 48 rows total
  (12 groups of 4). Hosts MEX/CAN/USA are placed in groups A/B/D respectively.

  The current draw is a pot-rule-respecting placeholder pending the actual
  December-2025 draw. Patch `tournament_manifest.yml` with the real assignment
  if needed; this asset will pick up changes on the next run.

  Source: `tournament_manifest.yml`.
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
  - name: group_id
    type: VARCHAR
    description: Group letter A through L.
    primary_key: true
    checks:
      - name: not_null
      - name: accepted_values
        value: [A, B, C, D, E, F, G, H, I, J, K, L]
  - name: fifa_code
    type: VARCHAR
    description: 3-letter FIFA country code of the team in this group.
    primary_key: true
    checks:
      - name: not_null
  - name: position
    type: INT64
    description: Position within the group (1-4); used to disambiguate fixture slots like A1_v_A2.
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
    for group_id, members in manifest["groups"].items():
        for i, code in enumerate(members, start=1):
            rows.append({
                "group_id": group_id,
                "fifa_code": code,
                "position": i,
                "extracted_at": snap_ts,
            })
    df = pd.DataFrame(rows)
    logger.info("groups rows: %d (expected 48)", len(df))
    return df
