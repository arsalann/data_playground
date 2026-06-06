"""@bruin

name: fifa_raw.live_group_standings
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Snapshot of group standings from the free worldcup26.ir API. Each run
  flattens the API's group/team nested structure and appends a standings
  snapshot for point, goal, and table-position tracking.

  Source: https://worldcup26.ir/api-docs
tags:
  - fifa_2026
  - raw_data
  - live_tracker

materialization:
  type: table
  strategy: append

columns:
  - name: standing_snapshot_id
    type: VARCHAR
    description: Synthetic primary key for one group/team row in one extraction snapshot.
    primary_key: true
  - name: group_id
    type: VARCHAR
    description: Group letter A-L.
  - name: team_id
    type: VARCHAR
    description: worldcup26.ir team identifier.
  - name: matches_played
    type: INTEGER
    description: Matches played.
  - name: wins
    type: INTEGER
    description: Wins.
  - name: draws
    type: INTEGER
    description: Draws.
  - name: losses
    type: INTEGER
    description: Losses.
  - name: points
    type: INTEGER
    description: Group-stage points.
  - name: goals_for
    type: INTEGER
    description: Goals scored.
  - name: goals_against
    type: INTEGER
    description: Goals conceded.
  - name: goal_difference
    type: INTEGER
    description: Goals for minus goals against.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when the API snapshot was extracted.

@bruin"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

API_URL = "https://worldcup26.ir/get/groups"
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 5


def _clean_null(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and value.strip().lower() in {"", "null", "none", "nan"}:
        return None
    return value


def _as_int(value: Any) -> int | None:
    value = _clean_null(value)
    if value is None:
        return None
    return int(value)


def fetch_groups() -> list[dict[str, Any]]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(API_URL, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code == 429:
                logger.warning("Rate limited by worldcup26.ir on attempt %d", attempt)
                time.sleep(min(2 ** attempt, 30))
                continue
            response.raise_for_status()
            groups = response.json().get("groups", [])
            logger.info("Fetched %d group rows", len(groups))
            return groups
        except requests.RequestException as exc:
            logger.warning("Attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            time.sleep(min(2 ** attempt, 30))

    logger.error("All attempts failed for %s; returning an empty snapshot", API_URL)
    return []


def materialize():
    logger.info(
        "Bruin interval: %s to %s; source endpoint is current-state only",
        os.environ.get("BRUIN_START_DATE"),
        os.environ.get("BRUIN_END_DATE"),
    )
    extracted_at = datetime.now(timezone.utc)
    rows = []
    for group in fetch_groups():
        group_id = _clean_null(group.get("name"))
        for team in group.get("teams", []):
            team_id = _clean_null(team.get("team_id"))
            rows.append(
                {
                    "standing_snapshot_id": f"{group_id}|{team_id}|{extracted_at.isoformat()}",
                    "group_id": group_id,
                    "team_id": team_id,
                    "matches_played": _as_int(team.get("mp")),
                    "wins": _as_int(team.get("w")),
                    "draws": _as_int(team.get("d")),
                    "losses": _as_int(team.get("l")),
                    "points": _as_int(team.get("pts")),
                    "goals_for": _as_int(team.get("gf")),
                    "goals_against": _as_int(team.get("ga")),
                    "goal_difference": _as_int(team.get("gd")),
                    "extracted_at": extracted_at,
                }
            )

    df = pd.DataFrame(rows)
    logger.info("Returning %d group-standing rows", len(df))
    return df
