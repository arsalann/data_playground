"""@bruin

name: fifa_raw.live_teams
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Current team reference data from the free worldcup26.ir API for the FIFA
  World Cup 2026 live tracker. The endpoint includes team names, FIFA codes,
  flag URLs, and group assignments.

  Source: https://worldcup26.ir/api-docs
tags:
  - fifa_2026
  - raw_data
  - live_tracker

materialization:
  type: table
  strategy: create+replace

columns:
  - name: team_id
    type: VARCHAR
    description: worldcup26.ir team identifier.
    primary_key: true
  - name: source_object_id
    type: VARCHAR
    description: Source MongoDB object identifier.
  - name: team_name_en
    type: VARCHAR
    description: Team English display name.
  - name: team_name_fa
    type: VARCHAR
    description: Team Persian display name from the source.
  - name: flag_url
    type: VARCHAR
    description: Source flag image URL.
  - name: fifa_code
    type: VARCHAR
    description: FIFA three-letter team code.
  - name: iso2
    type: VARCHAR
    description: Source country/territory code; some football associations use non-ISO values.
  - name: group_id
    type: VARCHAR
    description: Group letter A-L.
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

API_URL = "https://worldcup26.ir/get/teams"
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 5


def _clean_null(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and value.strip().lower() in {"", "null", "none", "nan"}:
        return None
    return value


def fetch_teams() -> list[dict[str, Any]]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(API_URL, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code == 429:
                logger.warning("Rate limited by worldcup26.ir on attempt %d", attempt)
                time.sleep(min(2 ** attempt, 30))
                continue
            response.raise_for_status()
            teams = response.json().get("teams", [])
            logger.info("Fetched %d team rows", len(teams))
            return teams
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
    for team in fetch_teams():
        rows.append(
            {
                "team_id": _clean_null(team.get("id")),
                "source_object_id": _clean_null(team.get("_id")),
                "team_name_en": _clean_null(team.get("name_en")),
                "team_name_fa": _clean_null(team.get("name_fa")),
                "flag_url": _clean_null(team.get("flag")),
                "fifa_code": _clean_null(team.get("fifa_code")),
                "iso2": _clean_null(team.get("iso2")),
                "group_id": _clean_null(team.get("groups")),
                "extracted_at": extracted_at,
            }
        )

    df = pd.DataFrame(rows)
    logger.info("Returning %d team rows", len(df))
    return df
