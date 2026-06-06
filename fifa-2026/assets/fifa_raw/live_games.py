"""@bruin

name: fifa_raw.live_games
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Snapshot of FIFA World Cup 2026 matches from the free worldcup26.ir API.
  The endpoint includes fixture metadata, current score, scorer strings, match
  status, and elapsed-time/status text. This raw asset appends every run so
  the warehouse can retain score/status changes over time.

  Source: https://worldcup26.ir/api-docs
tags:
  - fifa_2026
  - raw_data
  - live_tracker

materialization:
  type: table
  strategy: append

columns:
  - name: match_snapshot_id
    type: VARCHAR
    description: Synthetic primary key for one match row in one extraction snapshot.
    primary_key: true
  - name: api_match_id
    type: VARCHAR
    description: worldcup26.ir match identifier.
  - name: source_object_id
    type: VARCHAR
    description: Source MongoDB object identifier.
  - name: home_team_id
    type: VARCHAR
    description: worldcup26.ir home-team identifier.
  - name: away_team_id
    type: VARCHAR
    description: worldcup26.ir away-team identifier.
  - name: home_score
    type: INTEGER
    description: Current home-team score in goals at extraction time.
  - name: away_score
    type: INTEGER
    description: Current away-team score in goals at extraction time.
  - name: home_scorers
    type: VARCHAR
    description: Source-provided home scorer string; may be null-like text before or during matches.
  - name: away_scorers
    type: VARCHAR
    description: Source-provided away scorer string; may be null-like text before or during matches.
  - name: group_id
    type: VARCHAR
    description: Group letter for group-stage fixtures; null for knockout fixtures.
  - name: matchday
    type: INTEGER
    description: Source matchday number within the tournament phase.
  - name: local_date
    type: VARCHAR
    description: "Source kickoff timestamp string in local venue time, formatted MM/DD/YYYY HH:MM."
  - name: persian_date
    type: VARCHAR
    description: Source kickoff timestamp string in the Persian calendar.
  - name: stadium_id
    type: VARCHAR
    description: worldcup26.ir stadium identifier.
  - name: finished
    type: BOOLEAN
    description: Whether the source marks the fixture as finished at extraction time.
  - name: time_elapsed
    type: VARCHAR
    description: Source status/elapsed-time text, e.g. notstarted, live, halftime, fulltime.
  - name: match_type
    type: VARCHAR
    description: Source match type, e.g. group, round16, quarter, semi, final.
  - name: home_team_name_en
    type: VARCHAR
    description: Home team English display name.
  - name: away_team_name_en
    type: VARCHAR
    description: Away team English display name.
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

API_URL = "https://worldcup26.ir/get/games"
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


def _as_bool(value: Any) -> bool | None:
    value = _clean_null(value)
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().upper() == "TRUE"


def fetch_games() -> list[dict[str, Any]]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(API_URL, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code == 429:
                logger.warning("Rate limited by worldcup26.ir on attempt %d", attempt)
                time.sleep(min(2 ** attempt, 30))
                continue
            response.raise_for_status()
            payload = response.json()
            games = payload.get("games", [])
            logger.info("Fetched %d live game rows", len(games))
            return games
        except requests.RequestException as exc:
            logger.warning("Attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            time.sleep(min(2 ** attempt, 30))

    logger.error("All attempts failed for %s; returning an empty snapshot", API_URL)
    return []


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE")
    end_date = os.environ.get("BRUIN_END_DATE")
    logger.info("Bruin interval: %s to %s; source endpoint is current-state only", start_date, end_date)

    extracted_at = datetime.now(timezone.utc)
    rows = []
    for game in fetch_games():
        api_match_id = str(game.get("id"))
        rows.append(
            {
                "match_snapshot_id": f"{api_match_id}|{extracted_at.isoformat()}",
                "api_match_id": api_match_id,
                "source_object_id": _clean_null(game.get("_id")),
                "home_team_id": _clean_null(game.get("home_team_id")),
                "away_team_id": _clean_null(game.get("away_team_id")),
                "home_score": _as_int(game.get("home_score")),
                "away_score": _as_int(game.get("away_score")),
                "home_scorers": _clean_null(game.get("home_scorers")) or "",
                "away_scorers": _clean_null(game.get("away_scorers")) or "",
                "group_id": _clean_null(game.get("group")),
                "matchday": _as_int(game.get("matchday")),
                "local_date": _clean_null(game.get("local_date")),
                "persian_date": _clean_null(game.get("persian_date")),
                "stadium_id": _clean_null(game.get("stadium_id")),
                "finished": _as_bool(game.get("finished")),
                "time_elapsed": _clean_null(game.get("time_elapsed")),
                "match_type": _clean_null(game.get("type")),
                "home_team_name_en": _clean_null(game.get("home_team_name_en")),
                "away_team_name_en": _clean_null(game.get("away_team_name_en")),
                "extracted_at": extracted_at,
            }
        )

    df = pd.DataFrame(rows)
    logger.info("Returning %d match snapshot rows", len(df))
    return df
