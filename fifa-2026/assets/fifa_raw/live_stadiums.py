"""@bruin

name: fifa_raw.live_stadiums
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Current stadium reference data from the free worldcup26.ir API for the FIFA
  World Cup 2026 live tracker. The endpoint includes stadium names, host city,
  country, region, and capacity.

  Source: https://worldcup26.ir/api-docs
tags:
  - fifa_2026
  - raw_data
  - live_tracker

materialization:
  type: table
  strategy: create+replace

columns:
  - name: stadium_id
    type: VARCHAR
    description: worldcup26.ir stadium identifier.
    primary_key: true
  - name: source_object_id
    type: VARCHAR
    description: Source MongoDB object identifier.
  - name: stadium_name_en
    type: VARCHAR
    description: Stadium English display name.
  - name: fifa_stadium_name
    type: VARCHAR
    description: FIFA event-time stadium display name.
  - name: city_en
    type: VARCHAR
    description: Host city English display name.
  - name: country_en
    type: VARCHAR
    description: Host country English display name.
  - name: capacity
    type: INTEGER
    description: Stadium listed seating capacity in seats.
  - name: region
    type: VARCHAR
    description: Source geographic region label.
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

API_URL = "https://worldcup26.ir/get/stadiums"
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


def fetch_stadiums() -> list[dict[str, Any]]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(API_URL, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code == 429:
                logger.warning("Rate limited by worldcup26.ir on attempt %d", attempt)
                time.sleep(min(2 ** attempt, 30))
                continue
            response.raise_for_status()
            stadiums = response.json().get("stadiums", [])
            logger.info("Fetched %d stadium rows", len(stadiums))
            return stadiums
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
    for stadium in fetch_stadiums():
        rows.append(
            {
                "stadium_id": _clean_null(stadium.get("id")),
                "source_object_id": _clean_null(stadium.get("_id")),
                "stadium_name_en": _clean_null(stadium.get("name_en")),
                "fifa_stadium_name": _clean_null(stadium.get("fifa_name")),
                "city_en": _clean_null(stadium.get("city_en")),
                "country_en": _clean_null(stadium.get("country_en")),
                "capacity": _as_int(stadium.get("capacity")),
                "region": _clean_null(stadium.get("region")),
                "extracted_at": extracted_at,
            }
        )

    df = pd.DataFrame(rows)
    logger.info("Returning %d stadium rows", len(df))
    return df
