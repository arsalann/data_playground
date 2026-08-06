"""@bruin
name: chess_peterthiel.raw_chess_titled_players
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Retrieves the current public Chess.com titled-player registries (GM, IM, FM,
  NM, WGM, WIM, WFM, and WNM). This small reference table makes it possible to
  identify archive opponents who are currently listed with an official title
  by Chess.com without inferring a title from a username or rating.

  Source: https://www.chess.com/news/view/published-data-api
  License and terms: https://www.chess.com/legal/api

materialization:
  type: table
  strategy: create+replace

columns:
  - name: title
    type: VARCHAR
    description: Chess.com current public title-registry code, such as GM or IM.
    primary_key: true
    nullable: false
  - name: username
    type: VARCHAR
    description: Public Chess.com username returned by the title registry.
    primary_key: true
    nullable: false
  - name: source_url
    type: VARCHAR
    description: Public Chess.com API endpoint from which the title record was retrieved.
    nullable: false
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp at which the public title registry was fetched.
    nullable: false

@bruin"""

import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

API_BASE = "https://api.chess.com/pub/titled"
TITLE_CODES = ("GM", "IM", "FM", "NM", "WGM", "WIM", "WFM", "WNM")
MAX_RETRIES = 5


def fetch_title_registry(session: requests.Session, title: str) -> list[dict[str, str]]:
    """Fetch one public Chess.com title registry with bounded retries."""
    source_url = f"{API_BASE}/{title}"
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(source_url, timeout=30)
            if response.status_code == 200:
                players = response.json().get("players", [])
                if not isinstance(players, list):
                    logger.warning("Unexpected player list type for %s", title)
                    return []
                logger.info("Fetched %d current %s title records", len(players), title)
                return [
                    {"title": title, "username": str(username), "source_url": source_url}
                    for username in players
                    if username
                ]
            if response.status_code in (429, 500, 502, 503, 504):
                logger.warning(
                    "Transient response %s for %s on attempt %d/%d",
                    response.status_code,
                    title,
                    attempt + 1,
                    MAX_RETRIES,
                )
            else:
                response.raise_for_status()
        except requests.RequestException as error:
            logger.warning(
                "Request failed for %s on attempt %d/%d: %s",
                title,
                attempt + 1,
                MAX_RETRIES,
                error,
            )

        if attempt < MAX_RETRIES - 1:
            time.sleep(0.5 * (2**attempt))

    logger.warning("Title registry %s could not be fetched after retries", title)
    return []


def fetch_data() -> pd.DataFrame:
    """Fetch every title class from the small public registry API."""
    records: list[dict[str, str]] = []
    with requests.Session() as session:
        session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "bruin-peterthiel-chess-analysis/1.0",
            }
        )
        for index, title in enumerate(TITLE_CODES):
            records.extend(fetch_title_registry(session, title))
            if index < len(TITLE_CODES) - 1:
                time.sleep(0.5)

    return pd.DataFrame(records, columns=["title", "username", "source_url"])


def materialize() -> pd.DataFrame:
    start_date = os.environ.get("BRUIN_START_DATE", "<default>")
    end_date = os.environ.get("BRUIN_END_DATE", "<default>")
    logger.info("Interval: %s to %s (title registries are current snapshots)", start_date, end_date)

    dataframe = fetch_data()
    dataframe["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Materializing %d public title records", len(dataframe))
    return dataframe
