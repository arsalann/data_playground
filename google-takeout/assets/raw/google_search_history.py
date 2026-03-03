"""@bruin
name: raw.google_search_history
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Parses Google Takeout search history HTML and extracts searches.
  Source: local file `data/search-history.html` (override with
  GOOGLE_TAKEOUT_SEARCH_HISTORY_PATH).
  Includes only entries with "Searched for".

materialization:
  type: table
  strategy: create+replace

columns:
  - name: search_timestamp
    type: TIMESTAMP
    description: Timestamp of the search in UTC
    primary_key: true
  - name: search_phrase
    type: VARCHAR
    description: Search query text
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the file was parsed

@bruin"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

TIMESTAMP_FORMATS = (
    "%b %d, %Y, %I:%M:%S %p GMT%z",
    "%b %d, %Y, %H:%M:%S GMT%z",
)


def normalize_space(value: str) -> str:
    """Normalize whitespace and non-breaking spaces to regular spaces."""
    return " ".join(value.replace("\u202f", " ").replace("\xa0", " ").split())


def parse_timestamp(value: str) -> datetime:
    """Parse a Google Takeout timestamp string into a datetime."""
    cleaned = normalize_space(value)
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized timestamp format: {value}")


def get_source_path() -> Path:
    """Resolve the HTML file path from env or default location."""
    env_path = os.environ.get("GOOGLE_TAKEOUT_SEARCH_HISTORY_PATH")
    if env_path:
        return Path(env_path).expanduser()
    return Path(__file__).resolve().parents[3] / "data" / "search-history.html"


def extract_search_entries(html: str) -> list[dict[str, object]]:
    """Extract search phrases and timestamps from the raw HTML."""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, object]] = []
    for cell in soup.select("div.content-cell"):
        strings = [s.strip() for s in cell.stripped_strings if s.strip()]
        if not strings:
            continue
        if strings[0] != "Searched for":
            continue
        link = cell.find("a")
        if not link:
            continue
        phrase = link.get_text(strip=True)
        if len(strings) < 2:
            continue
        timestamp_text = strings[-1]
        if timestamp_text == phrase:
            continue
        try:
            search_timestamp = parse_timestamp(timestamp_text)
        except ValueError as exc:
            logger.warning("Skipping entry with bad timestamp: %s", exc)
            continue
        rows.append(
            {
                "search_timestamp": search_timestamp,
                "search_phrase": phrase,
            }
        )
    return rows


def apply_date_filter(
    df: pd.DataFrame, start_date: str | None, end_date: str | None
) -> pd.DataFrame:
    """Filter dataframe rows to the inclusive start/end dates if provided."""
    before = len(df)
    if start_date:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        df = df[df["search_timestamp"].dt.date >= start]
    if end_date:
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        df = df[df["search_timestamp"].dt.date <= end]
    after = len(df)
    if start_date or end_date:
        logger.info(
            "Applied date filter start=%s end=%s (%s -> %s rows)",
            start_date,
            end_date,
            before,
            after,
        )
    return df


def materialize():
    """Load, parse, filter, and return a dataframe for BigQuery materialization."""
    html_path = get_source_path()
    if not html_path.exists():
        raise FileNotFoundError(f"Search history file not found: {html_path}")

    logger.info("Reading search history from %s", html_path)
    html = html_path.read_text(encoding="utf-8")
    records = extract_search_entries(html)
    if not records:
        raise RuntimeError("No 'Searched for' entries found in search history")

    logger.info("Parsed %s search entries", len(records))
    df = pd.DataFrame(records)
    df["search_timestamp"] = pd.to_datetime(df["search_timestamp"], utc=True)
    df = apply_date_filter(
        df, os.environ.get("BRUIN_START_DATE"), os.environ.get("BRUIN_END_DATE")
    )
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Materializing %s rows", len(df))
    return df
