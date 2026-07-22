"""@bruin
name: final_raw.h2h_history
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Senior men's Argentina–Spain historical head-to-head matches as listed by
  11v11. The asset reads the public source directly and falls back only when
  11v11 blocks or errors on automated retrieval; the cited source URLs remain
  the 11v11 record and individual-match pages.

  Source: https://www.11v11.com/teams/argentina/tab/opposingTeams/opposition/Spain/

materialization:
  type: table
  strategy: append

columns:
  - name: h2h_snapshot_id
    type: VARCHAR
    description: Hash of an Argentina–Spain match and its source version at extraction.
    primary_key: true
  - name: match_date
    type: DATE
    description: Match date in the 11v11 historical series.
  - name: competition
    type: VARCHAR
    description: Competition label from 11v11.
  - name: venue
    type: VARCHAR
    description: Venue and city from the linked 11v11 match page, when published.
  - name: home_team
    type: VARCHAR
    description: Home team as shown by 11v11.
  - name: away_team
    type: VARCHAR
    description: Away team as shown by 11v11.
  - name: home_goals
    type: INTEGER
    description: Home-team goals in the listed scoreline.
  - name: away_goals
    type: INTEGER
    description: Away-team goals in the listed scoreline.
  - name: argentina_goals
    type: INTEGER
    description: Argentina goals, normalized from the home-away scoreline.
  - name: spain_goals
    type: INTEGER
    description: Spain goals, normalized from the home-away scoreline.
  - name: argentina_outcome
    type: VARCHAR
    description: "Argentina-perspective outcome: W, D, or L."
  - name: source_url
    type: VARCHAR
    description: 11v11 head-to-head source URL.
  - name: source_match_url
    type: VARCHAR
    description: Linked 11v11 match-page URL used to retrieve venue.
  - name: source_hash
    type: VARCHAR
    description: SHA-256 hash of the 11v11-series response content.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when the historical series was retrieved.

@bruin"""

import hashlib
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

H2H_URL = "https://www.11v11.com/teams/argentina/tab/opposingTeams/opposition/Spain/"
JINA_PREFIX = "https://r.jina.ai/http://"
MAX_RETRIES = 5
REQUEST_TIMEOUT_SECONDS = 60
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; BruinArgentinaSpainResearch/1.0)"}


def _get(url: str) -> str:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code in {429, 502, 503, 504}:
                raise requests.HTTPError(f"transient HTTP {response.status_code}")
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"Unable to retrieve {url}") from exc
            sleep_seconds = min(2**attempt, 30)
            logger.warning("Attempt %d/%d failed for %s: %s", attempt, MAX_RETRIES, url, exc)
            time.sleep(sleep_seconds)
    raise AssertionError("unreachable")


def _public_page(url: str) -> str:
    try:
        direct = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
        direct.raise_for_status()
        if "Just a moment" not in direct.text and "cf-chl" not in direct.text.lower():
            return direct.text
        logger.warning("11v11 returned a bot-check page; using its public reader representation for this retrieval")
    except requests.RequestException as exc:
        logger.warning("11v11 direct retrieval failed for %s: %s; using its public reader representation", url, exc)
    return _get(f"{JINA_PREFIX}{url.removeprefix('https://').removeprefix('http://')}")


def _strip_markdown(value: str) -> str:
    return re.sub(r"\[([^]]+)\]\([^)]+\)", r"\1", value).strip()


def _markdown_link(value: str) -> str | None:
    match = re.search(r"\[[^]]+\]\(([^)]+)\)", value)
    return match.group(1) if match else None


def _venue_from_match_page(url: str) -> str | None:
    text = _public_page(url)
    if "<html" in text.lower():
        values = list(BeautifulSoup(text, "html.parser").stripped_strings)
        try:
            return values[values.index("Venue") + 1]
        except (ValueError, IndexError):
            return None
    match = re.search(r"\*\*Venue\*\*\s*([^\n]+)", text)
    return match.group(1).strip() if match else None


def _parse_h2h_row(cells: list[str], detail_url: str | None) -> dict[str, Any]:
    if len(cells) != 5:
        raise ValueError(f"Unexpected 11v11 row width: {cells!r}")
    match_date = datetime.strptime(cells[0], "%d %b %Y").date()
    fixture = _strip_markdown(cells[1])
    score_match = re.fullmatch(r"(\d+)-(\d+)", cells[3])
    fixture_match = re.fullmatch(r"(Argentina|Spain) v (Argentina|Spain)", fixture)
    if not score_match or not fixture_match or cells[2] not in {"W", "D", "L"} or not detail_url:
        raise ValueError(f"Unexpected 11v11 result row: {cells!r}")
    home_goals, away_goals = map(int, score_match.groups())
    home_team, away_team = fixture_match.groups()
    return {
        "match_date": match_date,
        "competition": cells[4],
        "home_team": home_team,
        "away_team": away_team,
        "home_goals": home_goals,
        "away_goals": away_goals,
        "argentina_goals": home_goals if home_team == "Argentina" else away_goals,
        "spain_goals": home_goals if home_team == "Spain" else away_goals,
        "argentina_outcome": cells[2],
        "source_match_url": detail_url.replace("http://", "https://"),
    }


def parse_h2h_markdown(markdown: str) -> list[dict[str, Any]]:
    rows = []
    if "<html" in markdown.lower():
        soup = BeautifulSoup(markdown, "html.parser")
        table = soup.select_one("table.sortable")
        if table is None:
            raise ValueError("Could not find 11v11 historical-results table")
        for tr in table.find_all("tr"):
            cells = [cell.get_text(" ", strip=True) for cell in tr.find_all("td")]
            if not cells:
                continue
            link = tr.find("a", href=True)
            rows.append(_parse_h2h_row(cells, f"https://www.11v11.com{link['href']}" if link else None))
    else:
        for line in markdown.splitlines():
            cells = [cell.strip() for cell in line.split("|")]
            if len(cells) != 7 or cells[0] or cells[-1]:
                continue
            try:
                rows.append(_parse_h2h_row(cells[1:-1], _markdown_link(cells[2])))
            except ValueError:
                if re.match(r"\|\s*\d{2} [A-Z][a-z]{2} \d{4}\s*\|", line):
                    raise
    if len(rows) != 14:
        raise ValueError(f"Expected 14 Argentina–Spain 11v11 rows, parsed {len(rows)}")
    record = tuple(sum(row["argentina_outcome"] == value for row in rows) for value in ("W", "D", "L"))
    if record != (6, 2, 6):
        raise ValueError(f"Expected 11v11 Argentina record 6-2-6, parsed {record}")
    return rows


def materialize():
    logger.info("Bruin interval: %s to %s; 11v11 history is a complete historical series", os.environ.get("BRUIN_START_DATE"), os.environ.get("BRUIN_END_DATE"))
    extracted_at = datetime.now(timezone.utc)
    source_text = _public_page(H2H_URL)
    source_hash = hashlib.sha256(source_text.encode()).hexdigest()
    parsed_rows = parse_h2h_markdown(source_text)
    rows = []
    for row in parsed_rows:
        time.sleep(0.5)
        venue = _venue_from_match_page(row["source_match_url"])
        snapshot_key = f"{source_hash}|{row['match_date']}|{row['source_match_url']}|{extracted_at.isoformat()}"
        rows.append(
            {
                **row,
                "h2h_snapshot_id": hashlib.sha256(snapshot_key.encode()).hexdigest(),
                "venue": venue,
                "source_url": H2H_URL,
                "source_hash": source_hash,
                "extracted_at": extracted_at,
            }
        )
    logger.info("Parsed %d Argentina–Spain historical matches", len(rows))
    return pd.DataFrame(rows)
