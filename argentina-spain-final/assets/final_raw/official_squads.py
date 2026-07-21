"""@bruin
name: final_raw.official_squads
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Official 26-player FIFA World Cup 2026 squad lists for Argentina and Spain.
  The source document is versioned by FIFA; each extraction is appended so a
  later revision can be audited and staging can retain its latest version.

  Source: https://fdp.fifa.org/assetspublic/ce281/pdf/SquadLists-English.pdf

materialization:
  type: table
  strategy: append

columns:
  - name: squad_snapshot_id
    type: VARCHAR
    description: Stable hash of the source version, team, shirt number, and extraction timestamp.
    primary_key: true
  - name: team_name
    type: VARCHAR
    description: FIFA team name, Argentina or Spain.
  - name: squad_number
    type: INTEGER
    description: Official tournament shirt number.
  - name: position
    type: VARCHAR
    description: "FIFA position code: GK, DF, MF, or FW."
  - name: player_name
    type: VARCHAR
    description: FIFA player display name.
  - name: first_names
    type: VARCHAR
    description: Player first name(s) from the FIFA list.
  - name: last_names
    type: VARCHAR
    description: Player last name(s) from the FIFA list.
  - name: shirt_name
    type: VARCHAR
    description: Player name printed on the tournament shirt.
  - name: date_of_birth
    type: DATE
    description: Player date of birth in the FIFA squad list.
  - name: club
    type: VARCHAR
    description: Club listed by FIFA at the squad-list version date.
  - name: height_cm
    type: INTEGER
    description: Listed player height in centimetres.
  - name: caps
    type: INTEGER
    description: Senior international appearances listed by FIFA.
  - name: goals
    type: INTEGER
    description: Senior international goals listed by FIFA.
  - name: coach
    type: VARCHAR
    description: Head coach listed by FIFA for the team.
  - name: source_url
    type: VARCHAR
    description: Direct FIFA PDF URL.
  - name: source_version
    type: VARCHAR
    description: FIFA document footer version and publication timestamp.
  - name: source_hash
    type: VARCHAR
    description: SHA-256 hash of the downloaded FIFA PDF.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp at which this PDF was retrieved.

@bruin"""

import hashlib
import io
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import pdfplumber
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

SQUAD_LIST_URL = "https://fdp.fifa.org/assetspublic/ce281/pdf/SquadLists-English.pdf"
TARGET_TEAMS = ("Argentina", "Spain")
MAX_RETRIES = 5
REQUEST_TIMEOUT_SECONDS = 60


def _get_with_retry(url: str) -> bytes:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code in {429, 502, 503, 504}:
                raise requests.HTTPError(f"transient HTTP {response.status_code}")
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"Unable to retrieve FIFA squad list after {MAX_RETRIES} attempts") from exc
            sleep_seconds = min(2**attempt, 30)
            logger.warning("Squad list attempt %d/%d failed: %s; retrying in %ss", attempt, MAX_RETRIES, exc, sleep_seconds)
            time.sleep(sleep_seconds)
    raise AssertionError("unreachable")


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Return layout-preserving text so table columns remain separable."""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        pages = [page.extract_text(layout=True) or "" for page in pdf.pages]
    return "\n\f\n".join(pages)


def _parse_player_row(parts: list[str | None]) -> dict[str, Any] | None:
    if not parts[0] or not re.fullmatch(r"\d{1,2}", parts[0]) or parts[1] not in {"GK", "DF", "MF", "FW"}:
        return None
    try:
        dob_index = next(index for index, value in enumerate(parts) if value and re.fullmatch(r"\d{2}/\d{2}/\d{4}", value))
        trailing_values = [value for value in parts[dob_index + 1 :] if value]
        if len(trailing_values) != 4:
            raise ValueError(f"unexpected FIFA table tail {trailing_values!r}")
        return {
            "squad_number": int(parts[0]),
            "position": parts[1],
            "player_name": parts[2],
            "first_names": parts[4],
            "last_names": parts[5],
            "shirt_name": parts[7],
            "date_of_birth": datetime.strptime(str(parts[dob_index]), "%d/%m/%Y").date(),
            "club": trailing_values[0],
            "height_cm": int(str(trailing_values[1])),
            "caps": int(str(trailing_values[2])),
            "goals": int(str(trailing_values[3])),
        }
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Could not parse FIFA player row: {parts!r}") from exc


def parse_squad_pdf(pdf_bytes: bytes) -> tuple[list[dict[str, Any]], str]:
    """Parse both target squads and fail loudly if FIFA's table changes."""
    text = extract_pdf_text(pdf_bytes)
    version_match = re.search(r"([A-Za-z]+, \d{1,2} [A-Za-z]+ \d{4} \| \d{2}:\d{2} UTC \| Version \d+)", text)
    if not version_match:
        raise ValueError("FIFA squad PDF has no parseable source-version footer")
    source_version = version_match.group(1)
    rows: list[dict[str, Any]] = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for team_name in TARGET_TEAMS:
            page = next((page for page in pdf.pages if f"{team_name} (" in (page.extract_text() or "")), None)
            if page is None:
                raise ValueError(f"Could not find {team_name} page in FIFA squad PDF")
            tables = page.extract_tables()
            if not tables:
                raise ValueError(f"Could not extract FIFA squad table for {team_name}")
            table = max(tables, key=len)
            coach_row = next((row for row in table if row and row[0] == "Head coach"), None)
            if not coach_row or not coach_row[3]:
                raise ValueError(f"Could not parse {team_name} head coach")
            coach = coach_row[3].strip()
            team_rows = [parsed for row in table if (parsed := _parse_player_row(row))]
            if len(team_rows) != 26:
                raise ValueError(f"Expected 26 {team_name} players, parsed {len(team_rows)}")
            if len({row['squad_number'] for row in team_rows}) != 26:
                raise ValueError(f"Duplicate shirt number in {team_name} FIFA squad")
            for row in team_rows:
                rows.append({**row, "team_name": team_name, "coach": coach})
    return rows, source_version


def materialize():
    logger.info(
        "Bruin interval: %s to %s; FIFA squad source is a current-version document",
        os.environ.get("BRUIN_START_DATE"),
        os.environ.get("BRUIN_END_DATE"),
    )
    extracted_at = datetime.now(timezone.utc)
    pdf_bytes = _get_with_retry(SQUAD_LIST_URL)
    source_hash = hashlib.sha256(pdf_bytes).hexdigest()
    parsed_rows, source_version = parse_squad_pdf(pdf_bytes)
    rows = []
    for row in parsed_rows:
        snapshot_key = f"{source_hash}|{row['team_name']}|{row['squad_number']}|{extracted_at.isoformat()}"
        rows.append(
            {
                **row,
                "squad_snapshot_id": hashlib.sha256(snapshot_key.encode()).hexdigest(),
                "source_url": SQUAD_LIST_URL,
                "source_version": source_version,
                "source_hash": source_hash,
                "extracted_at": extracted_at,
            }
        )
    logger.info("Parsed %d official squad rows (%s)", len(rows), source_version)
    return pd.DataFrame(rows)
