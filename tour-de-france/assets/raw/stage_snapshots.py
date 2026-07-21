"""@bruin
name: raw.stage_snapshots
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Appends complete ProCyclingStats snapshots for published stages of the 2026
  men's Tour de France. Each row preserves the full stage-results payload and
  top-30 post-stage GC payload so source revisions remain auditable.

  Source: https://www.procyclingstats.com/race/tour-de-france/2026
  The source is fetched through a browser-compatible client and remains subject
  to ProCyclingStats terms of use and post-publication result revisions.

materialization:
  type: table
  strategy: append

parameters:
  enforce_schema: true

columns:
  - name: snapshot_id
    type: VARCHAR
    description: SHA-256 identifier for this immutable stage-source snapshot.
    primary_key: true
    checks:
      - name: unique
      - name: not_null
  - name: stage_number
    type: INTEGER
    description: Official 2026 Tour stage number, from 1 through 21.
    checks:
      - name: not_null
  - name: stage_date
    type: DATE
    description: Scheduled local race date for the completed stage.
    checks:
      - name: not_null
  - name: stage_name
    type: VARCHAR
    description: PCS route label for the stage, including start and finish locations.
  - name: stage_distance_km
    type: DOUBLE
    description: Published stage distance in kilometres.
  - name: stage_type
    type: VARCHAR
    description: Road-stage, team-time-trial, or individual-time-trial classification.
  - name: stage_status
    type: VARCHAR
    description: Publication status; only complete published snapshots are emitted.
  - name: stage_results_payload
    type: VARCHAR
    description: JSON array containing the complete published stage-result table.
  - name: gc_top30_payload
    type: VARCHAR
    description: JSON array containing the top 30 published GC standings after this stage.
  - name: stage_source_url
    type: VARCHAR
    description: PCS URL for the published stage-result page.
  - name: gc_source_url
    type: VARCHAR
    description: PCS URL for the published post-stage GC page.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this PCS source snapshot was extracted.

custom_checks:
  - name: stage numbers stay within the 2026 route
    description: Every saved snapshot must refer to one of the 21 scheduled stages.
    query: SELECT COUNT(*) FROM raw.stage_snapshots WHERE stage_number NOT BETWEEN 1 AND 21
    value: 0

@bruin"""

import hashlib
import html
import json
import logging
import os
import re
import time
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from curl_cffi import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

RACE_ROOT = "https://www.procyclingstats.com/race/tour-de-france/2026"
RACE_START = date(2026, 7, 4)
RACE_END = date(2026, 7, 26)
REQUEST_DELAY_SECONDS = 0.5
MAX_RETRIES = 5
TRANSIENT_STATUSES = {429, 500, 502, 503, 504}

STAGE_DATES = {
    stage: day
    for stage, day in enumerate(
        [
            date(2026, 7, 4), date(2026, 7, 5), date(2026, 7, 6),
            date(2026, 7, 7), date(2026, 7, 8), date(2026, 7, 9),
            date(2026, 7, 10), date(2026, 7, 11), date(2026, 7, 12),
            date(2026, 7, 14), date(2026, 7, 15), date(2026, 7, 16),
            date(2026, 7, 17), date(2026, 7, 18), date(2026, 7, 19),
            date(2026, 7, 21), date(2026, 7, 22), date(2026, 7, 23),
            date(2026, 7, 24), date(2026, 7, 25), date(2026, 7, 26),
        ],
        start=1,
    )
}

OUTPUT_COLUMNS = [
    "snapshot_id",
    "stage_number",
    "stage_date",
    "stage_name",
    "stage_distance_km",
    "stage_type",
    "stage_status",
    "stage_results_payload",
    "gc_top30_payload",
    "stage_source_url",
    "gc_source_url",
    "extracted_at",
]


class ResultsTableParser(HTMLParser):
    """Small dependency-free parser for a single PCS results table."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.headers: list[str] = []
        self.rows: list[list[dict[str, Any]]] = []
        self._current_row: list[dict[str, Any]] | None = None
        self._current_cell: dict[str, Any] | None = None
        self._current_anchor: dict[str, Any] | None = None
        self._in_header = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        if tag == "thead":
            self._in_header = True
        elif tag == "tr":
            self._current_row = []
        elif tag in {"td", "th"}:
            self._current_cell = {
                "text": [],
                "anchors": [],
                "code": attr_map.get("data-code", ""),
            }
        elif tag == "a" and self._current_cell is not None and attr_map.get("href"):
            self._current_anchor = {"href": attr_map["href"], "text": []}

    def handle_data(self, data: str) -> None:
        if self._current_cell is not None:
            self._current_cell["text"].append(data)
        if self._current_anchor is not None:
            self._current_anchor["text"].append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "thead":
            self._in_header = False
        elif tag == "a" and self._current_cell is not None and self._current_anchor is not None:
            self._current_anchor["text"] = " ".join("".join(self._current_anchor["text"]).split())
            self._current_cell["anchors"].append(self._current_anchor)
            self._current_anchor = None
        elif tag in {"td", "th"} and self._current_cell is not None:
            cell = self._current_cell
            cell["text"] = " ".join("".join(cell["text"]).split())
            if self._in_header:
                self.headers.append(cell["code"])
            elif self._current_row is not None:
                self._current_row.append(cell)
            self._current_cell = None
        elif tag == "tr" and self._current_row:
            self.rows.append(self._current_row)
            self._current_row = None


def _plain_text(value: str) -> str:
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", value)).split())


def _active_results_table(page_html: str) -> list[dict[str, dict[str, Any]]]:
    """Return rows from PCS's currently selected result tab, if published."""
    tabs_match = re.search(
        r'<ul class="tabs tabnav resultTabs.*?</ul>', page_html, flags=re.DOTALL
    )
    if not tabs_match:
        return []

    selected_match = re.search(
        r'<li class="cur"\s+data-id="(?P<tab_id>\d+)"', tabs_match.group(0)
    )
    if not selected_match:
        return []

    tab_id = selected_match.group("tab_id")
    active_match = re.search(
        rf'<div class="resTab[^\"]*"\s+data-id="{tab_id}">', page_html
    )
    if not active_match:
        return []

    table_match = re.search(
        r'<table class="results.*?</table>', page_html[active_match.end() :], flags=re.DOTALL
    )
    if not table_match:
        return []

    parser = ResultsTableParser()
    parser.feed(table_match.group(0))
    if not parser.headers:
        return []

    parsed_rows: list[dict[str, dict[str, Any]]] = []
    for cells in parser.rows:
        if len(cells) != len(parser.headers):
            continue
        row = dict(zip(parser.headers, cells))
        if not row.get("rnk", {}).get("text", "").isdigit():
            continue
        parsed_rows.append(row)
    return parsed_rows


def _clean_time(value: str) -> str:
    candidates = re.findall(r"\+?\d+:\d{2}(?::\d{2})?", value.replace(",,", "").replace("*", ""))
    return candidates[-1] if candidates else ""


def _slug(cell: dict[str, Any]) -> str | None:
    for anchor in cell.get("anchors", []):
        href = anchor["href"]
        if "/" in href:
            return href.rsplit("/", 1)[-1]
    return None


def _anchor_text(cell: dict[str, Any]) -> str | None:
    anchors = cell.get("anchors", [])
    return anchors[0]["text"] if anchors and anchors[0]["text"] else None


def _stage_rows(rows: list[dict[str, dict[str, Any]]]) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    for row in rows:
        rider = row.get("ridername", {})
        team = row.get("teamnamelink", {})
        parsed.append(
            {
                "rank": int(row["rnk"]["text"]),
                "rider_name": _anchor_text(rider) or rider.get("text") or None,
                "rider_slug": _slug(rider),
                "team_name": _anchor_text(team) or team.get("text") or None,
                "team_slug": _slug(team),
                "stage_time": _clean_time(row.get("time", {}).get("text", "")) or None,
            }
        )
    return parsed


def _gc_rows(rows: list[dict[str, dict[str, Any]]]) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    for row in rows[:30]:
        rider = row.get("ridername", {})
        team = row.get("teamnamelink", {})
        previous_rank = row.get("prev", {}).get("text", "")
        parsed.append(
            {
                "rank": int(row["rnk"]["text"]),
                "previous_rank": int(previous_rank) if previous_rank.isdigit() else None,
                "reported_rank_change": row.get("delta", {}).get("text") or None,
                "rider_name": _anchor_text(rider) or rider.get("text") or None,
                "rider_slug": _slug(rider),
                "team_name": _anchor_text(team) or team.get("text") or None,
                "team_slug": _slug(team),
                "gc_time": _clean_time(row.get("time", {}).get("text", "")) or None,
            }
        )
    return parsed


def _stage_metadata(page_html: str, stage_number: int) -> tuple[str, float | None, str]:
    title_match = re.search(
        r'<div class="title-line2[^>]*>(?P<title>.*?)</div>', page_html, flags=re.DOTALL
    )
    title = _plain_text(title_match.group("title")) if title_match else f"Stage {stage_number}"
    distance_match = re.search(r"\((?P<distance>\d+(?:\.\d+)?)km\)", title)
    distance_km = float(distance_match.group("distance")) if distance_match else None
    stage_name = re.sub(r"^Stage\s+\d+(?:\s+\([^)]*\))?\s*»\s*", "", title)
    stage_name = re.sub(r"\s*\(\d+(?:\.\d+)?km\)$", "", stage_name).strip()
    if stage_number == 1:
        stage_type = "Team time trial"
    elif stage_number == 16:
        stage_type = "Individual time trial"
    else:
        stage_type = "Road stage"
    return stage_name or f"Stage {stage_number}", distance_km, stage_type


def _fetch_page(session: requests.Session, url: str) -> str | None:
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=30)
            if response.status_code == 200:
                return response.text
            if response.status_code not in TRANSIENT_STATUSES:
                logger.warning("Skipping %s after non-retryable HTTP %s", url, response.status_code)
                return None
            raise RuntimeError(f"Transient HTTP {response.status_code}")
        except Exception as exc:  # curl-cffi errors do not share one stable base class
            if attempt >= MAX_RETRIES:
                logger.warning("Skipping %s after %d retries: %s", url, MAX_RETRIES, exc)
                return None
            delay = 2**attempt
            logger.warning("Request %s failed (%s); retrying in %ss", url, exc, delay)
            time.sleep(delay)
        finally:
            time.sleep(REQUEST_DELAY_SECONDS)
    return None


def _requested_stage_numbers(start_date: date, end_date: date) -> list[int]:
    paris_today = datetime.now(ZoneInfo("Europe/Paris")).date()
    latest_allowed_date = min(end_date, RACE_END, paris_today)
    return [
        stage
        for stage, stage_date in STAGE_DATES.items()
        if max(start_date, RACE_START) <= stage_date <= latest_allowed_date
    ]


def fetch_stage_snapshots(start_date: date, end_date: date) -> list[dict[str, Any]]:
    """Fetch complete published results for the requested race dates only."""
    stage_numbers = _requested_stage_numbers(start_date, end_date)
    logger.info("Requested interval %s to %s maps to stages %s", start_date, end_date, stage_numbers)
    if not stage_numbers:
        return []

    snapshots: list[dict[str, Any]] = []
    with requests.Session(impersonate="chrome") as session:
        for stage_number in stage_numbers:
            stage_url = f"{RACE_ROOT}/stage-{stage_number}"
            gc_url = f"{RACE_ROOT}/stage-{stage_number}-gc"
            stage_html = _fetch_page(session, stage_url)
            if not stage_html:
                continue
            stage_results = _stage_rows(_active_results_table(stage_html))
            if not stage_results:
                logger.info("Stage %s has no published result table yet; skipping", stage_number)
                continue

            gc_html = _fetch_page(session, gc_url)
            if not gc_html:
                continue
            gc_top30 = _gc_rows(_active_results_table(gc_html))
            if len(gc_top30) < 2:
                logger.info("Stage %s has no published GC table yet; skipping", stage_number)
                continue

            stage_name, stage_distance_km, stage_type = _stage_metadata(stage_html, stage_number)
            extracted_at = datetime.now(timezone.utc)
            payload = {
                "stage_results": stage_results,
                "gc_top30": gc_top30,
                "stage_url": stage_url,
                "gc_url": gc_url,
                "extracted_at": extracted_at.isoformat(),
            }
            snapshot_id = hashlib.sha256(
                json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
            ).hexdigest()
            snapshots.append(
                {
                    "snapshot_id": snapshot_id,
                    "stage_number": stage_number,
                    "stage_date": STAGE_DATES[stage_number].isoformat(),
                    "stage_name": stage_name,
                    "stage_distance_km": stage_distance_km,
                    "stage_type": stage_type,
                    "stage_status": "published",
                    "stage_results_payload": json.dumps(stage_results, ensure_ascii=False),
                    "gc_top30_payload": json.dumps(gc_top30, ensure_ascii=False),
                    "stage_source_url": stage_url,
                    "gc_source_url": gc_url,
                    "extracted_at": extracted_at,
                }
            )
            logger.info(
                "Captured stage %s: %d result rows and %d GC rows",
                stage_number,
                len(stage_results),
                len(gc_top30),
            )
    return snapshots


def materialize() -> pd.DataFrame | None:
    start_date = date.fromisoformat(os.environ.get("BRUIN_START_DATE", RACE_START.isoformat()))
    end_date = date.fromisoformat(os.environ.get("BRUIN_END_DATE", RACE_END.isoformat()))
    snapshots = fetch_stage_snapshots(start_date, end_date)
    if not snapshots:
        logger.info("No complete published stage snapshot was available for this interval")
        return None

    dataframe = pd.DataFrame(snapshots, columns=OUTPUT_COLUMNS)
    logger.info("Materializing %d append-only stage snapshots", len(dataframe))
    return dataframe
