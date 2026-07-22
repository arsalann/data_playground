"""@bruin
name: final_raw.fifa_match_facts
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Typed long-form match facts parsed from FIFA Training Centre's public World
  Cup 2026 post-match summary reports for Argentina and Spain. It retains
  source URLs and hashes, then extracts team metrics, phases of play, starters,
  shot events, player line breaks, and top passing connections, including the
  completed Spain–Argentina final.

  Sources:
  https://www.fifatrainingcentre.com/en/fifa-world-cup-2026/match-report-hub.php
  https://www.fifatrainingcentre.com/en/fifa-world-cup-2026/match-report-hub-knockout-stage.php

materialization:
  type: table
  strategy: append

columns:
  - name: fact_id
    type: VARCHAR
    description: SHA-256 identifier for one parsed fact in one source-PDF version.
    primary_key: true
  - name: match_id
    type: VARCHAR
    description: FIFA tournament match number from the post-match report.
  - name: match_date
    type: DATE
    description: Local match date reported by FIFA.
  - name: stage
    type: VARCHAR
    description: FIFA tournament stage label.
  - name: home_team
    type: VARCHAR
    description: Home team from the FIFA report cover.
  - name: away_team
    type: VARCHAR
    description: Away team from the FIFA report cover.
  - name: team_name
    type: VARCHAR
    description: Team to which the fact belongs.
  - name: opponent_name
    type: VARCHAR
    description: Opposing team in the report.
  - name: fact_type
    type: VARCHAR
    description: "Fact family: team_metric, phase, starter, shot, player_line_break, or passing_connection."
  - name: entity_name
    type: VARCHAR
    description: Player or other primary entity for player-level facts; null for team-level facts.
  - name: related_entity_name
    type: VARCHAR
    description: Related player or delivery type where applicable.
  - name: metric_name
    type: VARCHAR
    description: Snake-case metric identifier.
  - name: numeric_value
    type: DOUBLE
    description: Numeric measurement, count, percentage, or passing-share value.
  - name: text_value
    type: VARCHAR
    description: Categorical detail such as position, phase group, shot outcome, or body part.
  - name: event_minute
    type: INTEGER
    description: Minute for a shot event; null for non-event facts.
  - name: source_url
    type: VARCHAR
    description: Direct FIFA post-match PDF URL.
  - name: source_hash
    type: VARCHAR
    description: SHA-256 hash of the downloaded FIFA PDF.
  - name: report_title
    type: VARCHAR
    description: FIFA report cover title.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp at which the source PDF was retrieved.

@bruin"""

import hashlib
import io
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import pandas as pd
import pdfplumber
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

HUB_URLS = (
    "https://www.fifatrainingcentre.com/en/fifa-world-cup-2026/match-report-hub.php",
    "https://www.fifatrainingcentre.com/en/fifa-world-cup-2026/match-report-hub-knockout-stage.php",
)
TARGET_TEAMS = {"Argentina", "Spain"}
EXPECTED_MATCHES_PER_TEAM = 8
MINIMUM_TARGET_REPORT_URLS = 15
MAX_RETRIES = 5
REQUEST_TIMEOUT_SECONDS = 60
IN_POSSESSION_PHASES = (
    "Build Up Unopposed",
    "Build Up Opposed",
    "Progression",
    "Final Third",
    "Long Ball",
    "Attacking Transition",
    "Counter Attack",
    "Set Piece",
)
OUT_OF_POSSESSION_PHASES = (
    "High Press",
    "Mid Press",
    "Low Press",
    "High Block",
    "Mid Block",
    "Low Block",
    "Recovery",
    "Defensive Transition",
    "Counter-press",
)


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
                raise RuntimeError(f"Unable to retrieve {url} after {MAX_RETRIES} attempts") from exc
            sleep_seconds = min(2**attempt, 30)
            logger.warning("Attempt %d/%d failed for %s: %s; retrying in %ss", attempt, MAX_RETRIES, url, exc, sleep_seconds)
            time.sleep(sleep_seconds)
    raise AssertionError("unreachable")


def discover_report_urls() -> list[str]:
    urls: set[str] = set()
    for hub_url in HUB_URLS:
        soup = BeautifulSoup(_get_with_retry(hub_url), "html.parser")
        for link in soup.find_all("a", href=True):
            url = urljoin(hub_url, link["href"])
            tokens = re.split(r"[-_.]", urlparse(url).path.upper())
            if url.lower().endswith(".pdf") and ({"ARG", "ESP"} & set(tokens)):
                urls.add(url)
    if len(urls) < MINIMUM_TARGET_REPORT_URLS:
        raise ValueError(f"Expected at least {MINIMUM_TARGET_REPORT_URLS} Argentina/Spain FIFA reports, discovered {len(urls)}")
    return sorted(urls)


def _pdf_pages(pdf_bytes: bytes) -> list[tuple[str, list[dict[str, Any]]]]:
    pages = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            pages.append((page.extract_text() or "", page.extract_words(x_tolerance=1, y_tolerance=2)))
    return pages


def parse_match_info(first_page_text: str) -> dict[str, Any]:
    title_match = re.search(r"(?m)^(.+?)\s+(\d+)\s*-\s*(\d+)\s+(.+?)\s*$", first_page_text)
    stage_match = re.search(r"(?m)^(.+?)\s*-\s*Match\s+(\d+)\s*$", first_page_text)
    date_match = re.search(
        r"\b(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+2026)\b",
        first_page_text,
    )
    if not title_match or not stage_match or not date_match:
        raise ValueError("FIFA report cover is missing title, stage/match number, or date")
    lines = [line.strip() for line in first_page_text.splitlines() if line.strip()]
    # Some FIFA PDF fonts encode the final "f" in "Kick Off" as a null byte.
    kick_off_index = next((index for index, line in enumerate(lines) if "Kick O" in line), None)
    if kick_off_index is None or kick_off_index + 1 >= len(lines):
        raise ValueError("FIFA report cover is missing venue after kickoff time")
    return {
        "home_team": title_match.group(1),
        "away_team": title_match.group(4),
        "home_goals": int(title_match.group(2)),
        "away_goals": int(title_match.group(3)),
        "stage": stage_match.group(1),
        "match_id": stage_match.group(2),
        "match_date": datetime.strptime(date_match.group(1), "%d %B %Y").date(),
        "venue": lines[kick_off_index + 1],
        "report_title": title_match.group(0),
    }


def _metric_values(text: str) -> dict[str, tuple[float, float]]:
    patterns = {
        "goals": r"(?m)^\s*(\d+)\s+Goals\s+(\d+)\s*$",
        "xg": r"(?m)^\s*(\d+(?:\.\d+)?)\s+xG \(Expected Goals\)\s+(\d+(?:\.\d+)?)\s*$",
        "attempts": r"(?m)^\s*(\d+)\s+\((\d+)\)\s+Attempts at Goal \(On Target\)\s+(\d+)\s+\((\d+)\)\s*$",
        "passes": r"(?m)^\s*(\d+)\s+\((\d+)\)\s+Total Passes \(Complete\)\s+(\d+)\s+\((\d+)\)\s*$",
        "completed_line_breaks": r"(?m)^\s*(\d+)\s+Completed Line Breaks\s+(\d+)\s*$",
        "ball_progressions": r"(?m)^\s*(\d+)\s+Ball Progressions\s+(\d+)\s*$",
    }
    values: dict[str, tuple[float, float]] = {}
    for name, pattern in patterns.items():
        match = re.search(pattern, text)
        if not match:
            raise ValueError(f"FIFA key-statistics page is missing {name}")
        groups = match.groups()
        if name == "attempts":
            values["attempts"] = (float(groups[0]), float(groups[2]))
            values["attempts_on_target"] = (float(groups[1]), float(groups[3]))
        elif name == "passes":
            values["total_passes"] = (float(groups[0]), float(groups[2]))
            values["completed_passes"] = (float(groups[1]), float(groups[3]))
        else:
            values[name] = (float(groups[0]), float(groups[1]))

    possession = re.search(
        r"(?m)^\s*Total\s+(\d+(?:\.\d+)?)%\s+(?:\d+(?:\.\d+)?%\s+)?(\d+(?:\.\d+)?)%\s+Total\s*$",
        text,
    )
    if not possession:
        raise ValueError("FIFA key-statistics page is missing possession")
    values["possession_pct"] = (float(possession.group(1)), float(possession.group(2)))
    return values


def _phase_values(text: str) -> dict[str, tuple[float, float, str]]:
    values: dict[str, tuple[float, float, str]] = {}
    for group, phases in (("in_possession", IN_POSSESSION_PHASES), ("out_of_possession", OUT_OF_POSSESSION_PHASES)):
        for phase in phases:
            match = re.search(rf"(?m)^\s*(\d+)%\s+{re.escape(phase)}\s+(\d+)%\s*$", text)
            if not match:
                raise ValueError(f"FIFA phases page is missing {phase}")
            values[re.sub(r"[^a-z0-9]+", "_", phase.lower()).strip("_")] = (float(match.group(1)), float(match.group(2)), group)
    return values


def _line_groups(words: list[dict[str, Any]], x_min: float, x_max: float, y_min: float, y_max: float) -> list[list[str]]:
    selected = sorted(
        (word for word in words if x_min <= word["x0"] < x_max and y_min <= word["top"] <= y_max),
        key=lambda word: (round(word["top"] / 2) * 2, word["x0"]),
    )
    groups: list[list[str]] = []
    positions: list[float] = []
    for word in selected:
        if not positions or abs(word["top"] - positions[-1]) > 3:
            positions.append(word["top"])
            groups.append([])
        groups[-1].append(word["text"])
    return groups


def _starters(words: list[dict[str, Any]], home_team: str, away_team: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for team_name, x_min, x_max, side in ((home_team, 0, 450, "left"), (away_team, 520, 960, "right")):
        for tokens in _line_groups(words, x_min, x_max, 120, 285):
            position_match = next(
                ((index, re.fullmatch(r"(GK|DF|MF|FW)(\d+)?", token)) for index, token in enumerate(tokens) if re.fullmatch(r"(GK|DF|MF|FW)(\d+)?", token)),
                None,
            )
            if position_match is None:
                continue
            position_index, match = position_match
            position = match.group(1)
            attached_number = int(match.group(2)) if match.group(2) else None
            if side == "left":
                shirt_number = next((int(token) for token in tokens[:position_index] if token.isdigit()), None)
                player_tokens = [token for token in tokens[position_index + 1 :] if not re.fullmatch(r"\d+(?:\+\d+)?'", token)]
            else:
                player_tokens = [token for token in tokens[:position_index] if not re.fullmatch(r"\d+(?:\+\d+)?'", token)]
                shirt_number = attached_number or next((int(token) for token in tokens[position_index + 1 :] if token.isdigit()), None)
            player_name = " ".join(player_tokens).strip()
            if player_name and shirt_number is not None:
                rows.append({"team_name": team_name, "player_name": player_name, "position": position, "shirt_number": shirt_number})
    counts = {team: sum(row["team_name"] == team for row in rows) for team in (home_team, away_team)}
    if counts != {home_team: 11, away_team: 11}:
        raise ValueError(f"Expected 11 starters per team, parsed {counts}")
    return rows


def _shot_rows(page_text: str) -> tuple[str, list[dict[str, Any]]]:
    title = re.search(r"Attempts at Goal\s+(Argentina|Spain)\s*$", page_text, flags=re.MULTILINE)
    if not title or "Delivery Type" not in page_text:
        return "", []
    rows = []
    # FIFA's text layer sometimes joins shirt number and player name (e.g.
    # ``20Alexis MAC ALLISTER``), so use the controlled outcome vocabulary as
    # the field boundary instead of a layout-dependent run of spaces.
    pattern = re.compile(
        r"(?m)^\s*(\d{1,3})\s+(\d{1,2})\s*(.+?)\s+"
        r"(Deflected Off Target - Defensive Event|Incomplete - Player On Ball Error|Incomplete - Blocked|On Target - Goal|On Target - Saved|Off Target)\s+"
        r"(Right Foot|Left Foot|Head|Other)\s+(.+?)\s*$"
    )
    for match in pattern.finditer(page_text):
        outcome = match.group(4)
        if "Goal" in outcome:
            outcome_group = "goal"
        elif "On Target" in outcome:
            outcome_group = "on_target"
        elif "Off Target" in outcome:
            outcome_group = "off_target"
        elif "Blocked" in outcome:
            outcome_group = "blocked"
        else:
            outcome_group = "incomplete"
        rows.append(
            {
                "team_name": title.group(1),
                "event_minute": int(match.group(1)),
                "shirt_number": int(match.group(2)),
                "player_name": match.group(3).strip(),
                "outcome_group": outcome_group,
                "outcome": outcome,
                "body_part": match.group(5),
                "delivery_type": match.group(6).strip(),
            }
        )
    return title.group(1), rows


def _player_line_breaks(page_text: str) -> tuple[str, list[dict[str, Any]]]:
    title = re.search(r"Line Breaks\s+(Argentina|Spain)\s*$", page_text, flags=re.MULTILINE)
    if not title or "Line Break" not in page_text:
        return "", []
    rows = []
    for match in re.finditer(r"(?m)^\s*(\d+)\s+(.+?)\s+(\d+)\s+(\d+)\s+(\d+)%\s+", page_text):
        player_name = match.group(2).strip()
        if player_name in {"Player", "Line Breaks"}:
            continue
        rows.append(
            {
                "team_name": title.group(1),
                "shirt_number": int(match.group(1)),
                "player_name": player_name,
                "attempted": int(match.group(3)),
                "completed": int(match.group(4)),
                "completion_pct": float(match.group(5)),
            }
        )
    if len(rows) < 11:
        raise ValueError(f"Expected player line-break rows for {title.group(1)}, parsed {len(rows)}")
    return title.group(1), rows


def _passing_connections(page_text: str, words: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    title = re.search(r"Passing Networks\s+(Argentina|Spain)\s*$", page_text, flags=re.MULTILINE)
    if not title:
        return "", []
    rows = []
    for pct_word in words:
        if pct_word["x0"] < 890 or not 120 <= pct_word["top"] <= 300 or not re.fullmatch(r"\d+(?:\.\d+)?%", pct_word["text"]):
            continue
        y = pct_word["top"]
        source_words = sorted(
            (word for word in words if 745 <= word["x0"] < 812 and y - 10 <= word["top"] <= y + 15),
            key=lambda word: (word["top"], word["x0"]),
        )
        destination_words = sorted(
            (word for word in words if 812 <= word["x0"] < 900 and y - 10 <= word["top"] <= y + 15),
            key=lambda word: (word["top"], word["x0"]),
        )
        source = " ".join(word["text"] for word in source_words)
        destination = " ".join(word["text"] for word in destination_words)
        if source and destination:
            rows.append({"team_name": title.group(1), "source_player": source, "destination_player": destination, "share_pct": float(pct_word["text"].rstrip("%"))})
    if len(rows) != 5:
        raise ValueError(f"Expected five top passing connections for {title.group(1)}, parsed {len(rows)}")
    return title.group(1), rows


def is_target_match(match: dict[str, Any]) -> bool:
    """Return whether a FIFA report includes either team in scope, final included."""
    return bool({match["home_team"], match["away_team"]} & TARGET_TEAMS)


def parse_report(pdf_bytes: bytes) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    pages = _pdf_pages(pdf_bytes)
    match = parse_match_info(pages[0][0])
    if not is_target_match(match):
        return match, []

    facts: list[dict[str, Any]] = []
    team_order = (match["home_team"], match["away_team"])
    key_stats_text = next((text for text, _ in pages if "Match Summary - Key Statistics" in text), None)
    phase_text = next((text for text, _ in pages if "Phases of Play" in text), None)
    lineup_words = next((words for text, words in pages if "Match Summary - Teams" in text), None)
    if not key_stats_text or not phase_text or lineup_words is None:
        raise ValueError(f"FIFA report {match['match_id']} is missing a required core section")

    for metric_name, values in _metric_values(key_stats_text).items():
        for team_name, value in zip(team_order, values):
            facts.append({"team_name": team_name, "fact_type": "team_metric", "entity_name": None, "related_entity_name": None, "metric_name": metric_name, "numeric_value": value, "text_value": None, "event_minute": None})
    for phase_name, (home_value, away_value, phase_group) in _phase_values(phase_text).items():
        for team_name, value in zip(team_order, (home_value, away_value)):
            facts.append({"team_name": team_name, "fact_type": "phase", "entity_name": None, "related_entity_name": None, "metric_name": phase_name, "numeric_value": value, "text_value": phase_group, "event_minute": None})
    for starter in _starters(lineup_words, match["home_team"], match["away_team"]):
        facts.append({"team_name": starter["team_name"], "fact_type": "starter", "entity_name": starter["player_name"], "related_entity_name": None, "metric_name": "shirt_number", "numeric_value": starter["shirt_number"], "text_value": starter["position"], "event_minute": None})

    for page_text, words in pages:
        _, shots = _shot_rows(page_text)
        for shot in shots:
            facts.append({"team_name": shot["team_name"], "fact_type": "shot", "entity_name": shot["player_name"], "related_entity_name": shot["delivery_type"], "metric_name": shot["outcome_group"], "numeric_value": 1.0, "text_value": f"{shot['outcome']} | {shot['body_part']}", "event_minute": shot["event_minute"]})
        _, breaks = _player_line_breaks(page_text)
        for item in breaks:
            for metric_name, value in (("attempted_line_breaks", item["attempted"]), ("completed_line_breaks", item["completed"]), ("line_break_completion_pct", item["completion_pct"])):
                facts.append({"team_name": item["team_name"], "fact_type": "player_line_break", "entity_name": item["player_name"], "related_entity_name": None, "metric_name": metric_name, "numeric_value": float(value), "text_value": None, "event_minute": None})
        _, links = _passing_connections(page_text, words)
        for link in links:
            facts.append({"team_name": link["team_name"], "fact_type": "passing_connection", "entity_name": link["source_player"], "related_entity_name": link["destination_player"], "metric_name": "team_pass_share_pct", "numeric_value": link["share_pct"], "text_value": None, "event_minute": None})

    for team_name in TARGET_TEAMS & set(team_order):
        team_facts = [fact for fact in facts if fact["team_name"] == team_name]
        if not any(fact["fact_type"] == "shot" for fact in team_facts):
            raise ValueError(f"FIFA report {match['match_id']} has no parsed shots for {team_name}")
        if sum(fact["fact_type"] == "starter" for fact in team_facts) != 11:
            raise ValueError(f"FIFA report {match['match_id']} does not have 11 parsed starters for {team_name}")
    return match, facts


def materialize():
    logger.info("Bruin interval: %s to %s; source reports are post-match and current-state", os.environ.get("BRUIN_START_DATE"), os.environ.get("BRUIN_END_DATE"))
    extracted_at = datetime.now(timezone.utc)
    rows = []
    included_reports = {team: 0 for team in TARGET_TEAMS}
    for source_url in discover_report_urls():
        time.sleep(0.5)
        pdf_bytes = _get_with_retry(source_url)
        source_hash = hashlib.sha256(pdf_bytes).hexdigest()
        match, facts = parse_report(pdf_bytes)
        if not facts:
            logger.info("Skipped non-target report %s", source_url)
            continue
        for team_name in TARGET_TEAMS & {match["home_team"], match["away_team"]}:
            included_reports[team_name] += 1
        for sequence, fact in enumerate(facts):
            opponent_name = match["away_team"] if fact["team_name"] == match["home_team"] else match["home_team"]
            key = "|".join((source_hash, match["match_id"], fact["team_name"], fact["fact_type"], str(fact["entity_name"]), str(fact["related_entity_name"]), fact["metric_name"], str(fact["event_minute"]), str(sequence)))
            rows.append({"fact_id": hashlib.sha256(key.encode()).hexdigest(), **match, **fact, "opponent_name": opponent_name, "source_url": source_url, "source_hash": source_hash, "extracted_at": extracted_at})
        logger.info("Parsed match %s (%s): %d facts", match["match_id"], match["report_title"], len(facts))
    expected_reports = {team: EXPECTED_MATCHES_PER_TEAM for team in TARGET_TEAMS}
    if included_reports != expected_reports:
        raise ValueError(f"Expected {EXPECTED_MATCHES_PER_TEAM} FIFA reports per team, got {included_reports}")
    logger.info("Returning %d typed FIFA facts from %d completed reports", len(rows), sum(included_reports.values()))
    return pd.DataFrame(rows)
