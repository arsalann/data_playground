"""@bruin

name: raw.metro_costs_project
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Global urban rail construction project costs from the Transit Costs Project.
  The public spreadsheet contains project and phase-level route length, tunnel,
  elevated, and at-grade alignment, stations, construction dates, cost source,
  and PPP/inflation-adjusted cost per kilometer.

  Source: https://transitcosts.com/data/
  Current data export: https://docs.google.com/spreadsheets/d/16GoHcbW-eVzHUUP_XCWVXS1s_i3ZBnmZh4kvdSX7muU/export?format=csv
  Costs are reported in 2025 USD millions after the source's PPP and inflation adjustment.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: project_id
    type: VARCHAR
    description: Stable Transit Costs Project identifier, or a deterministic fallback key when the source identifier is missing.
    primary_key: true
  - name: country_code
    type: VARCHAR
    description: ISO-like two-letter country code used by the Transit Costs Project source.
  - name: city
    type: VARCHAR
    description: City or metropolitan area associated with the project.
  - name: line_name
    type: VARCHAR
    description: Transit line or project family name.
  - name: phase_name
    type: VARCHAR
    description: Named project phase or segment.
  - name: start_year
    type: INTEGER
    description: Construction start year when available.
  - name: end_year
    type: INTEGER
    description: Construction completion or expected completion year when available.
  - name: regional_rail_flag
    type: BOOLEAN
    description: Source regional-rail flag; true rows are excluded from the metro comparison.
  - name: length_km
    type: DOUBLE
    description: Revenue corridor length in kilometers, not single-track length.
  - name: tunnel_pct
    type: DOUBLE
    description: Percentage of corridor length in tunnel, from 0 to 100 percent.
  - name: tunnel_km
    type: DOUBLE
    description: Corridor length in tunnel, in kilometers.
  - name: elevated_km
    type: DOUBLE
    description: Elevated corridor length, in kilometers.
  - name: at_grade_km
    type: DOUBLE
    description: At-grade corridor length, in kilometers.
  - name: stations
    type: INTEGER
    description: Number of stations attributed to the project or phase.
  - name: platform_length_m
    type: DOUBLE
    description: Platform length in meters when available.
  - name: cost_source_type
    type: VARCHAR
    description: Source quality category for the cost value, such as Plan, Trade, Media, or Wiki.
  - name: cost_millions
    type: DOUBLE
    description: Source-reported project cost in millions of the source currency.
  - name: currency
    type: VARCHAR
    description: Currency code for the source-reported project cost.
  - name: cost_year
    type: INTEGER
    description: Year associated with the source-reported project cost.
  - name: ppp_rate
    type: DOUBLE
    description: Source purchasing-power-parity conversion rate.
  - name: real_cost_millions_usd
    type: DOUBLE
    description: Transit Costs Project real cost in millions of US dollars before the 2025 inflation adjustment.
  - name: cost_per_km_millions_usd
    type: DOUBLE
    description: Transit Costs Project cost per kilometer in millions of US dollars before the 2025 inflation adjustment.
  - name: real_cost_2025_millions_usd
    type: DOUBLE
    description: Project cost in 2025 US dollars and millions after source PPP and inflation adjustment.
  - name: cost_per_km_2025_usd_m
    type: DOUBLE
    description: Project cost per corridor kilometer in 2025 US dollars and millions.
  - name: source_length_type
    type: VARCHAR
    description: Source category for the length estimate, such as Plan, Measured, or Wiki.
  - name: reference_url
    type: VARCHAR
    description: Supporting reference URL supplied by the Transit Costs Project.
  - name: comparability_class
    type: VARCHAR
    description: Transparent classification used to include metro/urban rail or exclude regional, high-speed, BRT, and tram/streetcar projects.
  - name: analysis_region
    type: VARCHAR
    description: "Broad geography label: North America, Europe, or Other."
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this source snapshot was fetched.

@bruin"""

import csv
import hashlib
import io
import logging
import os
import re
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

SOURCE_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "16GoHcbW-eVzHUUP_XCWVXS1s_i3ZBnmZh4kvdSX7muU/export?format=csv"
)

NORTH_AMERICA = {"CA", "MX", "US"}
EUROPE = {
    "AT", "BE", "BG", "CH", "CZ", "DE", "DK", "EE", "ES", "FI", "FR",
    "GR", "HR", "HU", "IE", "IS", "IT", "LT", "LU", "LV", "NL", "NO",
    "PL", "PT", "RO", "SE", "SI", "SK", "UK", "GB",
}


def parse_number(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = str(value).strip().replace(",", "")
    if not cleaned or cleaned.lower() in {"null", "n/a", "na", "-"}:
        return None
    cleaned = cleaned.replace("%", "")
    match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
    return float(match.group(0)) if match else None


def parse_year(value: str | None) -> int | None:
    number = parse_number(value)
    if number is None:
        return None
    year = int(number)
    return year if 1900 <= year <= 2200 else None


def classify_project(line_name: str, phase_name: str, regional_rail: bool) -> str:
    text = f"{line_name} {phase_name}".lower()
    if regional_rail:
        return "excluded_regional_mainline"
    if any(token in text for token in ("high speed", "high-speed", "hsr", "tgv", "shinkansen")):
        return "excluded_high_speed"
    if any(token in text for token in ("brt", "bus rapid", "busway")):
        return "excluded_brt"
    if any(token in text for token in ("tram", "streetcar", "trolley", "light rail transit", " lrt")):
        return "excluded_tram_streetcar_lrt"
    return "included_metro_urban_rail"


def region_for(country_code: str) -> str:
    if country_code in NORTH_AMERICA:
        return "North America"
    if country_code in EUROPE:
        return "Europe"
    return "Other"


def fetch_source() -> list[dict]:
    response = requests.get(SOURCE_URL, timeout=120)
    response.raise_for_status()
    rows = list(csv.reader(io.StringIO(response.text)))
    if len(rows) < 3:
        raise ValueError("Transit Costs Project export did not contain a header and data rows")

    header = [re.sub(r"\s+", " ", cell).strip() for cell in rows[1]]
    return [dict(zip(header, row)) for row in rows[2:] if any(cell.strip() for cell in row)]


def materialize():
    extracted_at = datetime.now(timezone.utc)
    source_rows = fetch_source()
    output = []

    for row in source_rows:
        country_code = (row.get("Country") or "").strip().upper()
        city = (row.get("City") or "").strip()
        line_name = (row.get("Line") or "").strip()
        phase_name = (row.get("Phase") or "").strip()
        raw_id = (row.get("id") or "").strip()
        regional_rail = (row.get("RR?") or "").strip() == "1"
        fallback = "|".join([
            country_code, city, line_name, phase_name,
            row.get("Start year", ""), row.get("Length", ""),
        ])
        project_id = raw_id or hashlib.sha1(fallback.encode("utf-8")).hexdigest()[:16]
        stations = parse_number(row.get("Stations"))

        output.append({
            "project_id": project_id,
            "country_code": country_code,
            "city": city,
            "line_name": line_name,
            "phase_name": phase_name,
            "start_year": parse_year(row.get("Start year")),
            "end_year": parse_year(row.get("End year")),
            "regional_rail_flag": regional_rail,
            "length_km": parse_number(row.get("Length")),
            "tunnel_pct": parse_number(row.get("TunnelPer")),
            "tunnel_km": parse_number(row.get("Tunnel")),
            "elevated_km": parse_number(row.get("Elevated")),
            "at_grade_km": parse_number(row.get("Atgrade")),
            "stations": int(stations) if stations is not None else None,
            "platform_length_m": parse_number(row.get("Platform Length (Meters)")),
            "cost_source_type": (row.get("Source1") or "").strip(),
            "cost_millions": parse_number(row.get("Cost")),
            "currency": (row.get("Currency") or "").strip(),
            "cost_year": parse_year(row.get("Year")),
            "ppp_rate": parse_number(row.get("PPP rate")),
            "real_cost_millions_usd": parse_number(row.get("Real cost")),
            "cost_per_km_millions_usd": parse_number(row.get("Cost/km (Millions)")),
            "real_cost_2025_millions_usd": parse_number(row.get("Real cost (2025 dollars)")),
            "cost_per_km_2025_usd_m": parse_number(row.get("Cost/km (2025 dollars)")),
            "source_length_type": (row.get("Source2") or "").strip(),
            "reference_url": (row.get("Reference") or "").strip(),
            "comparability_class": classify_project(line_name, phase_name, regional_rail),
            "analysis_region": region_for(country_code),
            "extracted_at": extracted_at,
        })

    frame = pd.DataFrame(output)
    logger.info("Fetched %d Transit Costs Project rows", len(frame))
    logger.info(
        "Included metro/urban rail candidates: %d",
        (frame["comparability_class"] == "included_metro_urban_rail").sum(),
    )
    logger.info("Rows with 2025 cost/km: %d", frame["cost_per_km_2025_usd_m"].notna().sum())
    return frame
