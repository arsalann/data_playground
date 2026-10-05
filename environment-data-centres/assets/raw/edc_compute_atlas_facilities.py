"""@bruin
name: raw.edc_compute_atlas_facilities
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  US data centre facilities from the Compute Atlas open dataset.
  Every record is source-cited; the atlas is built from local permits,
  tax abatements, water filings and interconnection queues.

  Source: https://www.compute-atlas.com/ (public JSON API, no auth)
  Endpoint: GET /api/facilities
  License: CC-BY-4.0

  Note: the site robots.txt disallows /api/ to crawlers while the API docs
  state public reads are unauthenticated. This asset makes a single request
  per run with an identifying User-Agent.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: facility_id
    type: VARCHAR
    description: Compute Atlas facility slug identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: name
    type: VARCHAR
    description: Facility name as published
  - name: operator
    type: VARCHAR
    description: Operating company
  - name: status
    type: VARCHAR
    description: "Lifecycle status: operational, under_construction, permitted, proposed, cancelled"
    checks:
      - name: accepted_values
        value: [operational, under_construction, permitted, proposed, cancelled]
  - name: confidence
    type: VARCHAR
    description: "Evidence confidence: confirmed, reported, rumored"
  - name: lat
    type: DOUBLE
    description: Latitude in WGS84 decimal degrees
  - name: lon
    type: DOUBLE
    description: Longitude in WGS84 decimal degrees
  - name: location_precision
    type: VARCHAR
    description: "Coordinate precision: exact, approximate, representative_multi_site"
  - name: city
    type: VARCHAR
    description: City name
  - name: county
    type: VARCHAR
    description: County name as published (not FIPS)
  - name: state
    type: VARCHAR
    description: Two-letter US state code
  - name: capacity_mw_operational
    type: DOUBLE
    description: Operational IT/site capacity in megawatts, null where unknown
  - name: capacity_mw_planned
    type: DOUBLE
    description: Planned capacity in megawatts, null where unknown
  - name: energy_source
    type: VARCHAR
    description: Primary power source as reported
  - name: energy_utility
    type: VARCHAR
    description: Serving electric utility
  - name: on_site_generation_mw
    type: DOUBLE
    description: On-site generation capacity in megawatts
  - name: cooling_type
    type: VARCHAR
    description: Cooling technology as reported, null where unknown
  - name: water_reported_mgd
    type: DOUBLE
    description: Reported water use in million gallons per day
  - name: land_acres
    type: DOUBLE
    description: Site area in acres
  - name: investment_usd
    type: DOUBLE
    description: Announced capital investment in US dollars
  - name: jobs_construction
    type: INTEGER
    description: Announced construction jobs (promotional figure, not audited)
  - name: jobs_permanent
    type: INTEGER
    description: Announced permanent jobs (promotional figure, not audited)
  - name: subsidy_count
    type: INTEGER
    description: Number of subsidy records attached to this facility
  - name: community_status
    type: VARCHAR
    description: Community response status where recorded
  - name: facility_type
    type: VARCHAR
    description: Facility type classification
  - name: ai_classification
    type: VARCHAR
    description: "AI-workload classification: confirmed, likely, mixed_use"
  - name: announced_date
    type: VARCHAR
    description: Announcement date as published (free-form string upstream)
  - name: status_history_count
    type: INTEGER
    description: Number of recorded status transitions, used for attrition analysis
  - name: status_history_json
    type: VARCHAR
    description: Full status history as a JSON string
  - name: source_count
    type: INTEGER
    description: Number of cited public sources for this record
  - name: last_updated
    type: VARCHAR
    description: Upstream last-updated timestamp
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this snapshot was pulled

@bruin"""

import json
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

API_URL = "https://www.compute-atlas.com/api/facilities"
USER_AGENT = "bruin-data-playground/1.0 (research; environment-data-centres pipeline)"
TIMEOUT = 120


def _num(value):
    """Return a float, or None for missing/non-numeric values."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value):
    number = _num(value)
    return None if number is None else int(number)


def fetch_facilities() -> list:
    logger.info("Fetching Compute Atlas facilities from %s", API_URL)
    response = requests.get(
        API_URL, headers={"User-Agent": USER_AGENT, "Accept": "application/json"}, timeout=TIMEOUT
    )
    response.raise_for_status()
    payload = response.json()
    facilities = payload.get("facilities", [])
    logger.info("API reported count=%s, received %d records", payload.get("count"), len(facilities))
    return facilities


def flatten(record: dict) -> dict:
    location = record.get("location") or {}
    capacity = record.get("capacityMw") or {}
    energy = record.get("energy") or {}
    water = record.get("water") or {}
    jobs = record.get("jobs") or {}
    community = record.get("community") or {}
    history = record.get("statusHistory") or []

    return {
        "facility_id": record.get("id"),
        "name": record.get("name"),
        "operator": record.get("operator"),
        "status": record.get("status"),
        "confidence": record.get("confidence"),
        "lat": _num(location.get("lat")),
        "lon": _num(location.get("lon")),
        "location_precision": location.get("precision"),
        "city": location.get("city"),
        "county": location.get("county"),
        "state": location.get("state"),
        "capacity_mw_operational": _num(capacity.get("operational")),
        "capacity_mw_planned": _num(capacity.get("planned")),
        "energy_source": energy.get("source"),
        "energy_utility": energy.get("utility"),
        "on_site_generation_mw": _num(energy.get("onSiteGenerationMw")),
        "cooling_type": water.get("coolingType"),
        "water_reported_mgd": _num(water.get("reportedMgd")),
        "land_acres": _num(record.get("landAcres")),
        "investment_usd": _num(record.get("investmentUsd")),
        "jobs_construction": _int(jobs.get("construction")),
        "jobs_permanent": _int(jobs.get("permanent")),
        "subsidy_count": len(record.get("subsidies") or []),
        "community_status": community.get("status"),
        "facility_type": record.get("facilityType"),
        "ai_classification": record.get("aiClassification"),
        "announced_date": record.get("announcedDate"),
        "status_history_count": len(history),
        "status_history_json": json.dumps(history) if history else None,
        "source_count": len(record.get("sources") or []),
        "last_updated": record.get("lastUpdated"),
    }


def materialize():
    facilities = fetch_facilities()
    if not facilities:
        raise RuntimeError("Compute Atlas returned no facilities; refusing to replace the table")

    df = pd.DataFrame([flatten(r) for r in facilities])
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "Flattened %d facilities | capacity populated: %d | cooling_type populated: %d | county populated: %d",
        len(df),
        df[["capacity_mw_operational", "capacity_mw_planned"]].notna().any(axis=1).sum(),
        df["cooling_type"].notna().sum(),
        df["county"].notna().sum(),
    )
    return df
