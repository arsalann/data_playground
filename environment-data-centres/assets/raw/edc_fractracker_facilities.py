"""@bruin
name: raw.edc_fractracker_facilities
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  US data centre facilities from the FracTracker Alliance US Data Centers Tracker.
  Compiled from permits, parcel data, FOIA responses and media monitoring, and
  covers proposed and cancelled facilities as well as operating ones.

  Source: https://fractracker.org/data-centers/
  Endpoint: ArcGIS FeatureServer data_centers_v4_agol_all, paginated query
  License: free for non-commercial use with credit to FracTracker Alliance

  FracTracker is an environmental advocacy organisation. Treat reported MW
  figures as claims to cross-check, not as measurements.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: facility_id
    type: VARCHAR
    description: FracTracker facility identifier
    primary_key: true
    checks:
      - name: not_null
  - name: facility_name
    type: VARCHAR
    description: Facility name as published
  - name: operator_name
    type: VARCHAR
    description: Operating company
  - name: tenant
    type: VARCHAR
    description: Named tenant where different from the operator
  - name: status
    type: VARCHAR
    description: "Lifecycle status as published by FracTracker"
  - name: lat
    type: DOUBLE
    description: Latitude in WGS84 decimal degrees
  - name: lon
    type: DOUBLE
    description: Longitude in WGS84 decimal degrees
  - name: location_confidence
    type: VARCHAR
    description: FracTracker's own confidence rating for the coordinates
  - name: city
    type: VARCHAR
    description: City name
  - name: county
    type: VARCHAR
    description: County name as published (not FIPS)
  - name: state
    type: VARCHAR
    description: US state as published
  - name: mw_low
    type: DOUBLE
    description: Low end of the reported power demand range in megawatts
  - name: mw_high
    type: DOUBLE
    description: High end of the reported power demand range in megawatts
  - name: power_source
    type: VARCHAR
    description: Reported power sourcing arrangement
  - name: dedicated_power_plant
    type: VARCHAR
    description: Whether a dedicated generating plant serves the site
  - name: number_of_generators
    type: VARCHAR
    description: Reported backup generator count, free-form upstream
  - name: number_of_buildings
    type: VARCHAR
    description: Reported building count, free-form upstream
  - name: cooling_source
    type: VARCHAR
    description: Reported cooling water source
  - name: cooling_type
    type: VARCHAR
    description: Reported cooling technology (air, water, closed loop, open loop)
  - name: facility_size_sqft
    type: DOUBLE
    description: Facility floor area in square feet
  - name: property_size_acres
    type: VARCHAR
    description: Property area in acres, free-form upstream
  - name: community_pushback
    type: VARCHAR
    description: Whether organised community opposition is recorded
  - name: resistance_status
    type: VARCHAR
    description: Status of local opposition efforts
  - name: nda
    type: VARCHAR
    description: Whether local officials signed a non-disclosure agreement
  - name: expected_date_online
    type: VARCHAR
    description: Expected in-service date as published
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this snapshot was pulled

@bruin"""

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

LAYER_URL = (
    "https://services.arcgis.com/jDGuO8tYggdCCnUJ/arcgis/rest/services/"
    "data_centers_v4_agol_all/FeatureServer/0"
)
USER_AGENT = "bruin-data-playground/1.0 (research; environment-data-centres pipeline)"
PAGE_SIZE = 1000
TIMEOUT = 120

FIELDS = [
    "facility_id", "facility_name", "operator_name", "tenant", "status",
    "lat", "long", "location_confidence", "city", "county", "state",
    "mw_low", "mw_high", "power_source", "dedicated_power_plant",
    "number_of_generators", "number_of_buildings", "cooling_source", "cooling_type",
    "facility_size_sqft", "property_size_acres", "community_pushback",
    "resistance_status", "nda", "expected_date_online",
]


def _num(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fetch_page(offset: int) -> list:
    params = {
        "where": "1=1",
        "outFields": ",".join(FIELDS),
        "returnGeometry": "false",
        "resultOffset": offset,
        "resultRecordCount": PAGE_SIZE,
        "f": "json",
    }
    response = requests.get(
        f"{LAYER_URL}/query", params=params, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT
    )
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise RuntimeError(f"ArcGIS query failed: {payload['error']}")
    return [f.get("attributes", {}) for f in payload.get("features", [])]


def materialize():
    logger.info("Fetching FracTracker data centre layer from %s", LAYER_URL)

    records, offset = [], 0
    while True:
        page = fetch_page(offset)
        if not page:
            break
        records.extend(page)
        logger.info("Fetched %d records (offset %d)", len(records), offset)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    if not records:
        raise RuntimeError("FracTracker returned no features; refusing to replace the table")

    df = pd.DataFrame(records)
    df = df.rename(columns={"long": "lon"})

    for column in ["lat", "lon", "mw_low", "mw_high", "facility_size_sqft"]:
        df[column] = df[column].map(_num) if column in df.columns else None

    for column in [c if c != "long" else "lon" for c in FIELDS]:
        if column not in df.columns:
            df[column] = None

    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "FracTracker: %d facilities | cooling_type populated: %d | mw_high populated: %d",
        len(df),
        df["cooling_type"].notna().sum(),
        df["mw_high"].notna().sum(),
    )
    return df
