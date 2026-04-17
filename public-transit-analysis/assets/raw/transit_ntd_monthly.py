"""@bruin

name: raw.transit_ntd_monthly
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Monthly ridership data from the US National Transit Database (NTD),
  sourced via the Socrata Open Data API on data.transportation.gov.

  Contains monthly Unlinked Passenger Trips (UPT), Vehicle Revenue Miles (VRM),
  Vehicle Revenue Hours (VRH), and Vehicles Operated in Maximum Service (VOMS)
  by agency, mode, and type of service from January 2002 to present.

  Dataset: Complete Monthly Ridership (with Adjustments and Estimates)
  Socrata ID: 8bui-9xvu
  Source: https://data.transportation.gov/Public-Transit/Complete-Monthly-Ridership/8bui-9xvu
  License: Public domain (US Government)

materialization:
  type: table
  strategy: create+replace

columns:
  - name: ntd_id
    type: VARCHAR
    description: 5-digit NTD agency identifier (e.g. 00001 = King County Metro)
    primary_key: true
  - name: agency
    type: VARCHAR
    description: Transit agency name as reported to NTD
  - name: mode
    type: VARCHAR
    description: NTD mode code (HR=heavy rail, LR=light rail, CR=commuter rail, MB=bus, DR=demand response, CB=commuter bus, TB=trolleybus, FB=ferryboat, SR=streetcar, CC=cable car, etc.)
    primary_key: true
  - name: tos
    type: VARCHAR
    description: Type of service code (DO=directly operated, PT=purchased transportation, TX=taxi, TN=transportation network company)
    primary_key: true
  - name: report_month
    type: DATE
    description: First day of the reporting month (e.g. 2019-01-01 for January 2019)
    primary_key: true
  - name: upt
    type: INTEGER
    description: Unlinked Passenger Trips - each boarding counts as one trip, standard US transit ridership metric
  - name: vrm
    type: INTEGER
    description: Vehicle Revenue Miles - miles traveled while in revenue service
  - name: vrh
    type: INTEGER
    description: Vehicle Revenue Hours - hours operated in revenue service
  - name: voms
    type: INTEGER
    description: Vehicles Operated in Maximum Service - peak fleet size
  - name: mode_name
    type: VARCHAR
    description: Human-readable mode name (e.g. Heavy Rail, Bus, Light Rail)
  - name: uza_name
    type: VARCHAR
    description: Urbanized Area name (e.g. New York--Newark, NY--NJ--CT)
  - name: uace_cd
    type: VARCHAR
    description: Census Urbanized Area Code
  - name: state
    type: VARCHAR
    description: Two-letter US state code
  - name: reporter_type
    type: VARCHAR
    description: Reporter type (Full Reporter, Reduced Reporter, etc.)
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this batch was fetched from the Socrata API

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

SOCRATA_BASE = "https://data.transportation.gov/resource/8bui-9xvu.json"
PAGE_SIZE = 50000


def fetch_all_pages() -> list[dict]:
    """Fetch all rows from Socrata API with pagination."""
    all_rows = []
    offset = 0

    while True:
        params = {
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$order": "ntd_id,mode,tos,date",
        }

        for attempt in range(5):
            try:
                resp = requests.get(SOCRATA_BASE, params=params, timeout=120)
            except requests.RequestException as e:
                wait = 10 * (attempt + 1)
                logger.warning("Network error at offset %d, retrying in %ds: %s", offset, wait, e)
                time.sleep(wait)
                continue

            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning("Rate limited at offset %d, backing off %ds", offset, wait)
                time.sleep(wait)
                continue

            if resp.status_code in (500, 502, 503):
                wait = 15 * (attempt + 1)
                logger.warning("Server error %d at offset %d, retrying in %ds", resp.status_code, offset, wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            break
        else:
            logger.error("Failed to fetch offset %d after 5 attempts", offset)
            break

        page = resp.json()
        if not page:
            break

        all_rows.extend(page)
        logger.info("Fetched %d rows (offset %d, total so far: %d)", len(page), offset, len(all_rows))

        if len(page) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(0.5)

    return all_rows


def materialize():
    # For create+replace, we always fetch the complete dataset regardless of date range
    start_date = os.environ.get("BRUIN_START_DATE", "2002-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2026-12-31")

    logger.info("Interval: %s to %s (fetching complete dataset for create+replace)", start_date, end_date)
    logger.info("Fetching NTD Monthly Module from Socrata API...")

    rows = fetch_all_pages()

    if not rows:
        raise RuntimeError("No data fetched from NTD Monthly Module — check connectivity")

    df = pd.DataFrame(rows)

    # Parse date column to DATE type
    df["report_month"] = pd.to_datetime(df["date"]).dt.date

    # Map mode codes to names
    mode_names = {
        "HR": "Heavy Rail", "LR": "Light Rail", "CR": "Commuter Rail",
        "MB": "Bus", "DR": "Demand Response", "CB": "Commuter Bus",
        "TB": "Trolleybus", "FB": "Ferryboat", "SR": "Streetcar Rail",
        "IP": "Inclined Plane", "CC": "Cable Car", "RB": "Bus Rapid Transit",
        "MG": "Monorail/Automated Guideway", "YR": "Hybrid Rail",
        "AR": "Alaska Railroad", "TR": "Aerial Tramway", "VP": "Vanpool",
    }
    df["mode_name"] = df["mode"].map(mode_names).fillna(df.get("_3_mode", "Other"))

    # Convert numeric columns
    for col in ["upt", "vrm", "vrh", "voms"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Select and rename columns
    df = df.rename(columns={})
    keep_cols = [
        "ntd_id", "agency", "mode", "tos", "report_month",
        "upt", "vrm", "vrh", "voms", "mode_name",
        "uza_name", "uace_cd", "state", "reporter_type",
    ]
    df = df[[c for c in keep_cols if c in df.columns]]

    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Total: %d rows, date range: %s to %s", len(df), df["report_month"].min(), df["report_month"].max())
    logger.info("Unique agencies: %d, modes: %s", df["ntd_id"].nunique(), sorted(df["mode"].unique()))

    return df
