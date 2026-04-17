"""@bruin

name: raw.transit_ntd_annual
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Annual operational and financial metrics from the US National Transit Database (NTD),
  sourced via the Socrata Open Data API on data.transportation.gov.

  Contains per-agency, per-mode annual data including ridership (UPT), vehicle revenue
  miles/hours, fare revenues, operating expenses, and derived efficiency metrics.
  Also includes agency metadata: city, state, UZA, UZA population, agency VOMS.

  Dataset: 2022-2024 NTD Annual Data - Metrics
  Socrata ID: ekg5-frzt
  Source: https://data.transportation.gov/Public-Transit/2022-2024-NTD-Annual-Data-Metrics/ekg5-frzt
  License: Public domain (US Government)

  Supplemented with service area data from:
  Dataset: NTD Annual Data View - Service (by Agency)
  Socrata ID: 6y83-7vuw

materialization:
  type: table
  strategy: create+replace

columns:
  - name: ntd_id
    type: VARCHAR
    description: 5-digit NTD agency identifier
    primary_key: true
  - name: report_year
    type: INTEGER
    description: Reporting year (2022-2024)
    primary_key: true
  - name: mode
    type: VARCHAR
    description: NTD mode code (HR, LR, CR, MB, DR, CB, etc.)
    primary_key: true
  - name: type_of_service
    type: VARCHAR
    description: Type of service (DO=directly operated, PT=purchased transportation)
    primary_key: true
  - name: agency
    type: VARCHAR
    description: Transit agency name
  - name: city
    type: VARCHAR
    description: Agency city
  - name: state
    type: VARCHAR
    description: Two-letter US state code
  - name: mode_name
    type: VARCHAR
    description: Human-readable mode name
  - name: uace_code
    type: VARCHAR
    description: Census Urbanized Area Code
  - name: uza_name
    type: VARCHAR
    description: Urbanized Area name
  - name: primary_uza_population
    type: INTEGER
    description: Population of the primary Urbanized Area served
  - name: agency_voms
    type: INTEGER
    description: Total vehicles operated in maximum service across all modes for this agency
  - name: mode_voms
    type: INTEGER
    description: Vehicles operated in maximum service for this specific mode
  - name: unlinked_passenger_trips
    type: INTEGER
    description: Annual Unlinked Passenger Trips (boardings)
  - name: vehicle_revenue_miles
    type: INTEGER
    description: Annual Vehicle Revenue Miles
  - name: vehicle_revenue_hours
    type: INTEGER
    description: Annual Vehicle Revenue Hours
  - name: passenger_miles
    type: INTEGER
    description: Annual passenger miles traveled
  - name: fare_revenues_earned
    type: INTEGER
    description: Annual fare revenues earned in dollars
  - name: total_operating_expenses
    type: INTEGER
    description: Annual total operating expenses in dollars
  - name: cost_per_hour
    type: DOUBLE
    description: Operating cost per vehicle revenue hour (dollars)
  - name: cost_per_passenger
    type: DOUBLE
    description: Operating cost per unlinked passenger trip (dollars)
  - name: cost_per_passenger_mile
    type: DOUBLE
    description: Operating cost per passenger mile (dollars)
  - name: passengers_per_hour
    type: DOUBLE
    description: Unlinked passenger trips per vehicle revenue hour
  - name: fare_recovery_ratio
    type: DOUBLE
    description: Fare revenues earned divided by total operating expenses
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this batch was fetched

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

METRICS_URL = "https://data.transportation.gov/resource/ekg5-frzt.json"
PAGE_SIZE = 50000


def fetch_paginated(url: str, label: str) -> list[dict]:
    """Fetch all rows from a Socrata endpoint with pagination."""
    all_rows = []
    offset = 0

    while True:
        params = {"$limit": PAGE_SIZE, "$offset": offset, "$order": "ntd_id,report_year,mode"}

        for attempt in range(5):
            try:
                resp = requests.get(url, params=params, timeout=120)
            except requests.RequestException as e:
                wait = 10 * (attempt + 1)
                logger.warning("[%s] Network error at offset %d, retrying in %ds: %s", label, offset, wait, e)
                time.sleep(wait)
                continue

            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning("[%s] Rate limited, backing off %ds", label, wait)
                time.sleep(wait)
                continue

            if resp.status_code in (500, 502, 503):
                wait = 15 * (attempt + 1)
                logger.warning("[%s] Server error %d, retrying in %ds", label, resp.status_code, wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            break
        else:
            logger.error("[%s] Failed to fetch offset %d after 5 attempts", label, offset)
            break

        page = resp.json()
        if not page:
            break

        all_rows.extend(page)
        logger.info("[%s] Fetched %d rows (offset %d, total: %d)", label, len(page), offset, len(all_rows))

        if len(page) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(0.5)

    return all_rows


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2022-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2026-12-31")

    logger.info("Interval: %s to %s", start_date, end_date)

    # Fetch annual metrics (by mode)
    logger.info("Fetching NTD Annual Metrics...")
    metrics_rows = fetch_paginated(METRICS_URL, "metrics")

    if not metrics_rows:
        raise RuntimeError("No data fetched from NTD Annual Metrics — check connectivity")

    df = pd.DataFrame(metrics_rows)

    # Convert numeric columns
    int_cols = [
        "report_year", "primary_uza_population", "agency_voms", "mode_voms",
        "unlinked_passenger_trips", "vehicle_revenue_miles", "vehicle_revenue_hours",
        "passenger_miles", "fare_revenues_earned", "total_operating_expenses",
    ]
    float_cols = [
        "cost_per_hour", "cost_per_passenger", "cost_per_passenger_mile",
        "passengers_per_hour", "fare_revenues_per_total",
    ]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Compute fare recovery ratio
    df["fare_recovery_ratio"] = pd.to_numeric(df.get("fare_revenues_per_total", 0), errors="coerce")

    # Select columns
    keep_cols = [
        "ntd_id", "report_year", "mode", "type_of_service",
        "agency", "city", "state", "mode_name",
        "uace_code", "uza_name", "primary_uza_population", "agency_voms", "mode_voms",
        "unlinked_passenger_trips", "vehicle_revenue_miles", "vehicle_revenue_hours",
        "passenger_miles", "fare_revenues_earned", "total_operating_expenses",
        "cost_per_hour", "cost_per_passenger", "cost_per_passenger_mile",
        "passengers_per_hour", "fare_recovery_ratio",
    ]
    df = df[[c for c in keep_cols if c in df.columns]]

    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Total: %d rows across years %s", len(df), sorted(df["report_year"].dropna().unique()))
    logger.info("Unique agencies: %d", df["ntd_id"].nunique())

    return df
