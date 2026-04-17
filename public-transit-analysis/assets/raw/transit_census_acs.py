"""@bruin

name: raw.transit_census_acs
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  US Census American Community Survey (ACS) 1-Year estimates for commuting
  mode share by Metropolitan Statistical Area (MSA).

  Fetches Table B08301 (Means of Transportation to Work) for all MSAs,
  covering total workers, public transit commuters, walkers, and work-from-home
  workers. Data spans 2010-2023 (1-year ACS estimates).

  Used to calculate transit mode share (% of commuters using transit) and
  work-from-home rates by metro area for correlation with NTD ridership recovery.

  Source: US Census Bureau ACS 1-Year API
  API: https://api.census.gov/data/{year}/acs/acs1
  License: Public domain (US Government)
  No API key required for basic access.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: msa_code
    type: VARCHAR
    description: FIPS code for Metropolitan/Micropolitan Statistical Area
    primary_key: true
  - name: year
    type: INTEGER
    description: Survey year (2010-2023)
    primary_key: true
  - name: msa_name
    type: VARCHAR
    description: Metropolitan Statistical Area name (e.g. New York-Newark-Jersey City, NY-NJ-PA Metro Area)
  - name: total_workers
    type: INTEGER
    description: Total workers 16 years and over (B08301_001E)
  - name: transit_commuters
    type: INTEGER
    description: Workers who commute by public transportation excluding taxicab (B08301_010E)
  - name: walked
    type: INTEGER
    description: Workers who walked to work (B08301_019E)
  - name: worked_from_home
    type: INTEGER
    description: Workers who worked from home (B08301_021E)
  - name: transit_mode_share_pct
    type: DOUBLE
    description: Percentage of workers commuting by public transit (transit_commuters / total_workers * 100)
  - name: wfh_rate_pct
    type: DOUBLE
    description: Percentage of workers working from home (worked_from_home / total_workers * 100)
  - name: walk_share_pct
    type: DOUBLE
    description: Percentage of workers walking to work (walked / total_workers * 100)
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

ACS_BASE = "https://api.census.gov/data/{year}/acs/acs1"
FIELDS = "NAME,B08301_001E,B08301_010E,B08301_019E,B08301_021E"
GEO = "metropolitan statistical area/micropolitan statistical area:*"


def fetch_year(year: int) -> list[dict]:
    """Fetch ACS B08301 for all MSAs for a single year."""
    url = ACS_BASE.format(year=year)
    params = {"get": FIELDS, "for": GEO}

    api_key = os.environ.get("CENSUS_API_KEY")
    if api_key:
        params["key"] = api_key

    for attempt in range(5):
        try:
            resp = requests.get(url, params=params, timeout=60)
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            logger.warning("Network error for year %d, retrying in %ds: %s", year, wait, e)
            time.sleep(wait)
            continue

        if resp.status_code == 204:
            logger.info("No data available for year %d", year)
            return []

        if resp.status_code in (404, 400):
            logger.warning("Year %d not available (HTTP %d)", year, resp.status_code)
            return []

        if resp.status_code == 429:
            wait = 30 * (attempt + 1)
            logger.warning("Rate limited for year %d, backing off %ds", year, wait)
            time.sleep(wait)
            continue

        if resp.status_code in (500, 502, 503):
            wait = 15 * (attempt + 1)
            logger.warning("Server error %d for year %d, retrying in %ds", resp.status_code, year, wait)
            time.sleep(wait)
            continue

        resp.raise_for_status()
        break
    else:
        logger.error("Failed to fetch year %d after 5 attempts", year)
        return []

    data = resp.json()
    if not data or len(data) < 2:
        return []

    # First row is header, rest is data
    header = data[0]
    rows = []
    for row in data[1:]:
        record = dict(zip(header, row))
        try:
            total = int(record.get("B08301_001E") or 0)
            transit = int(record.get("B08301_010E") or 0)
            walked = int(record.get("B08301_019E") or 0)
            wfh = int(record.get("B08301_021E") or 0)
        except (ValueError, TypeError):
            continue

        if total <= 0:
            continue

        rows.append({
            "msa_code": record.get("metropolitan statistical area/micropolitan statistical area", ""),
            "year": year,
            "msa_name": record.get("NAME", ""),
            "total_workers": total,
            "transit_commuters": transit,
            "walked": walked,
            "worked_from_home": wfh,
            "transit_mode_share_pct": round(transit / total * 100, 2),
            "wfh_rate_pct": round(wfh / total * 100, 2),
            "walk_share_pct": round(walked / total * 100, 2),
        })

    return rows


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2010-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2023-12-31")

    start_year = max(int(start_date[:4]), 2010)
    end_year = min(int(end_date[:4]), 2023)

    logger.info("Fetching Census ACS B08301 for years %d-%d", start_year, end_year)

    all_rows = []
    for year in range(start_year, end_year + 1):
        logger.info("Fetching year %d...", year)
        rows = fetch_year(year)
        all_rows.extend(rows)
        logger.info("  Year %d: %d MSAs", year, len(rows))
        time.sleep(1.0)

    if not all_rows:
        raise RuntimeError("No data fetched from Census ACS — check connectivity")

    df = pd.DataFrame(all_rows)
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Total: %d rows across %d years, %d unique MSAs",
                len(df), df["year"].nunique(), df["msa_code"].nunique())

    return df
