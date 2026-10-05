"""@bruin
name: raw.edc_egrid_plants
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  EPA eGRID plant-level file, used to assign each data centre to an eGRID
  subregion. EPA does not publish a county-to-subregion crosswalk, so this
  asset provides the plant coordinates from which one is derived in staging
  (nearest generating plant, weighted by generation).

  Source: EPA eGRID2023 (data year 2023), file egrid2023_data_rev2.xlsx,
  sheet PLNT23. https://www.epa.gov/egrid/download-data
  License: public domain

materialization:
  type: table
  strategy: create+replace

columns:
  - name: orispl
    type: VARCHAR
    description: DOE/EIA ORIS plant identifier
    primary_key: true
    checks:
      - name: not_null
  - name: plant_name
    type: VARCHAR
    description: Plant name
  - name: state
    type: VARCHAR
    description: Two-letter state abbreviation
  - name: county_name
    type: VARCHAR
    description: County name as published by eGRID
  - name: county_fips
    type: VARCHAR
    description: Five-digit county FIPS code, state FIPS concatenated with county FIPS
  - name: subregion_code
    type: VARCHAR
    description: eGRID subregion acronym this plant belongs to
  - name: balancing_authority
    type: VARCHAR
    description: Balancing authority code
  - name: lat
    type: DOUBLE
    description: Plant latitude in WGS84 decimal degrees
  - name: lon
    type: DOUBLE
    description: Plant longitude in WGS84 decimal degrees
  - name: net_generation_mwh
    type: DOUBLE
    description: Plant annual net generation in megawatt-hours
  - name: co2e_rate_lb_per_mwh
    type: DOUBLE
    description: Plant annual CO2-equivalent output emission rate in pounds per MWh
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this snapshot was pulled

@bruin"""

import io
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

EGRID_URL = os.environ.get(
    "EDC_EGRID_URL",
    "https://www.epa.gov/system/files/documents/2025-06/egrid2023_data_rev2.xlsx",
)
SHEET = "PLNT23"
USER_AGENT = "bruin-data-playground/1.0 (research; environment-data-centres pipeline)"
TIMEOUT = 300

COLUMN_MAP = {
    "ORISPL": "orispl",
    "PNAME": "plant_name",
    "PSTATABB": "state",
    "CNTYNAME": "county_name",
    "SUBRGN": "subregion_code",
    "BACODE": "balancing_authority",
    "LAT": "lat",
    "LON": "lon",
    "PLNGENAN": "net_generation_mwh",
    "PLC2ERTA": "co2e_rate_lb_per_mwh",
}


def materialize():
    logger.info("Downloading eGRID workbook from %s", EGRID_URL)
    response = requests.get(EGRID_URL, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    response.raise_for_status()

    df = pd.read_excel(io.BytesIO(response.content), sheet_name=SHEET, header=1)

    # PLC2ERTA is present in most vintages; fall back to the CO2-only rate if absent.
    if "PLC2ERTA" not in df.columns and "PLCO2RTA" in df.columns:
        logger.warning("PLC2ERTA missing, falling back to PLCO2RTA")
        COLUMN_MAP["PLCO2RTA"] = COLUMN_MAP.pop("PLC2ERTA")

    fips_columns = [c for c in ("FIPSST", "FIPSCNTY") if c in df.columns]
    missing = [c for c in COLUMN_MAP if c not in df.columns]
    if missing:
        raise RuntimeError(f"eGRID sheet {SHEET} is missing expected columns: {missing}")

    selected = df[list(COLUMN_MAP) + fips_columns].rename(columns=COLUMN_MAP)
    selected = selected[selected["orispl"].notna()].copy()

    if len(fips_columns) == 2:
        state_fips = selected["FIPSST"].astype("Int64").astype(str).str.zfill(2)
        county_fips = selected["FIPSCNTY"].astype("Int64").astype(str).str.zfill(3)
        selected["county_fips"] = state_fips + county_fips
        selected.loc[selected["FIPSST"].isna() | selected["FIPSCNTY"].isna(), "county_fips"] = None
        selected = selected.drop(columns=fips_columns)
    else:
        logger.warning("FIPS columns absent from %s; county_fips will be null", SHEET)
        selected["county_fips"] = None

    selected["orispl"] = selected["orispl"].astype("Int64").astype(str)
    for column in ["lat", "lon", "net_generation_mwh", "co2e_rate_lb_per_mwh"]:
        selected[column] = pd.to_numeric(selected[column], errors="coerce")

    selected["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "eGRID plants: %d rows | with coordinates: %d | with subregion: %d | with FIPS: %d",
        len(selected),
        selected["lat"].notna().sum(),
        selected["subregion_code"].notna().sum(),
        selected["county_fips"].notna().sum(),
    )
    return selected
