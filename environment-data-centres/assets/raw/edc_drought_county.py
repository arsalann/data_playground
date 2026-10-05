"""@bruin
name: raw.edc_drought_county
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Weekly US Drought Monitor severity statistics for every US county, expressed
  as the percentage of county area in each drought category D0 to D4. Used to
  build the per-county drought-frequency baseline that hypothesis H3 compares
  data centre siting against.

  Sources:
  - US Drought Monitor data services, https://usdmdataservices.unl.edu/
  - County list from the US Census Bureau 2024 national county gazetteer
  License: US Drought Monitor is free to use with attribution to NDMC, USDA and NOAA

  The API has no bulk endpoint, so this asset issues one request per county.
  D-categories are cumulative upstream: d0 is the area at D0 OR WORSE, so
  d0 >= d1 >= d2 >= d3 >= d4. Staging derives exclusive bands from these.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: county_fips
    type: VARCHAR
    description: Five-digit county FIPS code
    primary_key: true
    checks:
      - name: not_null
  - name: map_date
    type: DATE
    description: US Drought Monitor map date (weekly, Tuesdays)
    primary_key: true
  - name: county_name
    type: VARCHAR
    description: County name as published by the US Drought Monitor
  - name: state
    type: VARCHAR
    description: Two-letter state code
  - name: pct_none
    type: DOUBLE
    description: Percentage of county area with no drought designation
  - name: pct_d0_or_worse
    type: DOUBLE
    description: Percentage of county area at D0 (abnormally dry) or worse, cumulative
  - name: pct_d1_or_worse
    type: DOUBLE
    description: Percentage of county area at D1 (moderate drought) or worse, cumulative
  - name: pct_d2_or_worse
    type: DOUBLE
    description: Percentage of county area at D2 (severe drought) or worse, cumulative
  - name: pct_d3_or_worse
    type: DOUBLE
    description: Percentage of county area at D3 (extreme drought) or worse, cumulative
  - name: pct_d4
    type: DOUBLE
    description: Percentage of county area at D4 (exceptional drought)
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this snapshot was pulled

@bruin"""

import io
import logging
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

USDM_URL = (
    "https://usdmdataservices.unl.edu/api/CountyStatistics/"
    "GetDroughtSeverityStatisticsByAreaPercent"
)
GAZETTEER_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
    "2024_Gazetteer/2024_Gaz_counties_national.zip"
)
USER_AGENT = "bruin-data-playground/1.0 (research; environment-data-centres pipeline)"
TIMEOUT = 90

START_DATE = os.environ.get("EDC_DROUGHT_START", "1/1/2015")
END_DATE = os.environ.get("EDC_DROUGHT_END", "12/31/2025")
MAX_WORKERS = int(os.environ.get("EDC_DROUGHT_WORKERS", "8"))
# Set to a small number to smoke-test without pulling all ~3,100 counties.
COUNTY_LIMIT = int(os.environ.get("EDC_COUNTY_LIMIT", "0"))


def fetch_county_list() -> list:
    logger.info("Fetching county FIPS list from the Census gazetteer")
    response = requests.get(GAZETTEER_URL, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        name = archive.namelist()[0]
        with archive.open(name) as handle:
            gazetteer = pd.read_csv(handle, sep="\t", dtype=str, encoding="latin-1")

    gazetteer.columns = [c.strip() for c in gazetteer.columns]
    fips = sorted(gazetteer["GEOID"].str.zfill(5).unique())
    logger.info("Census gazetteer lists %d counties", len(fips))
    return fips


def fetch_county(fips: str) -> list:
    params = {
        "aoi": fips,
        "startdate": START_DATE,
        "enddate": END_DATE,
        "statisticsType": 1,
    }
    try:
        response = requests.get(
            USDM_URL,
            params=params,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        return response.json()
    except Exception as exc:  # noqa: BLE001 - one bad county must not fail the run
        logger.warning("County %s failed: %s", fips, exc)
        return []


def materialize():
    counties = fetch_county_list()
    if COUNTY_LIMIT:
        counties = counties[:COUNTY_LIMIT]
        logger.warning("EDC_COUNTY_LIMIT set: pulling only %d counties", len(counties))

    logger.info(
        "Fetching USDM statistics for %d counties, %s to %s, %d workers",
        len(counties), START_DATE, END_DATE, MAX_WORKERS,
    )

    records = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for index, result in enumerate(pool.map(fetch_county, counties), start=1):
            records.extend(result)
            if index % 250 == 0:
                logger.info("Processed %d/%d counties, %d rows", index, len(counties), len(records))

    if not records:
        raise RuntimeError("US Drought Monitor returned no rows; refusing to replace the table")

    df = pd.DataFrame(records)
    df = df.rename(
        columns={
            "fips": "county_fips",
            "county": "county_name",
            "mapDate": "map_date",
            "none": "pct_none",
            "d0": "pct_d0_or_worse",
            "d1": "pct_d1_or_worse",
            "d2": "pct_d2_or_worse",
            "d3": "pct_d3_or_worse",
            "d4": "pct_d4",
        }
    )

    df["county_fips"] = df["county_fips"].astype(str).str.zfill(5)
    df["map_date"] = pd.to_datetime(df["map_date"]).dt.date
    for column in [
        "pct_none", "pct_d0_or_worse", "pct_d1_or_worse",
        "pct_d2_or_worse", "pct_d3_or_worse", "pct_d4",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df[
        ["county_fips", "map_date", "county_name", "state", "pct_none",
         "pct_d0_or_worse", "pct_d1_or_worse", "pct_d2_or_worse",
         "pct_d3_or_worse", "pct_d4"]
    ].drop_duplicates(subset=["county_fips", "map_date"])

    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "Drought: %d rows | %d counties | %s to %s",
        len(df), df["county_fips"].nunique(), df["map_date"].min(), df["map_date"].max(),
    )
    return df
