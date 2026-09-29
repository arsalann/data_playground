"""@bruin
name: raw.edc_county_reference
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  US county reference table: FIPS code, name, state, land area and internal
  point coordinates. Used to resolve the free-form county names published by
  the data centre inventories to FIPS codes so they can join to drought,
  emissions and demographic data.

  Source: US Census Bureau 2024 national county gazetteer file
  https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/
  License: public domain

materialization:
  type: table
  strategy: create+replace

columns:
  - name: county_fips
    type: VARCHAR
    description: Five-digit county FIPS code (GEOID)
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: county_name
    type: VARCHAR
    description: Full county name as published, e.g. "Loudoun County"
  - name: county_name_norm
    type: VARCHAR
    description: Lowercase name with the County/Parish/Borough suffix and punctuation stripped, for joining
  - name: state
    type: VARCHAR
    description: Two-letter state code
  - name: land_area_sqkm
    type: DOUBLE
    description: County land area in square kilometres
  - name: lat
    type: DOUBLE
    description: Latitude of the county internal point in WGS84
  - name: lon
    type: DOUBLE
    description: Longitude of the county internal point in WGS84
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this snapshot was pulled

@bruin"""

import io
import logging
import os
import re
import zipfile
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

GAZETTEER_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
    "2024_Gazetteer/2024_Gaz_counties_national.zip"
)
USER_AGENT = "bruin-data-playground/1.0 (research; environment-data-centres pipeline)"
TIMEOUT = 120

# "city" is deliberately NOT stripped. Virginia, Maryland and Missouri have
# independent cities whose names duplicate a neighbouring county (Richmond,
# Baltimore, St. Louis, Fairfax, Franklin, Roanoke). Keeping the "city" suffix
# means a bare inventory name like "Richmond" resolves to the county, which is
# the more common case, instead of fanning out to two FIPS codes.
SUFFIXES = (
    "county", "parish", "borough", "census area", "city and borough",
    "municipality", "municipio",
)


def normalise(name: str) -> str:
    """Strip the county-type suffix and punctuation so inventory names can join."""
    text = str(name).lower().strip()
    text = re.sub(r"[.'`]", "", text)
    for suffix in sorted(SUFFIXES, key=len, reverse=True):
        if text.endswith(" " + suffix):
            text = text[: -(len(suffix) + 1)]
            break
    return re.sub(r"\s+", " ", text).strip()


def materialize():
    logger.info("Downloading Census county gazetteer")
    response = requests.get(GAZETTEER_URL, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        name = archive.namelist()[0]
        with archive.open(name) as handle:
            df = pd.read_csv(handle, sep="\t", dtype=str, encoding="latin-1")

    df.columns = [c.strip() for c in df.columns]

    out = pd.DataFrame(
        {
            "county_fips": df["GEOID"].str.zfill(5),
            "county_name": df["NAME"].str.strip(),
            "state": df["USPS"].str.strip(),
            "land_area_sqkm": pd.to_numeric(df["ALAND_SQMI"], errors="coerce") * 2.589988,
            "lat": pd.to_numeric(df["INTPTLAT"], errors="coerce"),
            "lon": pd.to_numeric(df["INTPTLONG"], errors="coerce"),
        }
    )
    out["county_name_norm"] = out["county_name"].map(normalise)
    out["extracted_at"] = datetime.now(timezone.utc)

    logger.info("County reference: %d counties across %d states", len(out), out["state"].nunique())
    return out
