"""@bruin

name: raw.pc_oecd_retirement_age
description: |
  OECD statutory (normal) retirement age for full career, by country and sex.

  Source: OECD Pensions at a Glance 2023 (biennial). Dataflow exposed via OECD Data
  Explorer SDMX-JSON/CSV endpoints at sdmx.oecd.org. See:
  https://data-explorer.oecd.org/ → "Pensions at a Glance" → "Normal retirement age"

  Why this indicator, not "effective" retirement age: the normal (statutory) age is the
  parameter that each government actually legislates, so it is the cleanest cross-country
  comparison. Effective age blends voluntary early/late retirement choices.

  The SDMX CSV endpoint URL can be overridden via OECD_RETIREMENT_AGE_URL. The OECD
  endpoint structure has shifted across 2023/2024 — if the default fails, use OECD Data
  Explorer's "Download CSV" button to copy a fresh URL.

  Data is ingested for all OECD countries without country filtering — staging filters
  to the 38 OECD members via pc_country_dim.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: iso3_code
    type: VARCHAR
    description: ISO 3166-1 alpha-3 country code.
    primary_key: true
  - name: sex
    type: VARCHAR
    description: Sex dimension — "M", "F", or "_T" (total/both).
    primary_key: true
  - name: year
    type: INTEGER
    description: Observation year.
    primary_key: true
  - name: retirement_age
    type: DOUBLE
    description: Normal statutory retirement age in years, assuming full career.
  - name: indicator_code
    type: VARCHAR
    description: OECD indicator code/measure as reported.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this snapshot was ingested.

@bruin"""

import io
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

SDMX_URL = os.environ.get(
    "OECD_RETIREMENT_AGE_URL",
    "https://sdmx.oecd.org/public/rest/data/OECD.ELS.SPD,DSD_PAG@DF_DPS,1.0/all?format=csvfilewithlabels&dimensionAtObservation=AllDimensions",
)


def fetch_sdmx_csv(url: str) -> pd.DataFrame:
    logger.info("Fetching %s", url)
    for attempt in range(4):
        try:
            resp = requests.get(url, timeout=120, headers={"Accept": "text/csv"})
            if resp.status_code == 200:
                logger.info("Received %d bytes", len(resp.content))
                return pd.read_csv(io.BytesIO(resp.content))
            logger.warning("HTTP %d on attempt %d", resp.status_code, attempt + 1)
        except requests.RequestException as e:
            logger.warning("Network error on attempt %d: %s", attempt + 1, e)
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(
        f"OECD SDMX request failed after 4 attempts: {url}\n"
        "Endpoint may have changed. Set OECD_RETIREMENT_AGE_URL env var to a working "
        "OECD Data Explorer CSV export URL for normal retirement age."
    )


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Raw columns: %s", list(df.columns))

    # OECD SDMX CSV column variants: REF_AREA, Reference area, LOCATION, SEX, TIME_PERIOD, OBS_VALUE, MEASURE
    ref_area = next((c for c in df.columns if c.upper() in ("REF_AREA", "LOCATION")), None)
    sex_col = next((c for c in df.columns if c.upper() == "SEX"), None)
    time_col = next((c for c in df.columns if c.upper() in ("TIME_PERIOD", "TIME")), None)
    obs_col = next((c for c in df.columns if c.upper() == "OBS_VALUE"), None)
    measure_col = next((c for c in df.columns if c.upper() in ("MEASURE", "INDICATOR")), None)

    if not all((ref_area, time_col, obs_col)):
        raise RuntimeError(f"Unexpected OECD CSV shape, columns were: {list(df.columns)}")

    out = pd.DataFrame({
        "iso3_code": df[ref_area].astype(str),
        "sex": df[sex_col].astype(str) if sex_col else "_T",
        "year": pd.to_numeric(df[time_col], errors="coerce").astype("Int64"),
        "retirement_age": pd.to_numeric(df[obs_col], errors="coerce"),
        "indicator_code": df[measure_col].astype(str) if measure_col else "NRA",
    })
    out = out.dropna(subset=["year", "retirement_age"]).copy()
    out["year"] = out["year"].astype(int)
    return out


def materialize():
    df = fetch_sdmx_csv(SDMX_URL)
    out = normalise(df)
    out["extracted_at"] = datetime.now(timezone.utc)
    logger.info(
        "Emitting %d rows, %d countries, years %d-%d",
        len(out), out["iso3_code"].nunique(),
        int(out["year"].min()) if len(out) else 0,
        int(out["year"].max()) if len(out) else 0,
    )
    return out
