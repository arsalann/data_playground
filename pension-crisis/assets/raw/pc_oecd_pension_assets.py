"""@bruin

name: raw.pc_oecd_pension_assets
description: |
  OECD pension fund assets as percentage of GDP.

  Source: OECD Global Pension Statistics (GPS). Data Explorer path:
  "Pensions" → "Funded and private pensions" → "Assets by type of plan and financing
  vehicle". See: https://data-explorer.oecd.org/

  Why this indicator: measures how well-funded each country's private pension system
  is, relative to its economy. Large funded assets → lower fiscal risk from demographic
  shock; small funded assets → higher risk. OECD GPS uses harmonized definitions so
  values are comparable across countries.

  The SDMX CSV endpoint URL can be overridden via OECD_PENSION_ASSETS_URL.
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
  - name: year
    type: INTEGER
    description: Observation year.
    primary_key: true
  - name: vehicle_type
    type: VARCHAR
    description: Financing vehicle — "_T" all vehicles combined, "PF" pension funds, "PIC" insurance contracts, "BANK", "INV", "OTH".
    primary_key: true
  - name: assets_pct_gdp
    type: DOUBLE
    description: Pension-type assets as percentage of GDP (UNIT_MEASURE=PT_B1GQ).
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
    "OECD_PENSION_ASSETS_URL",
    "https://sdmx.oecd.org/public/rest/data/OECD.DAF.CM,DSD_FP@DF_PA,1.0/all?format=csvfilewithlabels&dimensionAtObservation=AllDimensions",
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
        "Endpoint may have changed. Set OECD_PENSION_ASSETS_URL env var to a working "
        "OECD Data Explorer CSV export URL for pension fund assets %GDP."
    )


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Raw columns: %s", list(df.columns))

    ref_area = next((c for c in df.columns if c.upper() in ("REF_AREA", "LOCATION")), None)
    time_col = next((c for c in df.columns if c.upper() in ("TIME_PERIOD", "TIME")), None)
    obs_col = next((c for c in df.columns if c.upper() == "OBS_VALUE"), None)
    unit_col = next((c for c in df.columns if c.upper() == "UNIT_MEASURE"), None)
    vehicle_col = next((c for c in df.columns if c.upper() == "VEHICLE_TYPE"), None)

    if not all((ref_area, time_col, obs_col, unit_col, vehicle_col)):
        raise RuntimeError(f"Unexpected OECD CSV shape, columns were: {list(df.columns)}")

    # DSD_FP@DF_PA contains multiple units; PT_B1GQ = % of GDP (B1GQ = gross domestic product).
    df = df[df[unit_col].astype(str).str.upper() == "PT_B1GQ"].copy()
    logger.info("Filtered to %d PT_B1GQ rows", len(df))

    out = pd.DataFrame({
        "iso3_code": df[ref_area].astype(str),
        "year": pd.to_numeric(df[time_col], errors="coerce").astype("Int64"),
        "vehicle_type": df[vehicle_col].astype(str),
        "assets_pct_gdp": pd.to_numeric(df[obs_col], errors="coerce"),
    })
    out = out.dropna(subset=["year", "assets_pct_gdp"]).copy()
    out["year"] = out["year"].astype(int)
    return out


def materialize():
    df = fetch_sdmx_csv(SDMX_URL)
    out = normalise(df)
    out["extracted_at"] = datetime.now(timezone.utc)
    logger.info(
        "Emitting %d rows, %d countries",
        len(out), out["iso3_code"].nunique(),
    )
    return out
