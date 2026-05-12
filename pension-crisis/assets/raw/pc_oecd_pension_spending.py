"""@bruin

name: raw.pc_oecd_pension_spending
description: |
  OECD public expenditure on pensions as percentage of GDP.

  Source: OECD Social Expenditure Database (SOCX) — category "Old age" + "Survivors".
  Data Explorer path: "Social Protection" → "Social Expenditure (SOCX)" → Aggregated
  indicators. See: https://data-explorer.oecd.org/

  Why this indicator: SOCX applies a single OECD methodology (aggregated at source)
  to every OECD country, so spending shares are directly comparable. Includes old-age
  cash benefits + survivors — i.e. public pension outlays.

  The SDMX CSV endpoint URL can be overridden via OECD_PENSION_SPENDING_URL.
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
  - name: branch
    type: VARCHAR
    description: SOCX branch — "OLD_AGE", "SURVIVORS", or "OLD_AGE_SURV" combined.
    primary_key: true
  - name: spending_pct_gdp
    type: DOUBLE
    description: Public pension expenditure as percentage of GDP.
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
    "OECD_PENSION_SPENDING_URL",
    "https://sdmx.oecd.org/public/rest/data/OECD.ELS.SPD,DSD_PAG@DF_PAG,1.0/all?format=csvfilewithlabels&dimensionAtObservation=AllDimensions",
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
        "Endpoint may have changed. Set OECD_PENSION_SPENDING_URL env var to a working "
        "OECD Data Explorer CSV export URL for SOCX old-age + survivors pension spending."
    )


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Raw columns: %s", list(df.columns))

    ref_area = next((c for c in df.columns if c.upper() in ("REF_AREA", "LOCATION")), None)
    measure_col = next((c for c in df.columns if c.upper() in ("MEASURE", "INDICATOR")), None)
    time_col = next((c for c in df.columns if c.upper() in ("TIME_PERIOD", "TIME")), None)
    obs_col = next((c for c in df.columns if c.upper() == "OBS_VALUE"), None)

    if not all((ref_area, time_col, obs_col, measure_col)):
        raise RuntimeError(f"Unexpected OECD CSV shape, columns were: {list(df.columns)}")

    # DF_PAG is the combined Pensions-at-a-Glance bundle; keep only the public pension
    # expenditure measure (PEP = Public expenditure on pensions as % of GDP).
    df = df[df[measure_col].astype(str).str.upper() == "PEP"].copy()
    logger.info("Filtered to %d PEP rows", len(df))

    out = pd.DataFrame({
        "iso3_code": df[ref_area].astype(str),
        "year": pd.to_numeric(df[time_col], errors="coerce").astype("Int64"),
        "branch": "OLD_AGE_SURV",
        "spending_pct_gdp": pd.to_numeric(df[obs_col], errors="coerce"),
    })
    out = out.dropna(subset=["year", "spending_pct_gdp"]).copy()
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
