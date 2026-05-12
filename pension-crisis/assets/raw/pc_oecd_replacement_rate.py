"""@bruin

name: raw.pc_oecd_replacement_rate
description: |
  OECD net pension replacement rate — the retirement income a full-career worker at
  average earnings can expect from the mandatory pension system, as a percentage of
  their pre-retirement net earnings.

  Source: OECD Pensions at a Glance 2023 (biennial). Dataflow on OECD Data Explorer:
  "Pensions at a Glance" → "Net pension replacement rates". See:
  https://data-explorer.oecd.org/

  Why this indicator: the net replacement rate (vs gross) strips out tax effects so
  cross-country comparisons reflect actual purchasing-power outcomes. Always measured
  for the same hypothetical full-career worker at average earnings — apples-to-apples
  by construction.

  The SDMX CSV endpoint URL can be overridden via OECD_REPLACEMENT_RATE_URL.
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
    description: Reference year of the estimate.
    primary_key: true
  - name: measure_code
    type: VARCHAR
    description: OECD measure code (NPRR50 / NPRR100 / NPRR200 for 50%/100%/200% of average earnings; GPRR* for gross).
    primary_key: true
  - name: optionality
    type: VARCHAR
    description: Scheme coverage — "M" mandatory only, "MV" mandatory + voluntary.
    primary_key: true
  - name: net_replacement_rate
    type: DOUBLE
    description: Net pension replacement rate as percentage of pre-retirement net earnings.
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
    "OECD_REPLACEMENT_RATE_URL",
    "https://sdmx.oecd.org/public/rest/data/OECD.ELS.SPD,DSD_PAG@DF_PRR,1.0/all?format=csvfilewithlabels&dimensionAtObservation=AllDimensions",
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
        "Endpoint may have changed. Set OECD_REPLACEMENT_RATE_URL env var to a working "
        "OECD Data Explorer CSV export URL for net pension replacement rates."
    )


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Raw columns: %s", list(df.columns))

    ref_area = next((c for c in df.columns if c.upper() in ("REF_AREA", "LOCATION")), None)
    sex_col = next((c for c in df.columns if c.upper() == "SEX"), None)
    time_col = next((c for c in df.columns if c.upper() in ("TIME_PERIOD", "TIME")), None)
    obs_col = next((c for c in df.columns if c.upper() == "OBS_VALUE"), None)
    measure_col = next((c for c in df.columns if c.upper() == "MEASURE"), None)
    opt_col = next((c for c in df.columns if c.upper() == "OPTIONALITY"), None)

    if not all((ref_area, time_col, obs_col, measure_col)):
        raise RuntimeError(f"Unexpected OECD CSV shape, columns were: {list(df.columns)}")

    out = pd.DataFrame({
        "iso3_code": df[ref_area].astype(str),
        "sex": df[sex_col].astype(str) if sex_col else "_T",
        "year": pd.to_numeric(df[time_col], errors="coerce").astype("Int64"),
        "measure_code": df[measure_col].astype(str),
        "optionality": df[opt_col].astype(str) if opt_col else "M",
        "net_replacement_rate": pd.to_numeric(df[obs_col], errors="coerce"),
    })
    out = out.dropna(subset=["year", "net_replacement_rate"]).copy()
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
