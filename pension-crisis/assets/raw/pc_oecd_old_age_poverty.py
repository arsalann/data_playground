"""@bruin

name: raw.pc_oecd_old_age_poverty
description: |
  OECD relative income poverty rate for persons aged over 65, from the OECD Income
  Distribution Database (IDD).

  Source: OECD IDD via SDMX. Dataflow `OECD.WISE.INE,DSD_WISE_IDD@DF_IDD,1.0`.
  See https://data-explorer.oecd.org/ → "Income Distribution Database" → "Poverty rate".

  Why this indicator: Mercer's Global Pension Index gives a "sustainability" sub-index
  that reflects system design, but the IDD poverty rate measures the *outcome* — the
  share of over-65s living below 50% of median equivalised disposable income. Pairing
  design-quality scores with measured poverty surfaces countries where a highly-rated
  system still produces widespread old-age poverty (and vice versa).

  Filter: MEASURE=PR_INC_DISP (poverty rate on disposable income), AGE=Y_GT65,
  METHODO=METH2012 (current OECD methodology), DEFINITION=D_CUR (current income
  definition), POVERTY_LINE=PL_50 (50% of median). One row per country per year.
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
  - name: poverty_rate
    type: DOUBLE
    description: Share of persons aged 65+ below 50% of median equivalised disposable income (%).
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
    "OECD_OLD_AGE_POVERTY_URL",
    "https://sdmx.oecd.org/public/rest/data/OECD.WISE.INE,DSD_WISE_IDD@DF_IDD,1.0"
    "/all?format=csvfilewithlabels&startPeriod=2015",
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
        "Endpoint may have changed. Set OECD_OLD_AGE_POVERTY_URL env var to a working "
        "OECD Data Explorer CSV export URL for IDD poverty rate (PR_INC_DISP, Y_GT65)."
    )


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Raw columns: %s", list(df.columns))

    mask = (
        (df["MEASURE"] == "PR_INC_DISP")
        & (df["AGE"] == "Y_GT65")
        & (df["METHODOLOGY"] == "METH2012")
        & (df["DEFINITION"] == "D_CUR")
        & (df["POVERTY_LINE"] == "PL_50")
    )
    sub = df[mask].copy()
    logger.info("Filtered to %d rows (from %d)", len(sub), len(df))

    out = pd.DataFrame({
        "iso3_code": sub["REF_AREA"].astype(str),
        "year": pd.to_numeric(sub["TIME_PERIOD"], errors="coerce").astype("Int64"),
        "poverty_rate": pd.to_numeric(sub["OBS_VALUE"], errors="coerce"),
    })
    out = out.dropna(subset=["year", "poverty_rate"]).copy()
    out["year"] = out["year"].astype(int)
    out = out.drop_duplicates(subset=["iso3_code", "year"], keep="last")
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
