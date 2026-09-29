"""@bruin
name: raw.edc_egrid_subregion_rates
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  EPA eGRID subregion-level output emission rates and generation mix.
  EPA recommends the subregion output rate for footprinting electricity
  consumption at a given location, which is what hypothesis H1 needs.

  Source: EPA eGRID2023 (data year 2023), file egrid2023_data_rev2.xlsx,
  released June 2025 as a revision of the January 2025 edition.
  https://www.epa.gov/egrid/download-data
  License: public domain

  Sheet SRL23. These are ANNUAL AVERAGE attributional rates. They answer
  "what is the average emissions intensity of this grid region", NOT
  "what would additional load emit here" - see NREL Cambium for marginal rates.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: subregion_code
    type: VARCHAR
    description: eGRID subregion acronym, e.g. SRVC, ERCT, RFCE
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: subregion_name
    type: VARCHAR
    description: Full eGRID subregion name
  - name: data_year
    type: INTEGER
    description: eGRID data year
  - name: net_generation_mwh
    type: DOUBLE
    description: Subregion annual net generation in megawatt-hours
  - name: co2e_rate_lb_per_mwh
    type: DOUBLE
    description: Annual total output CO2-equivalent emission rate in pounds per MWh (SRC2ERTA)
  - name: co2_rate_lb_per_mwh
    type: DOUBLE
    description: Annual total output CO2 emission rate in pounds per MWh (SRCO2RTA)
  - name: nox_rate_lb_per_mwh
    type: DOUBLE
    description: Annual total output NOx emission rate in pounds per MWh (SRNOXRTA)
  - name: so2_rate_lb_per_mwh
    type: DOUBLE
    description: Annual total output SO2 emission rate in pounds per MWh (SRSO2RTA)
  - name: co2e_rate_g_per_kwh
    type: DOUBLE
    description: CO2-equivalent rate converted to grams per kWh for comparability
  - name: coal_generation_pct
    type: DOUBLE
    description: Percentage of subregion generation from coal (SRCLPR)
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
SHEET = "SRL23"
USER_AGENT = "bruin-data-playground/1.0 (research; environment-data-centres pipeline)"
TIMEOUT = 300

# 1 lb/MWh = 453.59237 g / 1000 kWh
LB_PER_MWH_TO_G_PER_KWH = 453.59237 / 1000.0

COLUMN_MAP = {
    "SUBRGN": "subregion_code",
    "SRNAME": "subregion_name",
    "YEAR": "data_year",
    "SRNGENAN": "net_generation_mwh",
    "SRC2ERTA": "co2e_rate_lb_per_mwh",
    "SRCO2RTA": "co2_rate_lb_per_mwh",
    "SRNOXRTA": "nox_rate_lb_per_mwh",
    "SRSO2RTA": "so2_rate_lb_per_mwh",
    "SRCLPR": "coal_generation_pct",
}


def materialize():
    logger.info("Downloading eGRID workbook from %s", EGRID_URL)
    response = requests.get(EGRID_URL, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    response.raise_for_status()
    logger.info("Downloaded %.1f MB", len(response.content) / 1e6)

    # Row 1 is the long descriptive header; row 2 holds the short field names.
    df = pd.read_excel(io.BytesIO(response.content), sheet_name=SHEET, header=1)

    missing = [c for c in COLUMN_MAP if c not in df.columns]
    if missing:
        raise RuntimeError(f"eGRID sheet {SHEET} is missing expected columns: {missing}")

    df = df[list(COLUMN_MAP)].rename(columns=COLUMN_MAP)
    df = df[df["subregion_code"].notna()].copy()

    numeric = [c for c in df.columns if c not in ("subregion_code", "subregion_name")]
    for column in numeric:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df["data_year"] = df["data_year"].astype("Int64")
    df["co2e_rate_g_per_kwh"] = df["co2e_rate_lb_per_mwh"] * LB_PER_MWH_TO_G_PER_KWH
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "eGRID: %d subregions | CO2e g/kWh range %.0f to %.0f",
        len(df),
        df["co2e_rate_g_per_kwh"].min(),
        df["co2e_rate_g_per_kwh"].max(),
    )
    return df
