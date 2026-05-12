"""@bruin

name: raw.pc_imf_public_debt
description: |
  General government gross debt as percentage of GDP from the IMF World Economic
  Outlook (WEO) via the IMF DataMapper API.

  Source: IMF DataMapper (https://www.imf.org/external/datamapper/api/v1).
  Indicator GGXWDG_NGDP — General government gross debt, % of GDP. The WEO is
  released twice a year (April and October). One methodology applied to every
  country, so values are comparable OECD-wide.

  Why this series (not WB GC.DOD.TOTL.GD.ZS): WB's central-government-debt series
  is sparse for most OECD countries. IMF WEO general-government figures cover
  every OECD-38 country historically plus five-year forecasts.
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
  - name: debt_pct_gdp
    type: DOUBLE
    description: General government gross debt as % of GDP.
  - name: is_forecast
    type: BOOLEAN
    description: True if the value is a WEO forecast rather than a realised observation.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this snapshot was ingested.

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

IMF_URL = os.environ.get(
    "IMF_PUBLIC_DEBT_URL",
    "https://www.imf.org/external/datamapper/api/v1/GGXWDG_NGDP",
)

OECD_ISO3 = {
    "AUS", "AUT", "BEL", "CAN", "CHL", "COL", "CRI", "CZE", "DNK", "EST",
    "FIN", "FRA", "DEU", "GRC", "HUN", "ISL", "IRL", "ISR", "ITA", "JPN",
    "KOR", "LVA", "LTU", "LUX", "MEX", "NLD", "NZL", "NOR", "POL", "PRT",
    "SVK", "SVN", "ESP", "SWE", "CHE", "TUR", "GBR", "USA",
}


def fetch_imf() -> dict:
    for attempt in range(4):
        try:
            resp = requests.get(IMF_URL, timeout=60)
            if resp.status_code == 200:
                logger.info("Received %d bytes", len(resp.content))
                return resp.json()
            logger.warning("HTTP %d attempt %d", resp.status_code, attempt + 1)
        except requests.RequestException as e:
            logger.warning("Network error attempt %d: %s", attempt + 1, e)
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"IMF DataMapper request failed after 4 attempts: {IMF_URL}")


def materialize():
    payload = fetch_imf()
    series = payload.get("values", {}).get("GGXWDG_NGDP", {})
    current_year = datetime.now(timezone.utc).year

    rows = []
    for iso3, years in series.items():
        if iso3 not in OECD_ISO3:
            continue
        for year_str, val in years.items():
            if val is None:
                continue
            year = int(year_str)
            rows.append({
                "iso3_code": iso3,
                "year": year,
                "debt_pct_gdp": float(val),
                "is_forecast": year > current_year,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No OECD rows extracted from IMF payload")
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info(
        "Emitting %d rows, %d countries, years %d-%d",
        len(df), df["iso3_code"].nunique(), df["year"].min(), df["year"].max(),
    )
    return df
