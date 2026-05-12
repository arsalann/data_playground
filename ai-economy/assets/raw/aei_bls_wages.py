"""@bruin

name: raw.aei_bls_wages
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  BLS OES (Occupational Employment and Wage Statistics) national-level May 2024 release.
  Filtered to `O_GROUP = 'detailed'` (individual occupations, not major/minor/broad groups).

  BLS uses SOC 2018 6-digit codes (e.g. `15-1252`). Joins to O*NET-SOC by truncating the
  O*NET 8-digit code to 6 digits (`15-1252.00` → `15-1252`).

  Caveats:
    - BLS suppresses small cells with `*` or `#`. Those are parsed as null.
    - Wages are US-only. Any wage-based analysis is inherently a US view.

  Source: https://www.bls.gov/oes/special-requests/oesm24nat.zip (national_M2024_dl.xlsx)
  License: US public domain.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: soc_code
    type: VARCHAR
    description: BLS SOC 2018 6-digit code.
    primary_key: true
  - name: occupation_title
    type: VARCHAR
    description: BLS occupation title.
  - name: total_employment
    type: INTEGER
    description: Estimated total US employment in this occupation (national).
  - name: median_annual_wage
    type: DOUBLE
    description: Annual median wage in USD. Null when BLS suppressed the cell.
  - name: mean_annual_wage
    type: DOUBLE
    description: Annual mean wage in USD. Null when BLS suppressed the cell.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this asset was materialized.

@bruin"""

import io
import logging
import os
import zipfile
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

URL = "https://www.bls.gov/oes/special-requests/oesm24nat.zip"
XLSX_INNER_PATH = "oesm24nat/national_M2024_dl.xlsx"

# BLS Akamai blocks minimal requests; mirror real Chrome.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Ch-Ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}


def parse_bls_number(val):
    """BLS uses '*', '#' for suppressed/excluded values; return None."""
    if pd.isna(val):
        return None
    if isinstance(val, str):
        if val.strip() in ("", "*", "#", "**"):
            return None
        try:
            return float(val.replace(",", ""))
        except ValueError:
            return None
    return float(val)


def materialize():
    logger.info("Downloading %s", URL)
    resp = requests.get(URL, headers=HEADERS, timeout=120)
    resp.raise_for_status()
    logger.info("  %.1f KB", len(resp.content) / 1024)

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open(XLSX_INNER_PATH) as f:
            df = pd.read_excel(f, engine="openpyxl")

    logger.info("Parsed XLSX: %d rows, cols=%s", len(df), list(df.columns)[:10])

    detailed = df[df["O_GROUP"] == "detailed"].copy()
    logger.info("Detailed occupations: %d", len(detailed))

    detailed["total_employment"] = (
        detailed["TOT_EMP"].apply(parse_bls_number).astype("Int64")
    )
    detailed["median_annual_wage"] = detailed["A_MEDIAN"].apply(parse_bls_number)
    detailed["mean_annual_wage"] = detailed["A_MEAN"].apply(parse_bls_number)

    out = detailed[[
        "OCC_CODE",
        "OCC_TITLE",
        "total_employment",
        "median_annual_wage",
        "mean_annual_wage",
    ]].rename(columns={
        "OCC_CODE": "soc_code",
        "OCC_TITLE": "occupation_title",
    })

    out["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "Ready: %d rows, %d distinct SOC, null wage=%d",
        len(out),
        out["soc_code"].nunique(),
        out["median_annual_wage"].isna().sum(),
    )
    return out
