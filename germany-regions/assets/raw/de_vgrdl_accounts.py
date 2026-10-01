"""@bruin
name: raw.de_vgrdl_accounts
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Regional accounts of the German states from the Arbeitskreis "Volkswirtschaftliche
  Gesamtrechnungen der Laender" (VGRdL), downloaded as the official Excel releases from
  https://www.statistikportal.de/de/vgrdl.

  - GDP at current prices per inhabitant (sheet 3.3 of Reihe 1 Band 1,
    `vgrdl_r1b1_bs2025.xlsx`, Berechnungsstand August 2025 / February 2026, years 1991-2025).
  - Disposable income of private households (incl. NPISH) per inhabitant
    (`vgrdl_arbtab_pekvek_phh_bs2025.xlsx`, sheet "Verf. Eink. je Einwohner", years 1991-2024).

  The release URLs are versioned by publication; update WORKBOOKS when a new release
  appears. One row per region x year x indicator, including Germany and the
  East/West aggregates published by VGRdL.
  License: dl-de/by-2-0, source: Arbeitskreis VGR der Laender.

materialization:
  type: table
  strategy: append

columns:
  - name: indicator_code
    type: VARCHAR
    description: gdp_per_capita or disposable_income_per_capita.
    primary_key: true
  - name: region_name
    type: VARCHAR
    description: Region column header as published (state name, Deutschland, or East/West aggregate), line breaks removed.
    primary_key: true
  - name: year
    type: INTEGER
    description: Reference year.
    primary_key: true
  - name: value
    type: DOUBLE
    description: Value in EUR per inhabitant, current prices.
  - name: unit
    type: VARCHAR
    description: Unit of the value (EUR per inhabitant).
  - name: source_file
    type: VARCHAR
    description: URL of the Excel release the value was read from.
  - name: source_sheet
    type: VARCHAR
    description: Worksheet name inside the release.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp of ingestion.

@bruin"""

import io
import logging
import os
import re
import time
from datetime import datetime, timezone

import openpyxl
import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

BASE = "https://www.statistikportal.de/sites/default/files"
WORKBOOKS = [
    {
        "indicator_code": "gdp_per_capita",
        "url": f"{BASE}/2026-03/vgrdl_r1b1_bs2025.xlsx",
        "sheet": "3.3",
    },
    {
        "indicator_code": "disposable_income_per_capita",
        "url": f"{BASE}/2026-06/vgrdl_arbtab_pekvek_phh_bs2025.xlsx",
        "sheet": "Verf. Eink. je Einwohner",
    },
]
MAX_RETRIES = 5


def download(url: str) -> bytes:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=180)
            if r.status_code in (429, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.content
        except requests.RequestException as e:
            wait = 2 ** attempt
            logger.warning("Download failed (%s), attempt %d; retry in %ds", e, attempt, wait)
            time.sleep(wait)
    raise RuntimeError(f"Could not download {url}")


def clean_header(value) -> str:
    text = str(value or "").replace("_x000D_", "")
    text = re.sub(r"-\s*\n\s*(?=[a-zäöüß])", "", text)  # soft break: "Nieder-\nsachsen" -> "Niedersachsen"
    text = re.sub(r"-\s*\n\s*", "-", text)  # real hyphen: "Baden-\nWuerttemberg" -> "Baden-Wuerttemberg"
    return re.sub(r"\s+", " ", text).strip()


def parse_levels(content: bytes, sheet: str) -> list[tuple[str, int, float]]:
    """Read the first block of the sheet (levels in EUR); stop at the growth-rate block."""
    ws = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)[sheet]
    header, out = None, []
    for row in ws.iter_rows(values_only=True):
        if row and row[0] == "Jahr":
            header = [clean_header(c) for c in row]
            continue
        if header is None:
            continue
        if row[0] is None and any(isinstance(c, str) and "Veränderung" in c for c in row if c):
            break
        if isinstance(row[0], int):
            for name, value in zip(header[1:], row[1:]):
                if name and isinstance(value, (int, float)):
                    out.append((name, row[0], float(value)))
    return out


def materialize():
    frames = []
    for wb in WORKBOOKS:
        logger.info("Downloading %s", wb["url"])
        rows = parse_levels(download(wb["url"]), wb["sheet"])
        df = pd.DataFrame(rows, columns=["region_name", "year", "value"])
        df["indicator_code"] = wb["indicator_code"]
        df["unit"] = "EUR per inhabitant"
        df["source_file"] = wb["url"]
        df["source_sheet"] = wb["sheet"]
        logger.info(
            "%s: %d rows, %d regions, years %d-%d",
            wb["indicator_code"], len(df), df["region_name"].nunique(), df["year"].min(), df["year"].max(),
        )
        frames.append(df)
        time.sleep(0.5)

    out = pd.concat(frames, ignore_index=True)
    out["extracted_at"] = datetime.now(timezone.utc)
    return out[["indicator_code", "region_name", "year", "value", "unit", "source_file", "source_sheet", "extracted_at"]]
