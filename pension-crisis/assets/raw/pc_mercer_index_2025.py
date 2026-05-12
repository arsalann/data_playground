"""@bruin

name: raw.pc_mercer_index_2025
description: |
  Mercer CFA Institute Global Pension Index 2025 — overall index plus the three
  sub-indices (Adequacy 40%, Sustainability 35%, Integrity 25%) per country.

  Methodology: a single harmonized scoring framework published annually since 2009,
  applied uniformly to 50+ pension systems. This is one of the very few truly
  apples-to-apples cross-country pension quality comparisons available.

  Source: Mercer CFA Institute Global Pension Index (annual), October 2025 edition.
  https://www.mercer.com/insights/investments/market-outlook-and-ideas/mercer-cfa-institute-global-pension-index/

  Ingestion strategy:
  1. If MERCER_GPI_2025_URL is set, fetch CSV from that URL.
  2. Otherwise read the committed seed CSV at assets/raw/seeds/mercer_gpi_2025.csv.

  The seed CSV ships as an empty template. Populate it from the published Mercer 2025
  report before running this asset. Values MUST be transcribed accurately — do not
  approximate.
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
  - name: country_name
    type: VARCHAR
    description: Country name as published in the Mercer report.
  - name: overall_index
    type: DOUBLE
    description: Overall GPI score (0-100).
  - name: adequacy_sub_index
    type: DOUBLE
    description: Adequacy sub-index score (0-100, 40% weight in overall).
  - name: sustainability_sub_index
    type: DOUBLE
    description: Sustainability sub-index score (0-100, 35% weight in overall).
  - name: integrity_sub_index
    type: DOUBLE
    description: Integrity sub-index score (0-100, 25% weight in overall).
  - name: grade
    type: VARCHAR
    description: Letter grade assigned by Mercer (A, B+, B, C+, C, D).
  - name: report_edition
    type: VARCHAR
    description: Edition label, e.g. "2025".
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp of ingestion.

@bruin"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

SEED_PATH = Path(__file__).parent / "seeds" / "mercer_gpi_2025.csv"
URL = os.environ.get("MERCER_GPI_2025_URL", "").strip()

EXPECTED_COLUMNS = [
    "iso3_code",
    "country_name",
    "overall_index",
    "adequacy_sub_index",
    "sustainability_sub_index",
    "integrity_sub_index",
    "grade",
]


def read_from_url(url: str) -> pd.DataFrame:
    logger.info("Fetching Mercer CSV from %s", url)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return pd.read_csv(pd.io.common.BytesIO(resp.content), comment="#")


def read_seed(path: Path) -> pd.DataFrame:
    logger.info("Reading Mercer seed CSV from %s", path)
    if not path.exists():
        raise FileNotFoundError(f"Mercer seed CSV not found at {path}")
    df = pd.read_csv(path, comment="#")
    if df.empty:
        raise RuntimeError(
            f"Mercer seed CSV at {path} is empty. Populate it with 2025 values from "
            "the Mercer CFA GPI 2025 report before running this asset."
        )
    return df


def materialize():
    if URL:
        df = read_from_url(URL)
    else:
        df = read_seed(SEED_PATH)

    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing:
        raise RuntimeError(f"Mercer input missing columns: {missing}")

    df = df[EXPECTED_COLUMNS].copy()
    df["report_edition"] = "2025"
    df["extracted_at"] = datetime.now(timezone.utc)

    for col in ("overall_index", "adequacy_sub_index", "sustainability_sub_index", "integrity_sub_index"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info("Emitting %d Mercer country rows", len(df))
    return df
