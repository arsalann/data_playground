"""@bruin

name: raw.aei_claude_usage
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Anthropic Economic Index (AEI) raw telemetry for Claude.ai (Free and Pro) usage,
  2026-01-15 release, covering the 2025-11-13 to 2025-11-20 observation window.

  Long-format CSV keyed by (geo_id, geography, date_start, date_end, facet, variable,
  cluster_name) with a single `value` column. Facets include `onet_task`, `collaboration`,
  `request`, `use_case`, `ai_autonomy`, `human_education_years`, and intersection facets
  of the form `onet_task::<sub_facet>`.

  Geographies: GLOBAL plus countries (ISO 3166-1 alpha-2) and country-states (ISO 3166-2).

  Source: https://huggingface.co/datasets/Anthropic/EconomicIndex
  License: CC BY 4.0
  No authentication required.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: geo_id
    type: VARCHAR
    description: Geography identifier (`GLOBAL`, ISO 3166-1 alpha-2 country code, or ISO 3166-2 country-state code).
    primary_key: true
  - name: geography
    type: VARCHAR
    description: Geography level (`global`, `country`, `country-state`).
  - name: date_start
    type: DATE
    description: Start of the observation window (inclusive). Always 2025-11-13 for this release.
    primary_key: true
  - name: date_end
    type: DATE
    description: End of the observation window (inclusive). Always 2025-11-20 for this release.
  - name: platform_and_product
    type: VARCHAR
    description: Source platform. Constant `Claude AI (Free and Pro)` for this asset.
  - name: facet
    type: VARCHAR
    description: Metric facet (e.g. `onet_task`, `collaboration`, `country`, `onet_task::ai_autonomy`).
    primary_key: true
  - name: level
    type: INTEGER
    description: Clustering depth level inside the facet (0 = top level, higher = more granular).
  - name: variable
    type: VARCHAR
    description: Specific metric (e.g. `onet_task_count`, `onet_task_pct`, `ai_autonomy_mean`).
    primary_key: true
  - name: cluster_name
    type: VARCHAR
    description: |
      Entity name within the facet (e.g. O*NET task description, collaboration type, country code).
      Empty string for facets that do not need a cluster dimension.
    primary_key: true
  - name: value
    type: DOUBLE
    description: Numeric value for this (geo, facet, variable, cluster) combination.
  - name: release_id
    type: VARCHAR
    description: AEI release identifier (constant `release_2026_01_15`).
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this asset was materialized.

@bruin"""

import logging
import os
import tempfile
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

RELEASE_ID = "release_2026_01_15"
URL = (
    "https://huggingface.co/datasets/Anthropic/EconomicIndex/resolve/main/"
    f"{RELEASE_ID}/data/intermediate/aei_raw_claude_ai_2025-11-13_to_2025-11-20.csv"
)

# Browser-like headers — HuggingFace accepts any UA but we mirror the BLS request style
# for consistency across raw assets.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
}


def stream_download(url: str, dest: str) -> int:
    """Download the CSV to a temp file, returning total byte count."""
    logger.info("Streaming %s", url)
    total = 0
    with requests.get(url, headers=HEADERS, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
    logger.info("Downloaded %.1f MB", total / 1024 / 1024)
    return total


def materialize():
    max_rows_env = os.environ.get("AEI_MAX_ROWS")
    max_rows = int(max_rows_env) if max_rows_env else None
    if max_rows:
        logger.info("AEI_MAX_ROWS=%d — capping ingestion", max_rows)

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        tmp_path = tmp.name

    stream_download(URL, tmp_path)

    logger.info("Reading CSV %s", tmp_path)
    df = pd.read_csv(
        tmp_path,
        dtype={
            "geo_id": str,
            "geography": str,
            "platform_and_product": str,
            "facet": str,
            "variable": str,
            "cluster_name": str,
        },
        parse_dates=["date_start", "date_end"],
        nrows=max_rows,
        keep_default_na=False,
        na_values=[""],
    )

    # Drop any row with NaN in PK columns (guards against CSV parse artifacts).
    pk_cols = ["geo_id", "date_start", "facet", "variable"]
    before = len(df)
    df = df.dropna(subset=pk_cols).reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        logger.warning("Dropped %d rows with null PK columns (of %d)", dropped, before)

    df["cluster_name"] = df["cluster_name"].fillna("").astype(str)
    df["geo_id"] = df["geo_id"].astype(str)
    df["geography"] = df["geography"].astype(str)
    df["platform_and_product"] = df["platform_and_product"].astype(str)
    df["facet"] = df["facet"].astype(str)
    df["variable"] = df["variable"].astype(str)
    df["level"] = df["level"].fillna(0).astype(int)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["date_start"] = df["date_start"].dt.date
    df["date_end"] = df["date_end"].dt.date
    df["release_id"] = RELEASE_ID
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "Rows: %d, geographies: %d, facets: %d, variables: %d",
        len(df),
        df["geo_id"].nunique(),
        df["facet"].nunique(),
        df["variable"].nunique(),
    )

    try:
        os.unlink(tmp_path)
    except OSError:
        pass

    return df
