"""@bruin

name: raw.aei_api_usage
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Anthropic Economic Index (AEI) 1P API telemetry for the 2026-01-15 release,
  covering the 2025-11-13 to 2025-11-20 observation window.

  Same long-format schema as `raw.aei_claude_usage`, but `geo_id` is always `GLOBAL`
  (no country breakdown available for enterprise/API usage). Adds API-specific metrics
  via intersection facets `onet_task::cost`, `onet_task::prompt_tokens`,
  `onet_task::completion_tokens`.

  Source: https://huggingface.co/datasets/Anthropic/EconomicIndex
  License: CC BY 4.0

materialization:
  type: table
  strategy: create+replace

columns:
  - name: geo_id
    type: VARCHAR
    description: Constant `GLOBAL` — 1P API telemetry is not broken down by country.
    primary_key: true
  - name: geography
    type: VARCHAR
    description: Constant `global`.
  - name: date_start
    type: DATE
    description: Observation window start.
    primary_key: true
  - name: date_end
    type: DATE
    description: Observation window end.
  - name: platform_and_product
    type: VARCHAR
    description: Source platform identifier (`1P API`).
  - name: facet
    type: VARCHAR
    description: Metric facet.
    primary_key: true
  - name: level
    type: INTEGER
    description: Clustering depth level inside the facet.
  - name: variable
    type: VARCHAR
    description: Specific metric name.
    primary_key: true
  - name: cluster_name
    type: VARCHAR
    description: Entity name within the facet (O*NET task description, etc.). Empty string if unused.
    primary_key: true
  - name: value
    type: DOUBLE
    description: Numeric value.
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
    f"{RELEASE_ID}/data/intermediate/aei_raw_1p_api_2025-11-13_to_2025-11-20.csv"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
}


def stream_download(url: str, dest: str) -> int:
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
        "Rows: %d, facets: %d, variables: %d",
        len(df),
        df["facet"].nunique(),
        df["variable"].nunique(),
    )

    try:
        os.unlink(tmp_path)
    except OSError:
        pass

    return df
