"""@bruin

name: raw.aei_prior_snapshot
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Anthropic Economic Index (AEI) Claude.ai telemetry from the 2025-09-15 release.
  Same long-format schema as `raw.aei_claude_usage`, used solely for release-over-release
  comparison of overlapping tasks.

  Note: the 2025-09-15 release was produced on Sonnet 4; the 2026-01-15 release runs on
  Sonnet 4.5. Any release-over-release delta confounds time, sampling, and model version.

  Source: https://huggingface.co/datasets/Anthropic/EconomicIndex
  License: CC BY 4.0

materialization:
  type: table
  strategy: create+replace

columns:
  - name: geo_id
    type: VARCHAR
    description: Geography identifier.
    primary_key: true
  - name: geography
    type: VARCHAR
    description: Geography level (`global`, `country`, `country-state`).
  - name: date_start
    type: DATE
    description: Observation window start for this snapshot.
    primary_key: true
  - name: date_end
    type: DATE
    description: Observation window end.
  - name: platform_and_product
    type: VARCHAR
    description: Source platform.
  - name: facet
    type: VARCHAR
    description: Metric facet.
    primary_key: true
  - name: level
    type: INTEGER
    description: Clustering depth level.
  - name: variable
    type: VARCHAR
    description: Specific metric name.
    primary_key: true
  - name: cluster_name
    type: VARCHAR
    description: Entity name within the facet. Empty string if unused.
    primary_key: true
  - name: value
    type: DOUBLE
    description: Numeric value.
  - name: release_id
    type: VARCHAR
    description: AEI release identifier (constant `release_2025_09_15`).
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this asset was materialized.

@bruin"""

import logging
import os
import re
import tempfile
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

RELEASE_ID = "release_2025_09_15"
RELEASE_BASE = (
    f"https://huggingface.co/datasets/Anthropic/EconomicIndex/tree/main/{RELEASE_ID}"
)
# HuggingFace's CSV resolve URL pattern for the prior release.
CANDIDATE_PATHS = [
    # Observed path in the HuggingFace dataset browser for the Sept release.
    "release_2025_09_15/data/intermediate/aei_raw_claude_ai_2025-08-04_to_2025-08-11.csv",
    "release_2025_09_15/data/intermediate/aei_raw_claude_ai_2025-08-11_to_2025-08-18.csv",
    "release_2025_09_15/data/intermediate/aei_raw_claude_ai_2025-07-28_to_2025-08-04.csv",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
}


def hf_url(path: str) -> str:
    return f"https://huggingface.co/datasets/Anthropic/EconomicIndex/resolve/main/{path}"


def discover_csv_path() -> str:
    """Find the Claude.ai CSV for the prior release by listing HF tree API."""
    for path in CANDIDATE_PATHS:
        url = hf_url(path)
        head = requests.head(url, headers=HEADERS, allow_redirects=True, timeout=60)
        if head.status_code == 200:
            logger.info("Resolved prior release CSV at %s", path)
            return url

    # Fall back to the HF tree API to discover the exact file name in case the
    # date window differs from our candidates.
    logger.info("Listing HF tree for %s/data/intermediate/", RELEASE_ID)
    tree_url = (
        f"https://huggingface.co/api/datasets/Anthropic/EconomicIndex/tree/main/"
        f"{RELEASE_ID}/data/intermediate"
    )
    resp = requests.get(tree_url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    entries = resp.json()
    for entry in entries:
        name = entry.get("path", "")
        if re.search(r"aei_raw_claude_ai_.*\.csv$", name):
            logger.info("Discovered %s", name)
            return hf_url(name)

    raise RuntimeError("Unable to locate prior-release Claude.ai CSV on HuggingFace")


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

    url = discover_csv_path()

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        tmp_path = tmp.name

    stream_download(url, tmp_path)

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
        "Rows: %d, geographies: %d, facets: %d",
        len(df),
        df["geo_id"].nunique(),
        df["facet"].nunique(),
    )

    try:
        os.unlink(tmp_path)
    except OSError:
        pass

    return df
