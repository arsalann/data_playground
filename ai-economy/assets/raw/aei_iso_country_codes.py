"""@bruin

name: raw.aei_iso_country_codes
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  ISO 3166-1 country code mapping published alongside the AEI release. Maps ISO alpha-2
  (used by AEI) to ISO alpha-3 (used by World Bank) and the canonical English country
  name. Required for cross-source country-level joins.

  The 2026-01-15 release does not ship the ISO CSV so this asset pulls from the
  2025-09-15 release. ISO codes are stable across releases.

  Source: https://huggingface.co/datasets/Anthropic/EconomicIndex/resolve/main/release_2025_09_15/data/intermediate/iso_country_codes.csv
  License: CC BY 4.0

materialization:
  type: table
  strategy: create+replace

columns:
  - name: iso_alpha_2
    type: VARCHAR
    description: ISO 3166-1 alpha-2 code (2 letters, e.g. `US`).
    primary_key: true
  - name: iso_alpha_3
    type: VARCHAR
    description: ISO 3166-1 alpha-3 code (3 letters, e.g. `USA`).
  - name: country_name
    type: VARCHAR
    description: Canonical English country name.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this asset was materialized.

@bruin"""

import logging
import os
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

URL = (
    "https://huggingface.co/datasets/Anthropic/EconomicIndex/resolve/main/"
    "release_2025_09_15/data/intermediate/iso_country_codes.csv"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
}


def materialize():
    logger.info("Fetching %s", URL)
    resp = requests.get(URL, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    # `keep_default_na=False` is critical: Namibia's iso_alpha_2 is "NA", which
    # pandas otherwise reads as NaN — corrupting the row and the load.
    df = pd.read_csv(StringIO(resp.text), dtype=str, keep_default_na=False, na_values=[])
    logger.info("Raw: %d rows, cols=%s", len(df), list(df.columns))

    # Normalize column names — the CSV columns are `alpha-2`, `alpha-3`, `name` in the release.
    col_map = {}
    for c in df.columns:
        lc = c.strip().lower()
        if lc in ("alpha-2", "alpha_2", "iso_alpha_2", "iso 3166-1 alpha-2"):
            col_map[c] = "iso_alpha_2"
        elif lc in ("alpha-3", "alpha_3", "iso_alpha_3", "iso 3166-1 alpha-3"):
            col_map[c] = "iso_alpha_3"
        elif lc in ("name", "country_name", "country", "english short name"):
            col_map[c] = "country_name"

    df = df.rename(columns=col_map)
    required = {"iso_alpha_2", "iso_alpha_3", "country_name"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"ISO CSV missing expected columns: {missing} (got {list(df.columns)})")

    df = df[["iso_alpha_2", "iso_alpha_3", "country_name"]]
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Ready: %d rows", len(df))
    return df
