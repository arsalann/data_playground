"""@bruin
name: raw.aep_ember_yearly
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches Ember Global Electricity annual data covering generation by source,
  capacity, demand, and emissions for 215 countries (2000-2024).

  Tries the Ember API first, falls back to bulk CSV download.
  Data is stored in long format (one row per country-year-variable).

  Data source: https://ember-climate.org/data-catalogue/yearly-electricity-data/
  License: CC-BY-4.0

materialization:
  type: table
  strategy: create+replace

columns:
  - name: country_or_area
    type: VARCHAR
    description: Country or region name
    primary_key: true
  - name: year
    type: INTEGER
    description: Year of the observation
    primary_key: true
  - name: variable
    type: VARCHAR
    description: "Variable name (e.g. Electricity generation, Coal, Solar)"
    primary_key: true
  - name: value
    type: DOUBLE
    description: Numeric value of the variable
  - name: unit
    type: VARCHAR
    description: "Unit (TWh, GW, gCO2/kWh, mtCO2, %)"
  - name: category
    type: VARCHAR
    description: "Category grouping (Electricity generation, Capacity, Emissions, etc.)"
  - name: subcategory
    type: VARCHAR
    description: "Subcategory for finer grouping (e.g. fuel type)"
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-3 country code
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was fetched

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

EMBER_API_BASE = "https://ember-data.org/api"
EMBER_CSV_URLS = [
    "https://ember-data.org/data/data-explorer/yearly-electricity-data.csv",
    "https://storage.googleapis.com/ember-data-warehouse/latest/yearly_full_release_long_format.csv",
    "https://ember-climate.org/app/uploads/2024/11/yearly_full_release_long_format.csv",
]


def try_ember_api() -> pd.DataFrame | None:
    """Attempt to fetch data from Ember's API. Returns None if API is unavailable."""
    endpoints = [
        "/v1/electricity-generation/yearly",
        "/electricity-generation/yearly",
    ]

    for endpoint in endpoints:
        url = f"{EMBER_API_BASE}{endpoint}"
        try:
            logger.info("Trying Ember API: %s", url)
            resp = requests.get(url, params={"per_page": 10}, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict) and "data" in data:
                    logger.info("Ember API available at %s", url)
                    return fetch_from_api(url)
                elif isinstance(data, list) and len(data) > 0:
                    logger.info("Ember API available at %s", url)
                    return fetch_from_api(url)
            else:
                logger.info("API endpoint %s returned %d", url, resp.status_code)
        except requests.RequestException as e:
            logger.info("API endpoint %s failed: %s", url, e)
            continue

    logger.info("Ember API not available, falling back to CSV")
    return None


def fetch_from_api(base_url: str) -> pd.DataFrame | None:
    """Fetch full dataset from Ember API with pagination."""
    all_data = []
    page = 1
    per_page = 1000

    for _ in range(500):
        try:
            resp = requests.get(
                base_url,
                params={"page": page, "per_page": per_page},
                timeout=60,
            )
            if resp.status_code != 200:
                logger.warning("API returned %d on page %d", resp.status_code, page)
                break

            data = resp.json()
            records = data.get("data", data) if isinstance(data, dict) else data

            if not records:
                break

            all_data.extend(records)
            logger.info("  Page %d: %d records (total: %d)", page, len(records), len(all_data))

            if len(records) < per_page:
                break
            page += 1
            time.sleep(0.3)
        except requests.RequestException as e:
            logger.warning("API error on page %d: %s", page, e)
            break

    if not all_data:
        return None

    df = pd.DataFrame(all_data)
    logger.info("API returned %d total records with columns: %s", len(df), list(df.columns))
    return df


def fetch_from_csv() -> pd.DataFrame:
    """Download Ember bulk CSV with fallback URLs."""
    for url in EMBER_CSV_URLS:
        for attempt in range(3):
            try:
                logger.info("Downloading Ember CSV from %s (attempt %d)...", url, attempt + 1)
                resp = requests.get(url, timeout=180)
                if resp.status_code == 200:
                    logger.info("Downloaded %.1f MB", len(resp.content) / 1e6)
                    df = pd.read_csv(io.StringIO(resp.text))
                    logger.info("CSV has %d rows, columns: %s", len(df), list(df.columns))
                    return df
                else:
                    logger.info("URL returned %d, trying next", resp.status_code)
                    break
            except requests.RequestException as e:
                wait = 10 * (attempt + 1)
                logger.warning("Download failed, retrying in %ds: %s", wait, e)
                time.sleep(wait)

    raise RuntimeError(
        "Failed to download Ember data from any source. "
        "Try downloading manually from https://ember-climate.org/data-catalogue/yearly-electricity-data/"
    )


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize whatever format we got into a consistent long-format schema."""
    cols_lower = {c: c.lower().strip() for c in df.columns}
    df = df.rename(columns=cols_lower)

    col_map = {}
    for col in df.columns:
        if col in ("country_or_area", "country", "area", "entity"):
            col_map[col] = "country_or_area"
        elif col in ("year",):
            col_map[col] = "year"
        elif col in ("variable", "series", "indicator"):
            col_map[col] = "variable"
        elif col in ("value",):
            col_map[col] = "value"
        elif col in ("unit",):
            col_map[col] = "unit"
        elif col in ("category",):
            col_map[col] = "category"
        elif col in ("subcategory", "sub_category"):
            col_map[col] = "subcategory"
        elif col in ("country_code", "iso_code", "iso3", "country_or_area_code"):
            col_map[col] = "country_code"

    df = df.rename(columns=col_map)

    required = ["country_or_area", "year", "variable", "value"]
    missing = [c for c in required if c not in df.columns]

    if missing:
        logger.info("Columns available: %s", list(df.columns))
        if "country_or_area" not in df.columns and len(df.columns) > 0:
            wide_cols = [c for c in df.columns if c not in ("year", "unit", "category")]
            if len(wide_cols) > 5:
                logger.info("Data appears to be in wide format, melting...")
                id_vars = [c for c in ["country_or_area", "year", "country_code", "unit", "category"]
                           if c in df.columns]
                value_vars = [c for c in df.columns if c not in id_vars]
                df = df.melt(id_vars=id_vars, value_vars=value_vars,
                             var_name="variable", value_name="value")

    for col in ["unit", "category", "subcategory", "country_code"]:
        if col not in df.columns:
            df[col] = None

    if "country_or_area" not in df.columns:
        raise RuntimeError(f"Cannot find country column. Available: {list(df.columns)}")

    df = df[df["value"].notna()].copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df[df["year"].notna()].copy()
    df["year"] = df["year"].astype(int)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df[["country_or_area", "year", "variable", "value", "unit",
               "category", "subcategory", "country_code"]]


def materialize():
    logger.info("Starting Ember yearly electricity data ingestion")

    df = try_ember_api()
    source = "API"

    if df is None:
        df = fetch_from_csv()
        source = "CSV"

    logger.info("Data source: %s", source)
    df = normalize_dataframe(df)
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "Final: %d rows, %d countries, years %d-%d",
        len(df), df["country_or_area"].nunique(),
        df["year"].min(), df["year"].max(),
    )
    return df
