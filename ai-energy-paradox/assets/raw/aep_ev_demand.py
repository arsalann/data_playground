"""@bruin
name: raw.aep_ev_demand
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches IEA Global EV Data Explorer data covering EV sales, stock,
  charging infrastructure, and electricity demand by country (2010-2024
  actuals, 2025-2030 projections).

  Tries the IEA EV API endpoint first, falls back to Excel download,
  then to a local file path via AEP_EV_DATA_PATH env var.

  Data source: https://www.iea.org/data-and-statistics/data-tools/global-ev-data-explorer
  License: CC-BY-4.0

materialization:
  type: table
  strategy: create+replace

columns:
  - name: region
    type: VARCHAR
    description: Country or region name
    primary_key: true
  - name: category
    type: VARCHAR
    description: "Data category: Historical or Projection"
    primary_key: true
  - name: parameter
    type: VARCHAR
    description: "Metric: EV sales, EV stock, EV sales share, Electricity demand, etc."
    primary_key: true
  - name: mode
    type: VARCHAR
    description: "Vehicle mode: Cars, Buses, Vans, Trucks"
    primary_key: true
  - name: powertrain
    type: VARCHAR
    description: "Powertrain: BEV, PHEV, EV (total)"
    primary_key: true
  - name: year
    type: INTEGER
    description: Year of the observation
    primary_key: true
  - name: value
    type: DOUBLE
    description: Numeric value of the metric
  - name: unit
    type: VARCHAR
    description: "Unit: vehicles, %, GWh, TWh"
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

IEA_API_URL = "https://api.iea.org/evs"
IEA_EXCEL_URLS = [
    "https://api.iea.org/evs?parameters=EV+sales&category=Historical&csv=true",
    "https://api.iea.org/evs?parameters=EV+stock&category=Historical&csv=true",
    "https://api.iea.org/evs?parameters=EV+sales+share&category=Historical&csv=true",
    "https://api.iea.org/evs?parameters=Electricity+demand&category=Historical&csv=true",
]


def try_iea_api() -> pd.DataFrame | None:
    """Try fetching all EV data from IEA API."""
    all_frames = []

    parameters = [
        "EV sales", "EV stock", "EV sales share",
        "Electricity demand", "EV charging points",
    ]
    categories = ["Historical", "Projection"]

    for param in parameters:
        for cat in categories:
            try:
                resp = requests.get(
                    IEA_API_URL,
                    params={"parameters": param, "category": cat},
                    timeout=60,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list) and len(data) > 0:
                        df = pd.DataFrame(data)
                        logger.info("  API: %s / %s = %d rows", param, cat, len(df))
                        all_frames.append(df)
                    elif isinstance(data, dict) and "data" in data:
                        df = pd.DataFrame(data["data"])
                        logger.info("  API: %s / %s = %d rows", param, cat, len(df))
                        all_frames.append(df)
                    else:
                        logger.info("  API: %s / %s returned empty", param, cat)
                else:
                    logger.info("  API: %s / %s returned %d", param, cat, resp.status_code)
                time.sleep(0.3)
            except requests.RequestException as e:
                logger.info("  API: %s / %s failed: %s", param, cat, e)
                continue

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        logger.info("API returned %d total rows", len(combined))
        return combined

    return None


def try_csv_download() -> pd.DataFrame | None:
    """Try downloading EV data as CSV from IEA API."""
    all_frames = []

    for url in IEA_EXCEL_URLS:
        for attempt in range(3):
            try:
                logger.info("Trying CSV download: %s", url[:80])
                resp = requests.get(url, timeout=60)
                if resp.status_code == 200 and len(resp.content) > 100:
                    df = pd.read_csv(io.StringIO(resp.text))
                    logger.info("  Downloaded %d rows", len(df))
                    all_frames.append(df)
                    break
                else:
                    logger.info("  URL returned %d or empty", resp.status_code)
                    break
            except Exception as e:
                wait = 10 * (attempt + 1)
                logger.warning("  Download failed, retrying in %ds: %s", wait, e)
                time.sleep(wait)

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        logger.info("CSV downloads returned %d total rows", len(combined))
        return combined

    return None


def try_local_file() -> pd.DataFrame | None:
    """Try loading from a local file path."""
    local_path = os.environ.get("AEP_EV_DATA_PATH")
    if not local_path:
        return None

    logger.info("Trying local file: %s", local_path)
    if not os.path.exists(local_path):
        logger.warning("Local file not found: %s", local_path)
        return None

    if local_path.endswith(".xlsx") or local_path.endswith(".xls"):
        df = pd.read_excel(local_path, engine="openpyxl")
    else:
        df = pd.read_csv(local_path)

    logger.info("Local file loaded: %d rows", len(df))
    return df


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize IEA EV data into consistent schema."""
    cols_lower = {c: c.lower().strip() for c in df.columns}
    df = df.rename(columns=cols_lower)

    col_map = {}
    for col in df.columns:
        if col in ("region", "country", "area"):
            col_map[col] = "region"
        elif col in ("category",):
            col_map[col] = "category"
        elif col in ("parameter", "variable", "indicator"):
            col_map[col] = "parameter"
        elif col in ("mode", "vehicle_type"):
            col_map[col] = "mode"
        elif col in ("powertrain", "fuel_type"):
            col_map[col] = "powertrain"
        elif col in ("year",):
            col_map[col] = "year"
        elif col in ("value",):
            col_map[col] = "value"
        elif col in ("unit",):
            col_map[col] = "unit"

    df = df.rename(columns=col_map)

    for col in ["region", "category", "parameter", "mode", "powertrain", "unit"]:
        if col not in df.columns:
            df[col] = "Unknown"

    if "year" not in df.columns:
        raise RuntimeError(f"No year column found. Available: {list(df.columns)}")
    if "value" not in df.columns:
        raise RuntimeError(f"No value column found. Available: {list(df.columns)}")

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df[df["year"].notna()].copy()
    df["year"] = df["year"].astype(int)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[df["value"].notna()].copy()

    return df[["region", "category", "parameter", "mode", "powertrain", "year", "value", "unit"]]


def materialize():
    logger.info("Starting IEA EV data ingestion")

    df = try_iea_api()
    source = "API"

    if df is None:
        logger.info("API unavailable, trying CSV download...")
        df = try_csv_download()
        source = "CSV"

    if df is None:
        logger.info("CSV download unavailable, trying local file...")
        df = try_local_file()
        source = "local file"

    if df is None:
        raise RuntimeError(
            "Failed to fetch IEA EV data from any source. "
            "Download manually from https://www.iea.org/data-and-statistics/data-tools/global-ev-data-explorer "
            "and set AEP_EV_DATA_PATH to the file path."
        )

    logger.info("Data source: %s", source)
    df = normalize_dataframe(df)
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "Final: %d rows, %d regions, years %d-%d",
        len(df), df["region"].nunique(),
        df["year"].min(), df["year"].max(),
    )
    return df
