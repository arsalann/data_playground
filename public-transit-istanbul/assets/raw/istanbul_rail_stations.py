"""@bruin
name: raw.istanbul_rail_stations
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Daily rail station-level ridership with geographic coordinates (2021-2025).
  Source: IBB Open Data Portal - https://data.ibb.gov.tr/en/dataset/rayli-sistemler-istasyon-bazli-yolcu-ve-yolculuk-sayilari
  License: Istanbul Metropolitan Municipality Open Data License.
  Covers all metro, tram, funicular, teleferik, and Marmaray stations.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: transaction_year
    type: INTEGER
    description: Year of the ridership observation
    primary_key: true
  - name: transaction_month
    type: INTEGER
    description: Month of the ridership observation (1-12)
    primary_key: true
  - name: transaction_day
    type: INTEGER
    description: Day of the ridership observation (1-31)
    primary_key: true
  - name: line
    type: VARCHAR
    description: Rail line identifier (e.g. M1-YENIKAPI ATATURK HAVALIMANI METRO HATTI)
    primary_key: true
  - name: station_name
    type: VARCHAR
    description: Name of the rail station
    primary_key: true
  - name: station_number
    type: VARCHAR
    description: Station code identifier
  - name: terminal_number
    type: VARCHAR
    description: Terminal/gate code within the station
  - name: town
    type: VARCHAR
    description: Istanbul district (ilce) where the station is located
  - name: longitude
    type: DOUBLE
    description: Station longitude in WGS84 (EPSG:4326)
  - name: latitude
    type: DOUBLE
    description: Station latitude in WGS84 (EPSG:4326)
  - name: passage_cnt
    type: INTEGER
    description: Number of Istanbulkart tap-ins (passages) at the station for the day
  - name: passenger_cnt
    type: INTEGER
    description: Number of unique passengers at the station for the day
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was extracted

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

RAIL_STATION_URLS = {
    2021: "https://data.ibb.gov.tr/dataset/ae3b2e4b-073a-48d0-8ef3-f28f19bcb19c/resource/604776d6-e99f-469c-bf25-25ccadc5e89b/download/2021-yl-rayl-sistemler-istasyon-bazl-yolcu-ve-yolculuk-saylar.csv",
    2022: "https://data.ibb.gov.tr/dataset/ae3b2e4b-073a-48d0-8ef3-f28f19bcb19c/resource/5c2b78fc-3b68-4722-844f-3051c358a13f/download/2022-yl-rayl-sistemler-istasyon-bazl-yolcu-ve-yolculuk-saylar.csv",
    2023: "https://data.ibb.gov.tr/dataset/ae3b2e4b-073a-48d0-8ef3-f28f19bcb19c/resource/5de4fee9-4b31-45f3-bb8d-f566e6a302af/download/rayl-sistemler-istasyon-bazl-yolcu-ve-yolculuk-saylar-2023.csv",
    2024: "https://data.ibb.gov.tr/dataset/ae3b2e4b-073a-48d0-8ef3-f28f19bcb19c/resource/6028373f-6bcf-45a9-95a3-f3f741b4b55e/download/2024-yl-rayl-sistemler-istasyon-bazl-yolcu-ve-yolculuk-saylar.csv",
    2025: "https://data.ibb.gov.tr/dataset/ae3b2e4b-073a-48d0-8ef3-f28f19bcb19c/resource/cce8e138-686a-4b8d-bac0-c2a0ab294a6e/download/2025-yl-rayl-sistemler-istasyon-bazl-yolcu-ve-yolculuk-saylar.csv",
}


def download_csv(url: str, year: int) -> pd.DataFrame:
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    for enc in ("utf-8-sig", "utf-8", "iso-8859-9"):
        try:
            text = resp.content.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(f"Could not decode {year} rail station CSV")

    # Detect delimiter
    first_line = text.split("\n")[0]
    sep = ";" if ";" in first_line and "," not in first_line else ","
    df = pd.read_csv(StringIO(text), sep=sep)
    return df


def fix_coord(val: str) -> float | None:
    """Fix coordinates that use dots as thousands separators.

    E.g. '289.343.888.888.889' -> 28.9343888888889
    Normal values like '28.9343888888889' pass through unchanged.
    """
    s = str(val).strip().strip('"')
    if s in ("", "nan", "None"):
        return None
    # If there are 2+ dots, it's the corrupted format
    if s.count(".") >= 2:
        # Remove all dots, then insert decimal after first 2 digits
        digits = s.replace(".", "")
        # Istanbul coords: lon ~28-29, lat ~40-41
        # Try inserting decimal after 2nd digit
        fixed = digits[:2] + "." + digits[2:]
        try:
            return float(fixed)
        except ValueError:
            return None
    try:
        return float(s)
    except ValueError:
        return None


def fetch_rail_stations() -> pd.DataFrame:
    all_frames = []
    for year, url in RAIL_STATION_URLS.items():
        logger.info("Downloading rail station data for %d ...", year)
        try:
            df = download_csv(url, year)
        except Exception as e:
            logger.warning("Failed to download %d: %s", year, e)
            continue

        df.columns = [c.strip().strip('"').strip("\ufeff") for c in df.columns]
        col_map = {
            "transaction_year": "transaction_year",
            "transaction_month": "transaction_month",
            "transaction_day": "transaction_day",
            "line": "line",
            "station_name": "station_name",
            "station_number": "station_number",
            "terminal_number": "terminal_number",
            "town": "town",
            "longitude": "longitude",
            "latitude": "latitude",
            "passage_cnt": "passage_cnt",
            "passanger_cnt": "passenger_cnt",
        }
        df = df.rename(columns=col_map)
        keep_cols = [v for v in col_map.values() if v in df.columns]
        df = df[keep_cols]

        # Add missing terminal_number if absent (some years don't have it)
        if "terminal_number" not in df.columns:
            df["terminal_number"] = None

        df["transaction_year"] = pd.to_numeric(df["transaction_year"], errors="coerce").astype("Int64")
        df["transaction_month"] = pd.to_numeric(df["transaction_month"], errors="coerce").astype("Int64")
        df["transaction_day"] = pd.to_numeric(df["transaction_day"], errors="coerce").astype("Int64")
        df["longitude"] = df["longitude"].apply(fix_coord)
        df["latitude"] = df["latitude"].apply(fix_coord)
        df["passage_cnt"] = pd.to_numeric(df["passage_cnt"], errors="coerce").astype("Int64")
        df["passenger_cnt"] = pd.to_numeric(df["passenger_cnt"], errors="coerce").astype("Int64")

        # Fill nulls in string columns to avoid Parquet issues
        for col in ["line", "station_name", "station_number", "terminal_number", "town"]:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)

        all_frames.append(df)
        logger.info("  %d: %d rows", year, len(df))

    if not all_frames:
        logger.warning("No rail station data fetched")
        return pd.DataFrame()

    return pd.concat(all_frames, ignore_index=True)


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2021-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2025-12-31")
    logger.info("Interval: %s to %s", start_date, end_date)

    df = fetch_rail_stations()
    if df.empty:
        return df

    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Total rail station rows: %d", len(df))
    return df
