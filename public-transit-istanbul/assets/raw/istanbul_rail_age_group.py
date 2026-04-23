"""@bruin
name: raw.istanbul_rail_age_group
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Daily rail station ridership segmented by age group (2021-2025).
  Source: IBB Open Data Portal - https://data.ibb.gov.tr/en/dataset/yas-grubuna-gore-rayli-sistemler-istasyon-bazli-yolcu-ve-yolculuk-sayilari
  License: Istanbul Metropolitan Municipality Open Data License.
  2021 data is XLSX format, 2022-2025 are CSV. Age groups are based on Istanbulkart registration data.

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
    description: Rail line identifier
    primary_key: true
  - name: station_name
    type: VARCHAR
    description: Name of the rail station
    primary_key: true
  - name: age_group
    type: VARCHAR
    description: Age group category (e.g. 0-10, 10-20, 20-30, 30-40, 40-50, 50-60, 60-70, 70+)
    primary_key: true
  - name: station_number
    type: VARCHAR
    description: Station code identifier
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
    description: Number of Istanbulkart tap-ins for this age group at the station for the day
  - name: passenger_cnt
    type: INTEGER
    description: Number of unique passengers for this age group at the station for the day
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was extracted

@bruin"""

import logging
import os
from datetime import datetime, timezone
from io import BytesIO, StringIO

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

AGE_GROUP_URLS = {
    2021: {
        "url": "https://data.ibb.gov.tr/dataset/d3df8db4-1ac6-4bfe-8896-7ec1159caa2b/resource/7aec630d-2757-4da0-97f1-71b140abd818/download/2021-yl-ya-grubuna-gore-rayl-sistemler-istasyon-bazl-yolcu-ve-yolculuk-saylar.xlsx",
        "format": "xlsx",
    },
    2022: {
        "url": "https://data.ibb.gov.tr/dataset/d3df8db4-1ac6-4bfe-8896-7ec1159caa2b/resource/8bed95de-bbe2-4550-80f2-87ca51a97f3d/download/2022-yl-ya-grubuna-gore-rayl-sistemler-istasyon-bazl-yolcu-ve-yolculuk-saylar.csv",
        "format": "csv",
    },
    2023: {
        "url": "https://data.ibb.gov.tr/dataset/d3df8db4-1ac6-4bfe-8896-7ec1159caa2b/resource/c96dc298-9d92-4d6f-beb9-d6022404bbce/download/ya-grubuna-gore-rayl-sistemler-istasyon-bazl-yolcu-ve-yolculuk-saylar-2023.csv",
        "format": "csv",
    },
    2024: {
        "url": "https://data.ibb.gov.tr/dataset/d3df8db4-1ac6-4bfe-8896-7ec1159caa2b/resource/f0efe978-7451-40d4-a03e-d8d7b992ae78/download/ya-grubuna-gore-rayl-sistemler-istasyon-bazl-yolcu-ve-yolculuk-saylar-2024.csv",
        "format": "csv",
    },
    2025: {
        "url": "https://data.ibb.gov.tr/dataset/d3df8db4-1ac6-4bfe-8896-7ec1159caa2b/resource/0307304a-458a-4a8c-8454-6946ad75a9d3/download/2025-yl-yaa_gore_rayl_sistemler_istasyon_bazl_yolcu_ve_yolculuk_saylar.csv",
        "format": "csv",
    },
}


def download_file(url: str, year: int, fmt: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=180)
    resp.raise_for_status()

    if fmt == "xlsx":
        df = pd.read_excel(BytesIO(resp.content), engine="openpyxl")
        return df

    for enc in ("utf-8-sig", "utf-8", "iso-8859-9"):
        try:
            text = resp.content.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(f"Could not decode {year} age group CSV")

    # Detect delimiter
    first_line = text.split("\n")[0]
    sep = ";" if ";" in first_line and "," not in first_line else ","
    df = pd.read_csv(StringIO(text), sep=sep)
    return df


def fix_coord(val) -> float | None:
    """Fix coordinates with dots as thousands separators or no separators at all.

    Patterns:
    - '28.9343888888889' -> normal, pass through
    - '289.343.888.888.889' -> dots as thousands sep -> 28.9343888888889
    - 289920277777778 -> integer with no separator -> 28.9920277777778
    """
    s = str(val).strip().strip('"')
    if s in ("", "nan", "None", "NaN"):
        return None

    # If it's a large integer (no dots), insert decimal after 2nd digit
    try:
        f = float(s)
        if f > 180:  # Not a valid coordinate — needs fixing
            digits = s.replace(".", "")
            fixed = digits[:2] + "." + digits[2:]
            return float(fixed)
        return f
    except ValueError:
        pass

    # Multiple dots: thousands separator format
    if s.count(".") >= 2:
        digits = s.replace(".", "")
        fixed = digits[:2] + "." + digits[2:]
        try:
            return float(fixed)
        except ValueError:
            return None
    return None


def fix_count(val) -> int | None:
    """Fix count values that may use dots as thousands separators (from XLSX)."""
    s = str(val).strip().strip('"')
    if s in ("", "nan", "None", "NaN"):
        return None
    # Remove dots used as thousands separators (e.g. 7.424 -> 7424)
    try:
        f = float(s)
        # If it looks like a decimal number with fractional part
        # that is actually thousands-separated (e.g., 7.424 -> 7424)
        if f != int(f) and "." in s:
            # Check if removing dot gives a reasonable integer
            no_dot = s.replace(".", "")
            return int(no_dot)
        return int(f)
    except (ValueError, OverflowError):
        return None


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().strip('"').strip("\ufeff").lower().replace("-", "_") for c in df.columns]
    col_map = {
        "passage_cnt": "passage_cnt",
        "passanger_cnt": "passenger_cnt",
        "transaction_year": "transaction_year",
        "transaction_month": "transaction_month",
        "transaction_day": "transaction_day",
        "line": "line",
        "station_name": "station_name",
        "station_number": "station_number",
        "town": "town",
        "age": "age_group",
        "age_group": "age_group",
        "longitude": "longitude",
        "latitude": "latitude",
    }
    df = df.rename(columns=col_map)

    # Add missing columns
    if "transaction_day" not in df.columns:
        df["transaction_day"] = None

    keep_cols = [v for v in set(col_map.values()) if v in df.columns]
    df = df[keep_cols]

    df["transaction_year"] = pd.to_numeric(df["transaction_year"], errors="coerce").astype("Int64")
    df["transaction_month"] = pd.to_numeric(df["transaction_month"], errors="coerce").astype("Int64")
    df["transaction_day"] = pd.to_numeric(df["transaction_day"], errors="coerce").astype("Int64")
    df["longitude"] = df["longitude"].apply(fix_coord)
    df["latitude"] = df["latitude"].apply(fix_coord)
    df["passage_cnt"] = pd.to_numeric(df["passage_cnt"], errors="coerce").round(0).astype("Int64")
    df["passenger_cnt"] = pd.to_numeric(df["passenger_cnt"], errors="coerce").round(0).astype("Int64")

    # Fill nulls in string columns
    for col in ["line", "station_name", "station_number", "town", "age_group"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    return df


def fetch_age_group_data() -> pd.DataFrame:
    all_frames = []
    for year, info in AGE_GROUP_URLS.items():
        logger.info("Downloading age group data for %d (%s) ...", year, info["format"])
        try:
            df = download_file(info["url"], year, info["format"])
        except Exception as e:
            logger.warning("Failed to download %d: %s", year, e)
            continue

        df = standardize_columns(df)
        all_frames.append(df)
        logger.info("  %d: %d rows", year, len(df))

    if not all_frames:
        logger.warning("No age group data fetched")
        return pd.DataFrame()

    return pd.concat(all_frames, ignore_index=True)


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2021-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2025-12-31")
    logger.info("Interval: %s to %s", start_date, end_date)

    df = fetch_age_group_data()
    if df.empty:
        return df

    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Total age group rows: %d", len(df))
    return df
