"""@bruin
name: raw.istanbul_ferry_piers
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Monthly ferry pier passenger counts from Istanbul sea piers (2021-2025).
  Source: IBB Open Data Portal - https://data.ibb.gov.tr/en/dataset/istanbul-deniz-iskeleleri-yolcu-sayilari
  License: Istanbul Metropolitan Municipality Open Data License.
  Fields: year, month, operator name, pier name, unique passengers, total journeys.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: year
    type: INTEGER
    description: Calendar year of the observation
    primary_key: true
  - name: month
    type: INTEGER
    description: Calendar month (1-12)
    primary_key: true
  - name: operator_name
    type: VARCHAR
    description: Ferry operator name (e.g. IBB, TURYOL, IDO)
    primary_key: true
  - name: pier_name
    type: VARCHAR
    description: Name of the ferry pier / terminal
    primary_key: true
  - name: unique_passengers
    type: INTEGER
    description: Number of unique passengers (distinct Istanbulkart taps) for the month
  - name: total_journeys
    type: INTEGER
    description: Total number of journeys (including return trips) for the month
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was extracted

@bruin"""

import logging
import os
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

FERRY_URLS = {
    2021: "https://data.ibb.gov.tr/dataset/20f33ff0-1ab3-4378-9998-486e28242f48/resource/634727f0-dadf-40fb-a359-c4c4eeb40ee0/download/2021-yl-istanbul-deniz-iskeleleri-yolcu-saylar.csv",
    2022: "https://data.ibb.gov.tr/dataset/20f33ff0-1ab3-4378-9998-486e28242f48/resource/6fbdd928-8c37-43a4-8e6a-ba0fa7f767fb/download/2022-yl-istanbul-deniz-iskeleleri-yolcu-saylar.csv",
    2023: "https://data.ibb.gov.tr/dataset/20f33ff0-1ab3-4378-9998-486e28242f48/resource/994e619c-9efa-4ce9-853e-7828502bf9f7/download/2023-yl-istanbul-deniz-iskeleleri-yolcu-saylar.csv",
    2024: "https://data.ibb.gov.tr/dataset/20f33ff0-1ab3-4378-9998-486e28242f48/resource/00686466-bb9f-4e32-897a-0c7efeb31686/download/2024-yl-istanbul-deniz-iskeleleri-yolcu-saylar.csv",
    2025: "https://data.ibb.gov.tr/dataset/20f33ff0-1ab3-4378-9998-486e28242f48/resource/569932a5-96c3-455c-ba57-31893965af12/download/2025-yl-istanbul-deniz-iskeleleri-yolcu-saylar.csv",
}


def fetch_ferry_data() -> pd.DataFrame:
    all_frames = []
    for year, url in FERRY_URLS.items():
        logger.info("Downloading ferry data for %d ...", year)
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to download %d: %s", year, e)
            continue

        from io import StringIO

        # Try comma-separated first, then semicolon (2025 uses semicolons)
        decoded = False
        for enc in ("utf-8-sig", "utf-8", "iso-8859-9"):
            try:
                text = resp.content.decode(enc)
                decoded = True
                break
            except UnicodeDecodeError:
                continue

        if not decoded:
            logger.warning("Could not decode %d CSV with any encoding", year)
            continue

        # Detect delimiter
        first_line = text.split("\n")[0]
        sep = ";" if ";" in first_line and "," not in first_line else ","
        df = pd.read_csv(StringIO(text), sep=sep)

        df.columns = [c.strip() for c in df.columns]
        col_map = {
            "yil": "year",
            "ay": "month",
            "otorite_adi": "operator_name",
            "istasyon_adi": "pier_name",
            "tekil_yolcu_sayisi": "unique_passengers",
            "toplam_yolculuk_sayisi": "total_journeys",
            "yolcu_sayisi": "total_journeys",
        }
        df = df.rename(columns=col_map)
        expected_cols = ["year", "month", "operator_name", "pier_name", "unique_passengers", "total_journeys"]
        df = df[[c for c in expected_cols if c in df.columns]]
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        df["month"] = pd.to_numeric(df["month"], errors="coerce").astype("Int64")
        df["operator_name"] = df["operator_name"].fillna("UNKNOWN").astype(str)
        df["pier_name"] = df["pier_name"].fillna("UNKNOWN").astype(str)
        df["unique_passengers"] = pd.to_numeric(df["unique_passengers"], errors="coerce").fillna(0).astype(int)
        df["total_journeys"] = pd.to_numeric(df["total_journeys"], errors="coerce").fillna(0).astype(int)
        all_frames.append(df)
        logger.info("  %d: %d rows", year, len(df))

    if not all_frames:
        logger.warning("No ferry data fetched")
        return pd.DataFrame()

    return pd.concat(all_frames, ignore_index=True)


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2021-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2025-12-31")
    logger.info("Interval: %s to %s", start_date, end_date)

    df = fetch_ferry_data()
    if df.empty:
        return df

    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Total ferry pier rows: %d", len(df))
    return df
