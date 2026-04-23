"""@bruin
name: raw.istanbul_hourly_transport
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Hourly Istanbulkart tap data for all Istanbul public transport modes (2020-2024).
  Source: IBB Open Data Portal - https://data.ibb.gov.tr/en/dataset/hourly-public-transport-data-set
  License: Istanbul Metropolitan Municipality Open Data License.
  60 monthly CSV files, ~1-1.8 GB each (~60 GB total).
  Covers IETT bus, metro, metrobus, Marmaray, ferries, and all other modes.
  Processed month-by-month using chunked CSV reading to avoid memory issues.
  Uses CKAN API to discover monthly resource URLs dynamically.

materialization:
  type: table
  strategy: append

columns:
  - name: transition_date
    type: DATE
    description: Date of the transit trip
    primary_key: true
  - name: transition_hour
    type: INTEGER
    description: Hour of the day (0-23)
    primary_key: true
  - name: transport_type_id
    type: INTEGER
    description: Numeric transport mode identifier
    primary_key: true
  - name: road_type
    type: VARCHAR
    description: Road/system type (RAYLI, DENIZYOLU, KARAYOLU, etc.)
  - name: line
    type: VARCHAR
    description: Route/line identifier
    primary_key: true
  - name: transfer_type
    type: VARCHAR
    description: Trip type (Normal or Aktarma/transfer)
  - name: number_of_passage
    type: INTEGER
    description: Number of Istanbulkart tap-ins (passages)
  - name: number_of_passenger
    type: INTEGER
    description: Number of unique passengers
  - name: product_kind
    type: VARCHAR
    description: Fare product type (TAM, OGRENCI, etc.)
    primary_key: true
  - name: transaction_type_desc
    type: VARCHAR
    description: Human-readable fare type description
  - name: town
    type: VARCHAR
    description: Istanbul district (ilce) of the trip
    primary_key: true
  - name: line_name
    type: VARCHAR
    description: Human-readable line/route name
  - name: station_poi_desc_cd
    type: VARCHAR
    description: Station or point-of-interest description code
    primary_key: true
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was extracted

@bruin"""

import logging
import os
import time
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

DATASET_ID = "a6855ce7-4092-40a5-82b5-34cf3c7e36e3"
CKAN_API_URL = f"https://data.ibb.gov.tr/api/3/action/package_show?id={DATASET_ID}"
CHUNK_SIZE = 100_000
MAX_RETRIES = 3
RETRY_DELAY = 5


def get_resource_urls() -> list[dict]:
    """Fetch all monthly resource URLs from CKAN API."""
    logger.info("Fetching resource list from CKAN API ...")
    resp = requests.get(CKAN_API_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    resources = data["result"]["resources"]

    monthly_resources = []
    for r in resources:
        url = r.get("url", "")
        name = r.get("name", "")
        if "hourly_transportation_" in url and url.endswith(".csv"):
            # Extract YYYYMM from filename
            fname = url.split("/")[-1]
            yyyymm = fname.replace("hourly_transportation_", "").replace(".csv", "")
            monthly_resources.append({
                "url": url,
                "name": name,
                "yyyymm": yyyymm,
            })

    monthly_resources.sort(key=lambda x: x["yyyymm"])
    logger.info("Found %d monthly resources", len(monthly_resources))
    return monthly_resources


def download_month_chunked(url: str, yyyymm: str) -> pd.DataFrame:
    """Download and process a single month's CSV in chunks."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("  Downloading %s (attempt %d) ...", yyyymm, attempt)
            resp = requests.get(url, timeout=600, stream=True)
            resp.raise_for_status()

            # Detect encoding
            content = resp.content
            for enc in ("utf-8-sig", "utf-8", "iso-8859-9"):
                try:
                    text = content.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise ValueError(f"Could not decode {yyyymm}")

            chunks = []
            reader = pd.read_csv(StringIO(text), chunksize=CHUNK_SIZE)
            for i, chunk in enumerate(reader):
                chunk.columns = [c.strip() for c in chunk.columns]
                chunks.append(chunk)

            if not chunks:
                logger.warning("  %s: empty file", yyyymm)
                return pd.DataFrame()

            df = pd.concat(chunks, ignore_index=True)
            return df

        except requests.RequestException as e:
            logger.warning("  %s attempt %d failed: %s", yyyymm, attempt, e)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
            else:
                logger.error("  %s: all retries exhausted", yyyymm)
                return pd.DataFrame()

    return pd.DataFrame()


def standardize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column types."""
    if df.empty:
        return df

    df["transition_date"] = pd.to_datetime(df["transition_date"], errors="coerce").dt.date
    df["transition_hour"] = pd.to_numeric(df["transition_hour"], errors="coerce").astype("Int64")
    df["transport_type_id"] = pd.to_numeric(df["transport_type_id"], errors="coerce").astype("Int64")
    df["number_of_passage"] = pd.to_numeric(df["number_of_passage"], errors="coerce").astype("Int64")
    df["number_of_passenger"] = pd.to_numeric(df["number_of_passenger"], errors="coerce").astype("Int64")

    for col in ["road_type", "line", "transfer_type", "product_kind",
                "transaction_type_desc", "town", "line_name", "station_poi_desc_cd"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    return df


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2020-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2024-12-31")
    logger.info("Interval: %s to %s", start_date, end_date)

    # Parse date range to YYYYMM for filtering
    start_ym = start_date[:4] + start_date[5:7]
    end_ym = end_date[:4] + end_date[5:7]

    # Limit months per run to avoid OOM — default 6 months at a time
    month_limit = int(os.environ.get("ISTANBUL_MONTH_LIMIT", "6"))

    resources = get_resource_urls()
    # Filter to requested date range
    resources = [r for r in resources if start_ym <= r["yyyymm"] <= end_ym]
    logger.info("Found %d months in range %s to %s", len(resources), start_ym, end_ym)

    if month_limit > 0:
        resources = resources[:month_limit]
        logger.info("Processing %d months (ISTANBUL_MONTH_LIMIT=%d)", len(resources), month_limit)

    all_frames = []
    for i, res in enumerate(resources):
        logger.info("[%d/%d] Processing %s: %s", i + 1, len(resources), res["yyyymm"], res["name"])
        df = download_month_chunked(res["url"], res["yyyymm"])
        if df.empty:
            continue

        df = standardize_df(df)
        all_frames.append(df)
        logger.info("  %s: %d rows", res["yyyymm"], len(df))
        time.sleep(1)  # Be nice to the server

    if not all_frames:
        logger.warning("No hourly transport data fetched")
        return pd.DataFrame()

    result = pd.concat(all_frames, ignore_index=True)
    result["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Total hourly transport rows: %d", len(result))
    return result
