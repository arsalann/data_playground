"""@bruin
name: raw.hk_transit_mtr_csv
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches four MTR open data CSV datasets and loads them into BigQuery raw tables
  with full refresh (WRITE_TRUNCATE).

  Tables created:
    - raw.hk_transit_mtr_lines_stations (heavy rail line and station data)
    - raw.hk_transit_mtr_bus_stops (MTR feeder bus routes and stops)
    - raw.hk_transit_mtr_fares (station-to-station fare table)
    - raw.hk_transit_mtr_light_rail_stops (Light Rail routes and stops)

  Important: MTR portal drops connections without a User-Agent header.
  Important: CSVs have UTF-8 BOM — must decode with utf-8-sig.

  Data source: https://opendata.mtr.com.hk

materialization:
  type: table
  strategy: create+replace

columns:
  - name: table_name
    type: VARCHAR
    description: Name of the MTR table loaded
    primary_key: true
  - name: row_count
    type: INTEGER
    description: Number of rows loaded into the table
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the data was extracted

@bruin"""

import io
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import requests
from google.cloud import bigquery

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ID = "bruin-playground-arsalan"
DATASET = "raw"

# User-Agent is required — MTR portal drops connections without it
HEADERS = {"User-Agent": "Mozilla/5.0"}

# MTR CSV datasets and their target table names
MTR_DATASETS = {
    "hk_transit_mtr_lines_stations": "https://opendata.mtr.com.hk/data/mtr_lines_and_stations.csv",
    "hk_transit_mtr_bus_stops": "https://opendata.mtr.com.hk/data/mtr_bus_stops.csv",
    "hk_transit_mtr_fares": "https://opendata.mtr.com.hk/data/mtr_lines_fares.csv",
    "hk_transit_mtr_light_rail_stops": "https://opendata.mtr.com.hk/data/light_rail_routes_and_stops.csv",
}


def fetch_mtr_csv(url: str) -> pd.DataFrame:
    """Fetch a CSV from MTR portal with required headers and BOM handling."""
    logger.info("Fetching %s", url)
    response = requests.get(url, headers=HEADERS, timeout=60)
    response.raise_for_status()

    # Decode with utf-8-sig to strip BOM characters
    content = response.content.decode("utf-8-sig")
    df = pd.read_csv(io.StringIO(content), dtype=str, keep_default_na=False)
    logger.info("Fetched %d rows, %d columns", len(df), len(df.columns))
    return df


def materialize():
    client = bigquery.Client(project=PROJECT_ID)
    summary = []
    now = datetime.now(timezone.utc)

    for table_name, url in MTR_DATASETS.items():
        try:
            df = fetch_mtr_csv(url)
            df["extracted_at"] = now

            table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()

            logger.info("Loaded %d rows into %s", len(df), table_id)
            summary.append(
                {
                    "table_name": table_name,
                    "row_count": len(df),
                    "extracted_at": now,
                }
            )
        except Exception as e:
            logger.error("Failed to load %s: %s", table_name, e)
            summary.append(
                {
                    "table_name": table_name,
                    "row_count": 0,
                    "extracted_at": now,
                }
            )

    logger.info("MTR CSV ingestion complete: %d tables loaded", len(summary))
    return pd.DataFrame(summary)
