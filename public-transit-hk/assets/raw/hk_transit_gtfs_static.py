"""@bruin
name: raw.hk_transit_gtfs_static
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Downloads the Hong Kong GTFS static feed ZIP from data.gov.hk, extracts CSV files,
  and loads them into BigQuery raw tables with full refresh (WRITE_TRUNCATE).

  Tables created:
    - raw.hk_transit_stops (stop locations)
    - raw.hk_transit_routes (route definitions)
    - raw.hk_transit_trips (trip-route-service associations)
    - raw.hk_transit_stop_times (arrival/departure times per stop per trip — large, >100 MB)
    - raw.hk_transit_calendar (service day patterns)

  Data source: https://data.gov.hk
  Coverage: KMB buses, CTB/NWFB Citybus, trams, ferries
  Note: MTR does not publish GTFS data — heavy rail is not included.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: table_name
    type: VARCHAR
    description: Name of the GTFS table loaded
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
import tempfile
import zipfile
from datetime import datetime, timezone

import pandas as pd
import requests
from google.cloud import bigquery

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Hong Kong GTFS static feed URL — verify at https://data.gov.hk
GTFS_ZIP_URL = "https://static.data.gov.hk/td/pt-headway-en/gtfs.zip"
PROJECT_ID = "bruin-playground-arsalan"
DATASET = "raw"

# GTFS files to extract and their target table names
GTFS_FILES = {
    "stops.txt": "hk_transit_stops",
    "routes.txt": "hk_transit_routes",
    "trips.txt": "hk_transit_trips",
    "stop_times.txt": "hk_transit_stop_times",
    "calendar.txt": "hk_transit_calendar",
}


def download_gtfs_zip() -> bytes:
    """Download the GTFS ZIP archive."""
    logger.info("Downloading GTFS ZIP from %s", GTFS_ZIP_URL)
    response = requests.get(GTFS_ZIP_URL, timeout=300)
    response.raise_for_status()
    logger.info("Downloaded %.1f MB", len(response.content) / (1024 * 1024))
    return response.content


def load_small_table(client, zip_ref, filename, table_name):
    """Load a small GTFS file (<100 MB) via DataFrame."""
    with zip_ref.open(filename) as f:
        df = pd.read_csv(f, dtype=str, keep_default_na=False)

    df["extracted_at"] = datetime.now(timezone.utc)
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    logger.info("Loaded %d rows into %s", len(df), table_id)
    return len(df)


def load_large_table(client, zip_ref, filename, table_name):
    """Load a large GTFS file (>100 MB) via BigQuery load job from file."""
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"

    with zip_ref.open(filename) as f:
        raw_bytes = f.read()

    # Add extracted_at column to CSV content
    lines = raw_bytes.decode("utf-8").splitlines()
    header = lines[0] + ",extracted_at"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    data_lines = [line + f",{now_str}" for line in lines[1:]]
    modified_csv = "\n".join([header] + data_lines)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_file(
        io.BytesIO(modified_csv.encode("utf-8")),
        table_id,
        job_config=job_config,
    )
    job.result()

    row_count = len(data_lines)
    logger.info("Loaded %d rows into %s (large file mode)", row_count, table_id)
    return row_count


def materialize():
    zip_content = download_gtfs_zip()
    client = bigquery.Client(project=PROJECT_ID)

    summary = []
    now = datetime.now(timezone.utc)

    with tempfile.NamedTemporaryFile(suffix=".zip") as tmp:
        tmp.write(zip_content)
        tmp.flush()

        with zipfile.ZipFile(tmp.name, "r") as zip_ref:
            available_files = zip_ref.namelist()
            logger.info("ZIP contains: %s", available_files)

            for filename, table_name in GTFS_FILES.items():
                if filename not in available_files:
                    logger.warning("File %s not found in ZIP, skipping", filename)
                    continue

                # Use large file mode for stop_times (>100 MB)
                if filename == "stop_times.txt":
                    row_count = load_large_table(client, zip_ref, filename, table_name)
                else:
                    row_count = load_small_table(client, zip_ref, filename, table_name)

                summary.append(
                    {
                        "table_name": table_name,
                        "row_count": row_count,
                        "extracted_at": now,
                    }
                )

    logger.info("GTFS ingestion complete: %d tables loaded", len(summary))
    return pd.DataFrame(summary)
