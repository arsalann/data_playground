"""@bruin
name: raw.istanbul_traffic_index
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Daily traffic congestion index for Istanbul (2015-present).
  Source: IBB Open Data Portal - https://data.ibb.gov.tr/en/dataset/istanbul-trafik-indeksi
  License: Istanbul Metropolitan Municipality Open Data License.
  Includes minimum, maximum, and average daily traffic index values.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: traffic_date
    type: DATE
    description: Date of the traffic index measurement
    primary_key: true
  - name: min_traffic_index
    type: DOUBLE
    description: Minimum traffic index value for the day (0-100 scale)
  - name: max_traffic_index
    type: DOUBLE
    description: Maximum traffic index value for the day (0-100 scale)
  - name: avg_traffic_index
    type: DOUBLE
    description: Average traffic index value for the day (0-100 scale)
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

TRAFFIC_INDEX_URL = "https://data.ibb.gov.tr/dataset/b3fbb6ce-03a5-4777-8c6c-111c73775523/resource/ba47eacb-a4e1-441c-ae51-0e622d4a18e2/download/traffic_index.csv"


def fetch_traffic_data() -> pd.DataFrame:
    logger.info("Downloading traffic index data ...")
    resp = requests.get(TRAFFIC_INDEX_URL, timeout=120)
    resp.raise_for_status()

    for enc in ("utf-8-sig", "utf-8", "iso-8859-9"):
        try:
            text = resp.content.decode(enc)
            df = pd.read_csv(StringIO(text))
            break
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue
    else:
        raise ValueError("Could not decode traffic index CSV")

    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={
        "trafficindexdate": "traffic_date",
        "minimum_traffic_index": "min_traffic_index",
        "maximum_traffic_index": "max_traffic_index",
        "average_traffic_index": "avg_traffic_index",
    })

    # Parse the date — format: "2015-08-06 00:00:00 +0000 +0000"
    df["traffic_date"] = pd.to_datetime(df["traffic_date"].str[:10], format="%Y-%m-%d").dt.date
    df["min_traffic_index"] = pd.to_numeric(df["min_traffic_index"], errors="coerce")
    df["max_traffic_index"] = pd.to_numeric(df["max_traffic_index"], errors="coerce")
    df["avg_traffic_index"] = pd.to_numeric(df["avg_traffic_index"], errors="coerce")

    df = df[["traffic_date", "min_traffic_index", "max_traffic_index", "avg_traffic_index"]]
    df = df.dropna(subset=["traffic_date"])
    df = df.drop_duplicates(subset=["traffic_date"])

    return df


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2015-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2025-12-31")
    logger.info("Interval: %s to %s", start_date, end_date)

    df = fetch_traffic_data()
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Total traffic index rows: %d", len(df))
    return df
