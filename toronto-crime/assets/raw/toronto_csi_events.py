"""@bruin
name: raw.toronto_csi_events
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Ingests Toronto Police Community Safety Indicator rows from the public
  ArcGIS FeatureServer. The source contains selected major crime categories
  from 2014 onward at offence and/or victim level; rows are not guaranteed to
  be unique incidents and point locations are privacy-offset.

  Source: https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/Major_Crime_Indicators_Open_Data/FeatureServer/0

materialization:
  type: table
  strategy: append

columns:
  - name: objectid
    type: INTEGER
    description: ArcGIS object identifier for the source row.
    primary_key: true
    checks:
      - name: not_null
  - name: event_unique_id
    type: VARCHAR
    description: Toronto Police event identifier; multiple rows can share an event when there are multiple offences or victims.
  - name: report_date
    type: TIMESTAMP
    description: Timestamp when the event was reported, as published by the source.
  - name: occurrence_date
    type: TIMESTAMP
    description: Timestamp when the event occurred, as published by the source.
  - name: report_year
    type: INTEGER
    description: Report year.
  - name: report_month
    type: VARCHAR
    description: Report month name.
  - name: report_day
    type: INTEGER
    description: Report day of month.
  - name: report_day_of_year
    type: INTEGER
    description: Report day of year.
  - name: report_day_of_week
    type: VARCHAR
    description: Report day of week.
  - name: report_hour
    type: INTEGER
    description: Report hour of day, 0-23.
  - name: occurrence_year
    type: INTEGER
    description: Occurrence year.
  - name: occurrence_month
    type: VARCHAR
    description: Occurrence month name.
  - name: occurrence_day
    type: INTEGER
    description: Occurrence day of month.
  - name: occurrence_day_of_year
    type: INTEGER
    description: Occurrence day of year.
  - name: occurrence_day_of_week
    type: VARCHAR
    description: Occurrence day of week.
  - name: occurrence_hour
    type: INTEGER
    description: Occurrence hour of day, 0-23.
  - name: division
    type: VARCHAR
    description: Toronto Police division code.
  - name: location_type
    type: VARCHAR
    description: Published location type.
  - name: premises_type
    type: VARCHAR
    description: Published premises type.
  - name: ucr_code
    type: VARCHAR
    description: Uniform Crime Reporting offence code.
  - name: ucr_ext
    type: VARCHAR
    description: Uniform Crime Reporting extension code.
  - name: offence
    type: VARCHAR
    description: Published offence description.
  - name: csi_category
    type: VARCHAR
    description: Community Safety Indicator category.
  - name: hood_158
    type: VARCHAR
    description: Current 158-neighbourhood model identifier, or NSA when unavailable.
  - name: neighbourhood_158
    type: VARCHAR
    description: Current 158-neighbourhood model label, or NSA when unavailable.
  - name: hood_140
    type: VARCHAR
    description: Historical 140-neighbourhood model identifier, or NSA when unavailable.
  - name: neighbourhood_140
    type: VARCHAR
    description: Historical 140-neighbourhood model label, or NSA when unavailable.
  - name: longitude
    type: DOUBLE
    description: Privacy-offset longitude in WGS84 decimal degrees.
  - name: latitude
    type: DOUBLE
    description: Privacy-offset latitude in WGS84 decimal degrees.
  - name: source_url
    type: VARCHAR
    description: ArcGIS REST endpoint queried for this row.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was extracted.

@bruin"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

QUERY_URL = (
    "https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/"
    "Major_Crime_Indicators_Open_Data/FeatureServer/0/query"
)
PAGE_SIZE = 2000
REQUEST_TIMEOUT = 120
MAX_RETRIES = 5


def _to_int(value: Any) -> int | None:
    if value in (None, "", " "):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_timestamp(value: Any) -> pd.Timestamp | None:
    if value in (None, "", " "):
        return None
    try:
        return pd.to_datetime(value, unit="ms", utc=True)
    except (TypeError, ValueError, OverflowError):
        return None


def _request_page(params: dict[str, Any]) -> dict[str, Any]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(QUERY_URL, params=params, timeout=REQUEST_TIMEOUT)
            if response.status_code in {429, 502, 503, 504}:
                raise requests.HTTPError(f"retryable status {response.status_code}")
            response.raise_for_status()
            payload = response.json()
            if "error" in payload:
                raise requests.HTTPError(str(payload["error"]))
            return payload
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            if attempt == MAX_RETRIES:
                raise
            delay = min(2 ** attempt, 30)
            logger.warning("ArcGIS request failed on attempt %d/%d: %s; sleeping %ss", attempt, MAX_RETRIES, exc, delay)
            time.sleep(delay)
    raise RuntimeError("unreachable retry state")


def fetch_events(start_date: str, end_date: str) -> pd.DataFrame:
    end_exclusive = (
        datetime.strptime(end_date, "%Y-%m-%d").date() + timedelta(days=1)
    ).isoformat()
    where = (
        f"OCC_DATE >= timestamp '{start_date} 00:00:00' "
        f"AND OCC_DATE < timestamp '{end_exclusive} 00:00:00'"
    )
    extracted_at = datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []
    offset = 0

    while True:
        params = {
            "where": where,
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson",
            "resultRecordCount": PAGE_SIZE,
            "resultOffset": offset,
            "orderByFields": "OBJECTID",
        }
        payload = _request_page(params)
        features = payload.get("features", [])
        if not features:
            logger.info("No features returned at offset %d", offset)
            break

        for feature in features:
            props = feature.get("properties", {})
            geometry = feature.get("geometry") or {}
            coords = geometry.get("coordinates") or [None, None]
            rows.append(
                {
                    "objectid": _to_int(props.get("OBJECTID")),
                    "event_unique_id": props.get("EVENT_UNIQUE_ID"),
                    "report_date": _to_timestamp(props.get("REPORT_DATE")),
                    "occurrence_date": _to_timestamp(props.get("OCC_DATE")),
                    "report_year": _to_int(props.get("REPORT_YEAR")),
                    "report_month": props.get("REPORT_MONTH"),
                    "report_day": _to_int(props.get("REPORT_DAY")),
                    "report_day_of_year": _to_int(props.get("REPORT_DOY")),
                    "report_day_of_week": str(props.get("REPORT_DOW") or "").strip() or None,
                    "report_hour": _to_int(props.get("REPORT_HOUR")),
                    "occurrence_year": _to_int(props.get("OCC_YEAR")),
                    "occurrence_month": props.get("OCC_MONTH"),
                    "occurrence_day": _to_int(props.get("OCC_DAY")),
                    "occurrence_day_of_year": _to_int(props.get("OCC_DOY")),
                    "occurrence_day_of_week": str(props.get("OCC_DOW") or "").strip() or None,
                    "occurrence_hour": _to_int(props.get("OCC_HOUR")),
                    "division": props.get("DIVISION"),
                    "location_type": props.get("LOCATION_TYPE"),
                    "premises_type": props.get("PREMISES_TYPE"),
                    "ucr_code": props.get("UCR_CODE"),
                    "ucr_ext": props.get("UCR_EXT"),
                    "offence": props.get("OFFENCE"),
                    "csi_category": props.get("CSI_CATEGORY"),
                    "hood_158": props.get("HOOD_158"),
                    "neighbourhood_158": props.get("NEIGHBOURHOOD_158"),
                    "hood_140": props.get("HOOD_140"),
                    "neighbourhood_140": props.get("NEIGHBOURHOOD_140"),
                    "longitude": coords[0] if coords[0] is not None else props.get("LONG_WGS84"),
                    "latitude": coords[1] if coords[1] is not None else props.get("LAT_WGS84"),
                    "source_url": QUERY_URL,
                    "extracted_at": extracted_at,
                }
            )

        logger.info("Fetched page offset=%d rows=%d total=%d", offset, len(features), len(rows))
        if not payload.get("exceededTransferLimit") and len(features) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.5)

    return pd.DataFrame(rows)


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2014-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", start_date)

    logger.info("Fetching Toronto CSI interval: %s to %s", start_date, end_date)
    df = fetch_events(start_date, end_date)
    logger.info("Fetched %d CSI rows", len(df))

    if df.empty:
        logger.warning("No CSI rows returned for interval %s to %s", start_date, end_date)
        return None

    return df
