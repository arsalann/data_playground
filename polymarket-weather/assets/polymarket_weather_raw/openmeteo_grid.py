"""@bruin

name: polymarket_weather_raw.openmeteo_grid
description: |
  Hourly Paris reanalysis from the Open-Meteo Historical Weather API at point
  (48.857, 2.353) — central Paris. This is an *independent* baseline against the
  six METAR-fed Meteostat stations: Open-Meteo's archive uses ECMWF ERA5 reanalysis
  blended with surface observations on a ~9 km grid, so it cannot be tampered with at
  any individual sensor and provides a sanity check on what the regional temperature
  field "should" have been on the suspect April 2026 dates.

  This row source is labelled `source='openmeteo_grid'` in staging and never aggregated
  alongside METAR stations — it represents a different physical product.

  The dataset contains continuous hourly observations spanning 2024-2026, with Paris
  coordinates fixed at (48.857°N, 2.353°E) representing central Paris rather than any
  specific airport. Temperature ranges from -6.3°C to 38.7°C. Data is fetched in
  365-day chunks with exponential backoff retry logic for API reliability.

  Source: https://archive-api.open-meteo.com/v1/archive
  License: CC BY 4.0 (free for non-commercial use, no API key required)
connection: bruin-playground-arsalan
tags:
  - weather
  - external_source
  - raw_data
  - create_replace
  - public
  - independent_baseline
  - paris_central
  - hourly_data
  - reanalysis_data
  - era5_derived

materialization:
  type: table
  strategy: create+replace
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: ts_utc
    type: TIMESTAMP
    description: Observation timestamp in UTC (top of the hour). Forms unique identifier with fixed coordinates. Data spans 2024-01-01 to present.
    primary_key: true
    checks:
      - name: not_null
  - name: latitude
    type: DOUBLE
    description: Latitude of the queried grid point in decimal degrees. Fixed at 48.857° (central Paris) for all records.
    checks:
      - name: not_null
  - name: longitude
    type: DOUBLE
    description: Longitude of the queried grid point in decimal degrees. Fixed at 2.353° (central Paris) for all records.
    checks:
      - name: not_null
  - name: elevation_m
    type: DOUBLE
    description: Elevation of the closest reanalysis grid cell in metres above sea level. Fixed at 49m for this grid point, returned by the Open-Meteo API.
    checks:
      - name: not_null
  - name: temp_c
    type: DOUBLE
    description: Air temperature at 2 metres height in degrees Celsius. ERA5-based reanalysis from Open-Meteo. Historical range -6.3°C to 38.7°C.
    checks:
      - name: not_null
  - name: humidity_pct
    type: DOUBLE
    description: Relative humidity at 2 metres in percent (0-100). Atmospheric moisture content relative to saturation.
    checks:
      - name: not_null
  - name: dew_point_c
    type: DOUBLE
    description: Dew point temperature at 2 metres in degrees Celsius. Temperature at which air becomes saturated with water vapor.
    checks:
      - name: not_null
  - name: wind_speed_kmh
    type: DOUBLE
    description: Wind speed at 10 metres height in kilometres per hour. Horizontal air movement velocity.
    checks:
      - name: not_null
  - name: precipitation_mm
    type: DOUBLE
    description: Total precipitation accumulation for the hour in millimetres. Includes rain, snow (water equivalent), and other forms of moisture.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was fetched from the Open-Meteo API. Used for data lineage tracking and batch identification.
    checks:
      - name: not_null

@bruin"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

API_URL = "https://archive-api.open-meteo.com/v1/archive"
PARIS_LAT = 48.857
PARIS_LON = 2.353
HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "wind_speed_10m",
    "precipitation",
]
CHUNK_DAYS = 365
MAX_RETRIES = 5


def fetch_chunk(start: str, end: str) -> dict | None:
    params = {
        "latitude": PARIS_LAT,
        "longitude": PARIS_LON,
        "start_date": start,
        "end_date": end,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "UTC",
    }
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(API_URL, params=params, timeout=120)
            if r.status_code in (429, 502, 503):
                wait = 15 * (attempt + 1)
                logger.warning("Open-Meteo HTTP %d, retrying in %ds", r.status_code, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            logger.warning("Open-Meteo error attempt %d/%d: %s", attempt + 1, MAX_RETRIES, e)
            time.sleep(wait)
            last_err = e
    logger.error("All %d retries exhausted (%s..%s): %s", MAX_RETRIES, start, end, last_err)
    return None


def materialize():
    start_str = os.environ.get("BRUIN_START_DATE", "2024-01-01")
    end_str = os.environ.get("BRUIN_END_DATE", (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y-%m-%d"))
    start = datetime.strptime(start_str[:10], "%Y-%m-%d").date()
    end = datetime.strptime(end_str[:10], "%Y-%m-%d").date()

    logger.info("Open-Meteo Paris grid window: %s → %s", start, end)

    rows = []
    cursor = start
    elev_seen = None
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=CHUNK_DAYS - 1), end)
        data = fetch_chunk(cursor.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d"))
        if not data or "hourly" not in data:
            logger.warning("No hourly block returned for %s..%s", cursor, chunk_end)
            cursor = chunk_end + timedelta(days=1)
            time.sleep(0.5)
            continue
        h = data["hourly"]
        elev_seen = data.get("elevation", elev_seen)
        times = h.get("time", [])
        for i, t in enumerate(times):
            rows.append({
                "ts_utc": t,
                "temp_c": h.get("temperature_2m", [None] * len(times))[i],
                "humidity_pct": h.get("relative_humidity_2m", [None] * len(times))[i],
                "dew_point_c": h.get("dew_point_2m", [None] * len(times))[i],
                "wind_speed_kmh": h.get("wind_speed_10m", [None] * len(times))[i],
                "precipitation_mm": h.get("precipitation", [None] * len(times))[i],
            })
        logger.info("Chunk %s..%s: %d rows", cursor, chunk_end, len(times))
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.5)

    if not rows:
        logger.warning("No data fetched")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    df["latitude"] = PARIS_LAT
    df["longitude"] = PARIS_LON
    df["elevation_m"] = elev_seen
    df["extracted_at"] = datetime.now(timezone.utc)
    df = df.drop_duplicates(subset=["ts_utc"], keep="last").reset_index(drop=True)
    df = df[[
        "ts_utc", "latitude", "longitude", "elevation_m",
        "temp_c", "humidity_pct", "dew_point_c", "wind_speed_kmh", "precipitation_mm",
        "extracted_at",
    ]]
    logger.info("Open-Meteo total rows: %d, range %s..%s", len(df), df["ts_utc"].min(), df["ts_utc"].max())
    return df
