"""@bruin

name: polymarket_weather_raw.openmeteo_grid
description: |
  Hourly reanalysis from the Open-Meteo Historical Weather API at the city-centre
  grid point of every city in `city_manifest.yml` (Paris, London, Seoul, Toronto).
  This is an *independent* baseline against the METAR-fed Meteostat stations:
  Open-Meteo's archive uses ECMWF ERA5 reanalysis blended with surface observations
  on a ~9 km grid, so it cannot be tampered with at any individual sensor and
  provides a sanity check on what the regional temperature field "should" have
  been on any suspect date.

  Each row is keyed by (city, ts_utc). The lat/lon stored on each row is the grid
  point queried, taken verbatim from the manifest. This source is labelled
  `source='openmeteo_grid'` in staging and never aggregated alongside METAR
  stations — it represents a different physical product.

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
  - multi_city
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
  - name: city
    type: VARCHAR
    description: City identifier from the manifest (Paris, London, Seoul, Toronto). Composite primary key with ts_utc.
    primary_key: true
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Paris
          - London
          - Seoul
          - Toronto
  - name: ts_utc
    type: TIMESTAMP
    description: Observation timestamp in UTC (top of the hour).
    primary_key: true
    checks:
      - name: not_null
  - name: latitude
    type: DOUBLE
    description: Latitude of the queried grid point in decimal degrees.
    checks:
      - name: not_null
  - name: longitude
    type: DOUBLE
    description: Longitude of the queried grid point in decimal degrees.
    checks:
      - name: not_null
  - name: elevation_m
    type: DOUBLE
    description: Elevation of the closest reanalysis grid cell in metres above sea level.
  - name: temp_c
    type: DOUBLE
    description: Air temperature at 2 metres height in degrees Celsius.
    checks:
      - name: not_null
  - name: humidity_pct
    type: DOUBLE
    description: Relative humidity at 2 metres in percent (0-100).
  - name: dew_point_c
    type: DOUBLE
    description: Dew point temperature at 2 metres in degrees Celsius.
  - name: wind_speed_kmh
    type: DOUBLE
    description: Wind speed at 10 metres height in kilometres per hour.
  - name: precipitation_mm
    type: DOUBLE
    description: Total precipitation accumulation for the hour in millimetres.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was fetched from the Open-Meteo API.
    checks:
      - name: not_null

@bruin"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
import yaml

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

MANIFEST_PATH = Path(__file__).parent / "city_manifest.yml"
API_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "wind_speed_10m",
    "precipitation",
]
CHUNK_DAYS = 365
MAX_RETRIES = 5


def load_grid_points() -> list[dict]:
    with open(MANIFEST_PATH, "r") as f:
        manifest = yaml.safe_load(f)
    points = []
    for c in manifest["cities"]:
        og = c["openmeteo_grid"]
        points.append({"city": c["name"], "lat": float(og["lat"]), "lon": float(og["lon"])})
    return points


def fetch_chunk(lat: float, lon: float, start: str, end: str) -> dict | None:
    params = {
        "latitude": lat,
        "longitude": lon,
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


def fetch_city(city: str, lat: float, lon: float, start, end) -> pd.DataFrame:
    rows = []
    cursor = start
    elev_seen = None
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=CHUNK_DAYS - 1), end)
        data = fetch_chunk(lat, lon, cursor.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d"))
        if not data or "hourly" not in data:
            logger.warning("[%s] No hourly block returned for %s..%s", city, cursor, chunk_end)
            cursor = chunk_end + timedelta(days=1)
            time.sleep(0.5)
            continue
        h = data["hourly"]
        elev_seen = data.get("elevation", elev_seen)
        times = h.get("time", [])
        for i, t in enumerate(times):
            rows.append({
                "city": city,
                "ts_utc": t,
                "temp_c": h.get("temperature_2m", [None] * len(times))[i],
                "humidity_pct": h.get("relative_humidity_2m", [None] * len(times))[i],
                "dew_point_c": h.get("dew_point_2m", [None] * len(times))[i],
                "wind_speed_kmh": h.get("wind_speed_10m", [None] * len(times))[i],
                "precipitation_mm": h.get("precipitation", [None] * len(times))[i],
            })
        logger.info("[%s] Chunk %s..%s: %d rows", city, cursor, chunk_end, len(times))
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.5)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["latitude"] = lat
    df["longitude"] = lon
    df["elevation_m"] = elev_seen
    return df


def materialize():
    start_str = os.environ.get("BRUIN_START_DATE", "2026-01-01")
    end_str = os.environ.get("BRUIN_END_DATE", "2026-04-30")
    start = datetime.strptime(start_str[:10], "%Y-%m-%d").date()
    end = datetime.strptime(end_str[:10], "%Y-%m-%d").date()

    points = load_grid_points()
    logger.info("Window: %s → %s, cities=%d", start, end, len(points))

    pieces = []
    for p in points:
        df = fetch_city(p["city"], p["lat"], p["lon"], start, end)
        if not df.empty:
            pieces.append(df)
            logger.info("[%s] total rows: %d", p["city"], len(df))

    if not pieces:
        logger.warning("No data fetched")
        return pd.DataFrame()

    df = pd.concat(pieces, ignore_index=True)
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    df["extracted_at"] = datetime.now(timezone.utc)
    df = df.drop_duplicates(subset=["city", "ts_utc"], keep="last").reset_index(drop=True)
    df = df[[
        "city", "ts_utc", "latitude", "longitude", "elevation_m",
        "temp_c", "humidity_pct", "dew_point_c", "wind_speed_kmh", "precipitation_mm",
        "extracted_at",
    ]]
    logger.info("Open-Meteo total rows: %d, range %s..%s", len(df), df["ts_utc"].min(), df["ts_utc"].max())
    return df
