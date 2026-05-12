"""@bruin

name: fifa_raw.openmeteo_climatology
description: |
  Hourly ERA5 reanalysis from the Open-Meteo Historical Weather API at the
  Open-Meteo grid point of every FIFA-2026 host venue, for the June-July
  climatology window (2010-2024). Used downstream by H1 to compute the expected
  WBGT / heat-index distribution at each match's venue + kickoff hour.

  ERA5 is on a ~9 km grid, blended with surface observations, and cannot be
  tampered with at any individual sensor — so it provides an independent
  baseline against METAR-fed Meteostat stations. This source is labelled
  `source='openmeteo_grid'` in staging and is never aggregated alongside METAR
  stations (they represent different physical products).

  Source: https://archive-api.open-meteo.com/v1/archive (CC BY 4.0, no auth)
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - weather
  - era5_derived
  - external_source
  - raw_data

materialization:
  type: table
  strategy: create+replace
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: venue_id
    type: VARCHAR
    description: Foreign key to host_venues.venue_id.
    primary_key: true
    checks:
      - name: not_null
  - name: ts_utc
    type: TIMESTAMP
    description: Top-of-hour UTC.
    primary_key: true
    checks:
      - name: not_null
  - name: latitude
    type: DOUBLE
    checks:
      - name: not_null
  - name: longitude
    type: DOUBLE
    checks:
      - name: not_null
  - name: elevation_m
    type: DOUBLE
  - name: temp_c
    type: DOUBLE
    checks:
      - name: not_null
  - name: humidity_pct
    type: DOUBLE
  - name: dew_point_c
    type: DOUBLE
  - name: wind_speed_kmh
    type: DOUBLE
  - name: precipitation_mm
    type: DOUBLE
  - name: extracted_at
    type: TIMESTAMP
    checks:
      - name: not_null

@bruin"""

import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
import yaml

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

MANIFEST_PATH = Path(__file__).parent / "tournament_manifest.yml"
API_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "wind_speed_10m",
    "precipitation",
]
MAX_RETRIES = 5

# Climatology window: June 1 - July 31 of years 2010-2024 (15 years).
# Used as the recent-decade climatology for 2026 match-level expected
# apparent-temperature surface. The decadal-warming comparison is done in a
# separate asset (`openmeteo_baseline_1980s`) so a single rate-limited fetch
# can't block the recent-decade work.
CLIMATOLOGY_YEARS = list(range(2010, 2025))
SEASON_START_MD = (6, 1)   # June 1
SEASON_END_MD = (7, 31)    # July 31


def load_venues() -> list[dict]:
    with open(MANIFEST_PATH, "r") as f:
        manifest = yaml.safe_load(f)
    return [
        {
            "venue_id": v["venue_id"],
            "lat": float(v["openmeteo_grid"]["lat"]),
            "lon": float(v["openmeteo_grid"]["lon"]),
        }
        for v in manifest["venues"]
    ]


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
            logger.warning("Open-Meteo attempt %d/%d: %s", attempt + 1, MAX_RETRIES, e)
            time.sleep(wait)
            last_err = e
    logger.error("All retries exhausted (%s..%s): %s", start, end, last_err)
    return None


def fetch_venue(venue_id: str, lat: float, lon: float) -> pd.DataFrame:
    rows = []
    elev_seen = None
    for year in CLIMATOLOGY_YEARS:
        start = date(year, SEASON_START_MD[0], SEASON_START_MD[1])
        end = date(year, SEASON_END_MD[0], SEASON_END_MD[1])
        data = fetch_chunk(lat, lon, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        if not data or "hourly" not in data:
            logger.warning("[%s/%d] No hourly block returned", venue_id, year)
            time.sleep(0.5)
            continue
        h = data["hourly"]
        elev_seen = data.get("elevation", elev_seen)
        times = h.get("time", [])
        for i, t in enumerate(times):
            rows.append({
                "venue_id": venue_id,
                "ts_utc": t,
                "temp_c": h.get("temperature_2m", [None] * len(times))[i],
                "humidity_pct": h.get("relative_humidity_2m", [None] * len(times))[i],
                "dew_point_c": h.get("dew_point_2m", [None] * len(times))[i],
                "wind_speed_kmh": h.get("wind_speed_10m", [None] * len(times))[i],
                "precipitation_mm": h.get("precipitation", [None] * len(times))[i],
            })
        logger.info("[%s/%d] %d rows", venue_id, year, len(times))
        time.sleep(0.5)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["latitude"] = lat
    df["longitude"] = lon
    df["elevation_m"] = elev_seen
    return df


def materialize():
    venues = load_venues()
    logger.info("Climatology window: %s-%s of years %d-%d, venues=%d",
                SEASON_START_MD, SEASON_END_MD, CLIMATOLOGY_YEARS[0], CLIMATOLOGY_YEARS[-1], len(venues))

    pieces = []
    for v in venues:
        df = fetch_venue(v["venue_id"], v["lat"], v["lon"])
        if not df.empty:
            pieces.append(df)
            logger.info("[%s] total rows: %d", v["venue_id"], len(df))

    if not pieces:
        return pd.DataFrame()

    df = pd.concat(pieces, ignore_index=True)
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    df["extracted_at"] = datetime.now(timezone.utc)
    df = df.drop_duplicates(subset=["venue_id", "ts_utc"], keep="last").reset_index(drop=True)
    df = df[[
        "venue_id", "ts_utc", "latitude", "longitude", "elevation_m",
        "temp_c", "humidity_pct", "dew_point_c", "wind_speed_kmh", "precipitation_mm",
        "extracted_at",
    ]]
    logger.info("Open-Meteo total rows: %d, range %s..%s",
                len(df), df["ts_utc"].min(), df["ts_utc"].max())
    return df
