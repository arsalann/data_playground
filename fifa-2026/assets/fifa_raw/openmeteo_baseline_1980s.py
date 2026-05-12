"""@bruin

name: fifa_raw.openmeteo_baseline_1980s
description: |
  Hourly ERA5 reanalysis from the Open-Meteo Historical Weather API at the
  Open-Meteo grid point of every FIFA-2026 host venue, restricted to the
  1980-1989 June-July baseline decade. Paired with `openmeteo_climatology`
  (2010-2024) so a decadal-warming comparison per venue can be computed in
  staging without disturbing the recent-decade climatology used for 2026
  match-level expected weather.

  Fetched separately because the much-older years are sometimes rate-limited
  more aggressively by the Open-Meteo archive, and we don't want one slow
  fetch to invalidate the recent-decade table.

  Source: https://archive-api.open-meteo.com/v1/archive (CC BY 4.0, no auth)
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - weather
  - era5_derived
  - external_source
  - raw_data
  - climate_baseline

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
  - name: temp_c
    type: DOUBLE
    checks:
      - name: not_null
  - name: humidity_pct
    type: DOUBLE
  - name: wind_speed_kmh
    type: DOUBLE
  - name: extracted_at
    type: TIMESTAMP
    checks:
      - name: not_null

@bruin"""

import logging
import os
import time
from datetime import date, datetime, timezone
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
HOURLY_VARS = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"]
MAX_RETRIES = 4

CLIMATOLOGY_YEARS = list(range(1980, 1990))
SEASON_START_MD = (6, 1)
SEASON_END_MD = (7, 31)


def load_venues() -> list[dict]:
    with open(MANIFEST_PATH, "r") as f:
        manifest = yaml.safe_load(f)
    return [
        {"venue_id": v["venue_id"], "lat": float(v["openmeteo_grid"]["lat"]), "lon": float(v["openmeteo_grid"]["lon"])}
        for v in manifest["venues"]
    ]


def fetch_decade(lat: float, lon: float) -> dict | None:
    """Single multi-year request: pull 1980-01-01 .. 1989-12-31 in one shot,
    then filter to June-July client-side. Far fewer API calls than year-by-year."""
    start = date(CLIMATOLOGY_YEARS[0], 1, 1)
    end = date(CLIMATOLOGY_YEARS[-1], 12, 31)
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": end.strftime("%Y-%m-%d"),
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "UTC",
    }
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(API_URL, params=params, timeout=180)
            if r.status_code in (429, 502, 503):
                wait = 30 * (attempt + 1)
                logger.warning("Open-Meteo HTTP %d, retrying in %ds", r.status_code, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            wait = 15 * (attempt + 1)
            logger.warning("Open-Meteo attempt %d/%d failed: %s", attempt + 1, MAX_RETRIES, e)
            time.sleep(wait)
    return None


def fetch_venue(venue_id: str, lat: float, lon: float) -> pd.DataFrame:
    data = fetch_decade(lat, lon)
    if not data or "hourly" not in data:
        logger.warning("[%s] No hourly block returned", venue_id)
        return pd.DataFrame()
    h = data["hourly"]
    times = h.get("time", [])
    rows = []
    for i, t in enumerate(times):
        ts = pd.to_datetime(t, utc=True)
        if ts.month not in (6, 7):
            continue
        rows.append({
            "venue_id": venue_id,
            "ts_utc": ts,
            "temp_c": h.get("temperature_2m", [None] * len(times))[i],
            "humidity_pct": h.get("relative_humidity_2m", [None] * len(times))[i],
            "wind_speed_kmh": h.get("wind_speed_10m", [None] * len(times))[i],
        })
    logger.info("[%s] kept %d June-July rows", venue_id, len(rows))
    return pd.DataFrame(rows)


def materialize():
    venues = load_venues()
    logger.info("Fetching 1980-1989 baseline at %d venues (one multi-year request each)", len(venues))
    pieces = []
    for v in venues:
        df = fetch_venue(v["venue_id"], v["lat"], v["lon"])
        if not df.empty:
            pieces.append(df)
        time.sleep(2)
    if not pieces:
        return pd.DataFrame()
    df = pd.concat(pieces, ignore_index=True)
    df["extracted_at"] = datetime.now(timezone.utc)
    df = df.drop_duplicates(subset=["venue_id", "ts_utc"], keep="last").reset_index(drop=True)
    logger.info("baseline 1980s rows: %d, range %s..%s",
                len(df), df["ts_utc"].min(), df["ts_utc"].max())
    return df
