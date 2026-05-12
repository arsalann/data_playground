"""@bruin

name: eu_mortality_raw.openmeteo_daily_temperature
description: |
  Daily 2-meter air temperature at every EU-27 NUTS3 centroid, supporting environmental
  health analysis and heat-mortality attribution studies. Foundation dataset for
  investigating temperature-related excess mortality across 1,165 European regions.

  Temperature measurements are sampled at GISCO label points (representative inside-polygon
  coordinates) for each NUTS3 region and aggregated to weekly climatology panels downstream.
  Used to compute heat anomalies against 2015-2019 baselines following ISGlobal/Ballester
  (2023) methodology for excess mortality attribution.

  Source: Open-Meteo Historical Weather API (ERA5-Seamless reanalysis), free for
  non-commercial use, no API key required.
  https://archive-api.open-meteo.com/v1/archive

  ERA5-Seamless is the ECMWF ERA5 reanalysis blended with surface observations
  on a 9km grid. Provides the same physical product as the Copernicus C3S Climate
  Data Store ERA5-Land but served via a simpler REST endpoint without registration.

  Technical implementation: One API request per NUTS3 region retrieving daily aggregates
  (temperature_2m_max, temperature_2m_mean, temperature_2m_min). Concurrent processing
  with 3 worker threads and 0.6s inter-request spacing respects Open-Meteo's free-tier
  limits (10,000 calls/day, ~10 req/s burst). Failed NUTS3 requests are logged but not
  retried; the staging layer handles missing rows through deduplication logic.
connection: bruin-playground-arsalan
tags:
  - eu-27
  - mortality
  - raw
  - open-meteo
  - era5
  - temperature
  - daily
  - environmental_health
  - climatology
  - heat_attribution
  - external_api
  - nuts3
  - reanalysis
  - weather_data

materialization:
  type: table
  strategy: create+replace
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: nuts_id
    type: VARCHAR
    description: NUTS3 region code (5 characters, e.g., FR101, DE111). Links to eu_mortality_raw.nuts3_reference. Primary identifier for European statistical regions at NUTS3 level (1,165 regions across EU-27).
    primary_key: true
    checks:
      - name: not_null
  - name: obs_date
    type: DATE
    description: Calendar observation date in UTC timezone. Daily resolution weather data from ERA5-Seamless reanalysis. Range spans 2015-2025 with actual coverage limited by API request dates.
    primary_key: true
    checks:
      - name: not_null
  - name: temp_min_c
    type: DOUBLE
    description: Daily minimum 2-meter air temperature in degrees Celsius. ERA5-Seamless reanalysis on 9km grid blended with surface observations. Used for heat/cold exposure analysis in mortality studies. Typical range 5-25°C for EU-27 regions.
    checks:
      - name: not_null
  - name: temp_mean_c
    type: DOUBLE
    description: Daily mean 2-meter air temperature in degrees Celsius. ERA5-Seamless reanalysis on 9km grid blended with surface observations. Primary metric for climatology baselines in mortality attribution studies.
    checks:
      - name: not_null
  - name: temp_max_c
    type: DOUBLE
    description: Daily maximum 2-meter air temperature in degrees Celsius. ERA5-Seamless reanalysis on 9km grid blended with surface observations. Critical for heat wave detection and excess mortality attribution (95th percentile used downstream).
    checks:
      - name: not_null
  - name: centroid_lat
    type: DOUBLE
    description: GISCO label-point latitude in decimal degrees (EPSG:4326). Representative inside-polygon coordinate from NUTS3 reference data. Used as sampling point for Open-Meteo API query. EU-27 range approximately 35-68°N.
  - name: centroid_lon
    type: DOUBLE
    description: GISCO label-point longitude in decimal degrees (EPSG:4326). Representative inside-polygon coordinate from NUTS3 reference data. Used as sampling point for Open-Meteo API query. EU-27 range approximately -10 to 30°E.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when data was ingested from Open-Meteo API. Single extraction per pipeline run. Used for deduplication in downstream staging (row_number() over partition by natural key order by extracted_at desc).

@bruin"""

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import requests
from google.cloud import bigquery

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

API_URL = "https://archive-api.open-meteo.com/v1/archive"
DAILY_VARS = ["temperature_2m_max", "temperature_2m_mean", "temperature_2m_min"]
NUTS_TABLE = "bruin-playground-arsalan.eu_mortality_raw.nuts3_reference"
MAX_WORKERS = 3
PER_REQUEST_SPACING_SEC = 0.6
PER_REQUEST_TIMEOUT_SEC = 120
MAX_RETRIES = 4

_pace_lock = threading.Lock()
_last_request_t = [0.0]


def _paced():
    """Enforce a per-worker minimum spacing between requests so total throughput
    stays below ~5 req/sec across MAX_WORKERS workers."""
    with _pace_lock:
        now = time.time()
        wait = (_last_request_t[0] + PER_REQUEST_SPACING_SEC) - now
        if wait > 0:
            time.sleep(wait)
        _last_request_t[0] = time.time()


def fetch_nuts(nuts_id: str, lat: float, lon: float, start: str, end: str) -> pd.DataFrame:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "daily": ",".join(DAILY_VARS),
        "timezone": "UTC",
    }
    last_err = None
    for attempt in range(MAX_RETRIES):
        _paced()
        try:
            r = requests.get(API_URL, params=params, timeout=PER_REQUEST_TIMEOUT_SEC)
            if r.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning("[%s] 429, sleeping %ds (attempt %d/%d)",
                               nuts_id, wait, attempt + 1, MAX_RETRIES)
                time.sleep(wait)
                continue
            if r.status_code in (502, 503, 504):
                wait = 15 * (attempt + 1)
                logger.warning("[%s] HTTP %d, retry in %ds", nuts_id, r.status_code, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            payload = r.json()
            daily = payload.get("daily")
            if not daily or "time" not in daily:
                logger.warning("[%s] No daily block", nuts_id)
                return pd.DataFrame()
            n = len(daily["time"])
            rows = []
            for i in range(n):
                rows.append({
                    "nuts_id": nuts_id,
                    "obs_date": daily["time"][i],
                    "temp_min_c": daily.get("temperature_2m_min", [None] * n)[i],
                    "temp_mean_c": daily.get("temperature_2m_mean", [None] * n)[i],
                    "temp_max_c": daily.get("temperature_2m_max", [None] * n)[i],
                    "centroid_lat": lat,
                    "centroid_lon": lon,
                })
            return pd.DataFrame(rows)
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            logger.warning("[%s] err attempt %d/%d: %s",
                           nuts_id, attempt + 1, MAX_RETRIES, e)
            time.sleep(wait)
            last_err = e
    logger.error("[%s] failed after %d retries: %s", nuts_id, MAX_RETRIES, last_err)
    return pd.DataFrame()


def load_nuts_centroids() -> pd.DataFrame:
    bq = bigquery.Client()
    sql = f"""
        SELECT nuts_id, centroid_lat, centroid_lon
        FROM `{NUTS_TABLE}`
        WHERE centroid_lat IS NOT NULL
          AND centroid_lon IS NOT NULL
        ORDER BY nuts_id
    """
    return bq.query(sql).to_dataframe()


def materialize():
    start = os.environ.get("OM_START_DATE", "2015-01-01")
    end = os.environ.get("OM_END_DATE", "2025-12-31")
    limit_env = os.environ.get("OM_NUTS_LIMIT")
    limit = int(limit_env) if limit_env else None

    points = load_nuts_centroids()
    if limit:
        points = points.head(limit)
        logger.info("Sample limit applied: %d NUTS3", len(points))
    else:
        logger.info("Fetching daily temperature for %d NUTS3 centroids (%s -> %s) with %d workers",
                    len(points), start, end, MAX_WORKERS)

    pieces = []
    failed = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(fetch_nuts, row["nuts_id"], float(row["centroid_lat"]),
                      float(row["centroid_lon"]), start, end): row["nuts_id"]
            for _, row in points.iterrows()
        }
        completed = 0
        for fut in as_completed(futures):
            nuts_id = futures[fut]
            try:
                df = fut.result()
                if df.empty:
                    failed.append(nuts_id)
                else:
                    pieces.append(df)
            except Exception as e:
                logger.error("[%s] unhandled: %s", nuts_id, e)
                failed.append(nuts_id)
            completed += 1
            if completed % 100 == 0:
                logger.info("Progress: %d / %d  (failed=%d)",
                            completed, len(points), len(failed))

    if not pieces:
        raise RuntimeError("No daily temperature data fetched -- all NUTS3 failed")

    out = pd.concat(pieces, ignore_index=True)
    out["obs_date"] = pd.to_datetime(out["obs_date"]).dt.date
    out["extracted_at"] = datetime.now(timezone.utc)
    out = out.drop_duplicates(subset=["nuts_id", "obs_date"], keep="last")

    if failed:
        logger.warning("Failed NUTS3 (%d): %s", len(failed), failed[:30])
    logger.info("Total rows: %d, NUTS3 succeeded: %d / %d, range %s..%s",
                len(out), out["nuts_id"].nunique(), len(points),
                out["obs_date"].min(), out["obs_date"].max())
    return out
