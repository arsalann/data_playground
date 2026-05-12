"""@bruin

name: eu_mortality_raw.openmeteo_daily_temperature
description: |
  Daily 2m air temperature at every EU-27 NUTS3 centroid, 2015-01-01 to 2025-12-31.

  Source: Open-Meteo Historical Weather API (ERA5-Seamless reanalysis), free for
  non-commercial use, no API key.

  https://archive-api.open-meteo.com/v1/archive

  ERA5-Seamless is the ECMWF ERA5 reanalysis blended with surface observations
  on a 9 km grid. It is the same physical product as the Copernicus C3S Climate
  Data Store ERA5-Land but served via a simpler REST endpoint without registration.

  One request per NUTS3 region. Daily aggregates:
    - temperature_2m_max
    - temperature_2m_mean
    - temperature_2m_min

  Rate-limit posture: 3 concurrent threads, each with a 0.5 s minimum spacing
  between requests, well within Open-Meteo's free-tier limits (10,000 calls/day,
  ~10 req/s burst). Failed NUTS3 are reported but not retried beyond the per-
  request backoff -- the staging layer tolerates missing rows.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace
image: python:3.11

tags:
  - eu-27
  - mortality
  - raw
  - open-meteo
  - era5
  - temperature
  - daily

columns:
  - name: nuts_id
    type: VARCHAR
    description: NUTS3 region code (matches eu_mortality_staging.em_nuts3_dim.nuts_id).
    primary_key: true
    checks:
      - name: not_null
  - name: obs_date
    type: DATE
    description: Calendar date (Open-Meteo timezone=UTC).
    primary_key: true
    checks:
      - name: not_null
  - name: temp_min_c
    type: DOUBLE
    description: Minimum 2m temperature on the day, degrees Celsius.
  - name: temp_mean_c
    type: DOUBLE
    description: Mean 2m temperature on the day, degrees Celsius.
  - name: temp_max_c
    type: DOUBLE
    description: Maximum 2m temperature on the day, degrees Celsius.
  - name: centroid_lat
    type: DOUBLE
    description: Sampled latitude (EPSG:4326).
  - name: centroid_lon
    type: DOUBLE
    description: Sampled longitude (EPSG:4326).
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp of ingestion.

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
