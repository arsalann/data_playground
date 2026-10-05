"""@bruin
name: raw.era5_city_daily
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Daily ERA5 reanalysis weather for every city in raw.atlas_cities, fetched from
  the Open-Meteo Historical Weather API (https://open-meteo.com/en/docs/historical-weather-api)
  with models=era5. ERA5 is produced by ECMWF for the Copernicus Climate Change
  Service (C3S); Open-Meteo data is CC BY 4.0. Values are for the ~0.25 degree
  grid cell nearest the GHSL urban-centre centroid, aggregated to local calendar
  days (timezone=auto).

  Chosen over NOAA station data because NOAA GSOD 2026 is not published and
  GHCN-Daily 2026 has a qualifying station within 50 km of only 16% of the
  1M+ cities.

  Only the summer season of each year in the run interval is fetched
  (SEASON_START / SEASON_END as MM-DD, default 06-01 to 09-20). Resumable:
  city-seasons already complete in the table are skipped, so a run that hits the
  Open-Meteo daily limit can simply be re-run the next day.

depends:
  - raw.atlas_cities

materialization:
  type: table
  strategy: append

columns:
  - name: ghsl_id
    type: INTEGER
    description: GHSL urban centre identifier (joins to raw.atlas_cities).
    primary_key: true
  - name: date
    type: DATE
    description: Local calendar date at the city.
    primary_key: true
  - name: tmax_c
    type: DOUBLE
    description: Daily maximum 2 m air temperature (degrees C).
  - name: apparent_tmax_c
    type: DOUBLE
    description: Daily maximum apparent ("feels like") temperature combining heat, humidity and wind (degrees C).
  - name: precip_mm
    type: DOUBLE
    description: Daily total precipitation (mm).
  - name: grid_latitude
    type: DOUBLE
    description: Latitude of the ERA5 grid cell used (WGS84 degrees).
  - name: grid_longitude
    type: DOUBLE
    description: Longitude of the ERA5 grid cell used (WGS84 degrees).
  - name: grid_elevation_m
    type: DOUBLE
    description: Elevation used by Open-Meteo for the grid cell (m).
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was fetched.

@bruin"""

import logging
import os
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import requests
from google.cloud import bigquery

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ID = "bruin-playground-arsalan"
API = "https://archive-api.open-meteo.com/v1/archive"
DAILY = ["temperature_2m_max", "apparent_temperature_max", "precipitation_sum"]
BATCH = int(os.environ.get("ERA5_BATCH_SIZE", "20"))
SLEEP_S = float(os.environ.get("ERA5_SLEEP_S", "15"))
SEASON_START = os.environ.get("SEASON_START", "06-01")
SEASON_END = os.environ.get("SEASON_END", "09-20")
MAX_RETRIES = 8


class RateLimited(Exception):
    pass


def empty_frame() -> pd.DataFrame:
    """Typed zero-row frame - an untyped one makes the loader rewrite the table schema as STRING."""
    return pd.DataFrame({
        "ghsl_id": pd.Series(dtype="int64"), "date": pd.Series(dtype=pd.ArrowDtype(pa.date32())),
        "tmax_c": pd.Series(dtype="float64"), "apparent_tmax_c": pd.Series(dtype="float64"),
        "precip_mm": pd.Series(dtype="float64"), "grid_latitude": pd.Series(dtype="float64"),
        "grid_longitude": pd.Series(dtype="float64"), "grid_elevation_m": pd.Series(dtype="float64"),
        "extracted_at": pd.Series(dtype="datetime64[ns, UTC]"),
    })


class DailyLimit(Exception):
    pass


def season_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Clip the run interval to the summer season of each calendar year it touches."""
    windows = []
    for year in range(start.year, end.year + 1):
        ws = max(start, date.fromisoformat(f"{year}-{SEASON_START}"))
        we = min(end, date.fromisoformat(f"{year}-{SEASON_END}"))
        if ws <= we:
            windows.append((ws, we))
    return windows


def load_cities(bq: bigquery.Client) -> list[dict]:
    q = f"SELECT ghsl_id, latitude, longitude FROM `{PROJECT_ID}.raw.atlas_cities` ORDER BY population_2015 DESC"
    cities = [dict(r) for r in bq.query(q).result()]
    limit = os.environ.get("CITY_LIMIT")
    return cities[: int(limit)] if limit else cities


def loaded_cities(bq: bigquery.Client, ws: date, we: date) -> set[int]:
    """Cities that already have every day of the window in the raw table."""
    expected = (we - ws).days + 1
    q = f"""
        SELECT ghsl_id
        FROM `{PROJECT_ID}.raw.era5_city_daily`
        WHERE date BETWEEN '{ws}' AND '{we}'
        GROUP BY 1
        HAVING COUNT(DISTINCT date) >= {expected}
    """
    try:
        return {r.ghsl_id for r in bq.query(q).result()}
    except Exception as exc:  # table may not exist on the first run
        logger.info("No existing rows found (%s)", str(exc)[:80])
        return set()


def wait_for_limit(reason: str, attempt: int) -> None:
    if "Daily" in reason:
        raise DailyLimit(reason)
    if "Hourly" in reason:
        now = datetime.now(timezone.utc)
        wait = int((now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1) - now).total_seconds()) + 60
    else:
        wait = 65 * attempt
    logger.warning("Open-Meteo limit (%s), sleeping %ds", reason[:60], wait)
    time.sleep(wait)


def fetch_batch(session: requests.Session, batch: list[dict], start: str, end: str) -> list[dict]:
    params = {
        "latitude": ",".join(f"{c['latitude']:.4f}" for c in batch),
        "longitude": ",".join(f"{c['longitude']:.4f}" for c in batch),
        "start_date": start,
        "end_date": end,
        "daily": ",".join(DAILY),
        "models": "era5",
        "timezone": "auto",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(API, params=params, timeout=120)
            if resp.status_code == 429:
                try:
                    reason = resp.json().get("reason", resp.text[:200])
                except ValueError:
                    reason = resp.text[:200]
                raise RateLimited(reason)
            if resp.status_code in (500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {resp.status_code}")
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else [data]
        except RateLimited as exc:
            if attempt == MAX_RETRIES:
                raise
            wait_for_limit(str(exc), attempt)
        except (requests.HTTPError, requests.ConnectionError, requests.Timeout, ValueError) as exc:
            # ValueError covers truncated / malformed JSON bodies
            if attempt == MAX_RETRIES:
                raise
            wait = 2 ** attempt
            logger.warning("Request failed (%s), retry %d in %ds", exc, attempt, wait)
            time.sleep(wait)
    raise RuntimeError("unreachable")


def materialize():
    start = date.fromisoformat(os.environ.get("BRUIN_START_DATE", "2026-06-01")[:10])
    end = date.fromisoformat(os.environ.get("BRUIN_END_DATE", "2026-09-20")[:10])
    logger.info("Interval: %s to %s (season %s to %s)", start, end, SEASON_START, SEASON_END)

    bq = bigquery.Client(project=PROJECT_ID)
    cities = load_cities(bq)
    logger.info("Loaded %d cities", len(cities))
    session = requests.Session()

    rows = []
    stop = False
    for ws, we in season_windows(start, end):
        done = loaded_cities(bq, ws, we)
        todo = [c for c in cities if int(c["ghsl_id"]) not in done]
        logger.info("Season %s to %s: %d cities already loaded, %d to fetch", ws, we, len(done), len(todo))
        for i in range(0, len(todo), BATCH):
            batch = todo[i : i + BATCH]
            try:
                results = fetch_batch(session, batch, ws.isoformat(), we.isoformat())
            except (RateLimited, DailyLimit) as exc:
                logger.warning("Stopping on rate limit (%s) - returning %d rows fetched so far", str(exc)[:80], len(rows))
                stop = True
                break
            for city, res in zip(batch, results):
                d = res["daily"]
                for j, day in enumerate(d["time"]):
                    rows.append(
                        {
                            "ghsl_id": int(city["ghsl_id"]),
                            "date": day,
                            "tmax_c": d["temperature_2m_max"][j],
                            "apparent_tmax_c": d["apparent_temperature_max"][j],
                            "precip_mm": d["precipitation_sum"][j],
                            "grid_latitude": res["latitude"],
                            "grid_longitude": res["longitude"],
                            "grid_elevation_m": res.get("elevation"),
                        }
                    )
            logger.info("Season %s: fetched cities %d-%d of %d (%d rows so far)", ws.year, i + 1, i + len(batch), len(todo), len(rows))
            time.sleep(SLEEP_S)
        if stop:
            break

    if not rows:
        logger.info("Nothing new to load")
        return empty_frame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Fetched %d city-days", len(df))
    return df
