"""@bruin
name: raw.firms_modis_fires
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Global MODIS active-fire / thermal-anomaly detections from NASA FIRMS
  (Fire Information for Resource Management System), one row per detection.
  Pulled day by day from the FIRMS area API for the whole world
  (https://firms.modaps.eosdis.nasa.gov/api/area/). Days covered by the
  science-quality Standard Processing archive use MODIS_SP; later days use the
  Near Real-Time stream MODIS_NRT. The switch date is read from the FIRMS
  data-availability endpoint at run time.

  MODIS Collection 6.1, Terra + Aqua, ~1 km pixels. NASA data is free to use
  with attribution (https://www.earthdata.nasa.gov/engage/open-data-services-software-policies).
  Requires a free FIRMS MAP_KEY (5,000 transactions per 10 minutes).

  Only the summer season of each year in the run interval is fetched
  (SEASON_START / SEASON_END as MM-DD, default 06-01 to 09-20). Resumable: days
  already present in the table are skipped.

materialization:
  type: table
  strategy: append

secrets:
  - key: nasa-firms-map-key
    inject_as: FIRMS_MAP_KEY

columns:
  - name: detection_id
    type: VARCHAR
    description: Natural key - latitude, longitude, acq_date, acq_time and satellite joined with underscores.
    primary_key: true
  - name: latitude
    type: DOUBLE
    description: Centre latitude of the fire pixel (WGS84 degrees).
  - name: longitude
    type: DOUBLE
    description: Centre longitude of the fire pixel (WGS84 degrees).
  - name: acq_date
    type: DATE
    description: Satellite overpass date (UTC).
  - name: acq_time
    type: INTEGER
    description: Satellite overpass time (UTC, HHMM as an integer, e.g. 3 = 00:03).
  - name: satellite
    type: VARCHAR
    description: Satellite platform (Terra or Aqua).
  - name: brightness
    type: DOUBLE
    description: Channel 21/22 brightness temperature of the fire pixel (Kelvin).
  - name: scan
    type: DOUBLE
    description: Along-scan pixel size (km).
  - name: track
    type: DOUBLE
    description: Along-track pixel size (km).
  - name: instrument
    type: VARCHAR
    description: Sensor name (MODIS).
  - name: confidence
    type: INTEGER
    description: Detection confidence (0-100 %). FIRMS classes - low < 30, nominal 30-79, high >= 80.
  - name: version
    type: VARCHAR
    description: Collection and processing version (e.g. 61.03 for SP, 6.1NRT for NRT).
  - name: bright_t31
    type: DOUBLE
    description: Channel 31 brightness temperature of the fire pixel (Kelvin).
  - name: frp
    type: DOUBLE
    description: Fire Radiative Power (MW).
  - name: daynight
    type: VARCHAR
    description: D = daytime overpass, N = nighttime overpass.
  - name: fire_type
    type: INTEGER
    description: SP only - 0 presumed vegetation fire, 1 active volcano, 2 other static land source, 3 offshore. NULL for NRT rows.
  - name: data_source
    type: VARCHAR
    description: FIRMS dataset the row was fetched from (MODIS_SP or MODIS_NRT).
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was fetched.

@bruin"""

import io
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

BASE = "https://firms.modaps.eosdis.nasa.gov"
PROJECT_ID = "bruin-playground-arsalan"
SEASON_START = os.environ.get("SEASON_START", "06-01")
SEASON_END = os.environ.get("SEASON_END", "09-20")
TRANSACTION_SOFT_LIMIT = 4500
MAX_RETRIES = 5

COLUMNS = [
    "latitude", "longitude", "brightness", "scan", "track", "acq_date", "acq_time",
    "satellite", "instrument", "confidence", "version", "bright_t31", "frp",
    "daynight", "fire_type", "data_source",
]


DTYPES = {
    "detection_id": "string", "latitude": "float64", "longitude": "float64", "brightness": "float64",
    "scan": "float64", "track": "float64", "acq_date": pd.ArrowDtype(pa.date32()), "acq_time": "int64",
    "satellite": "string", "instrument": "string", "confidence": "Int64", "version": "string",
    "bright_t31": "float64", "frp": "float64", "daynight": "string", "fire_type": "Int64",
    "data_source": "string",
}


class RateLimited(Exception):
    pass


def empty_frame() -> pd.DataFrame:
    """Typed zero-row frame - an untyped one makes the loader rewrite the table schema as STRING."""
    df = pd.DataFrame({c: pd.Series(dtype=t) for c, t in DTYPES.items()})
    df["extracted_at"] = pd.Series(dtype="datetime64[ns, UTC]")
    return df


def get_with_retry(session: requests.Session, url: str, timeout: int = 180) -> requests.Response:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 429:
                raise RateLimited("HTTP 429")
            if resp.status_code in (500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {resp.status_code}")
            resp.raise_for_status()
            if "Exceeding allowed transaction limit" in resp.text[:200]:
                raise RateLimited(resp.text[:200])
            return resp
        except RateLimited:
            if attempt == MAX_RETRIES:
                raise
            wait = 60 * attempt
            logger.warning("FIRMS rate limit hit, sleeping %ds (attempt %d)", wait, attempt)
            time.sleep(wait)
        except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as exc:
            if attempt == MAX_RETRIES:
                raise
            wait = 2 ** attempt
            logger.warning("Request failed (%s), retry %d in %ds", exc, attempt, wait)
            time.sleep(wait)
    raise RuntimeError("unreachable")


def wait_for_transaction_budget(session: requests.Session, key: str) -> None:
    """Block until the MAP_KEY has headroom in its rolling 10-minute window."""
    while True:
        status = get_with_retry(session, f"{BASE}/mapserver/mapkey_status/?MAP_KEY={key}", timeout=30).json()
        used = int(status.get("current_transactions", 0))
        if used < TRANSACTION_SOFT_LIMIT:
            return
        logger.info("MAP_KEY at %d transactions, waiting 60s for the window to roll", used)
        time.sleep(60)


def season_days(start: date, end: date) -> list[date]:
    days = []
    for year in range(start.year, end.year + 1):
        d = max(start, date.fromisoformat(f"{year}-{SEASON_START}"))
        we = min(end, date.fromisoformat(f"{year}-{SEASON_END}"))
        while d <= we:
            days.append(d)
            d += timedelta(days=1)
    return days


def loaded_days(start: date, end: date) -> set[date]:
    q = f"""
        SELECT DISTINCT acq_date
        FROM `{PROJECT_ID}.raw.firms_modis_fires`
        WHERE acq_date BETWEEN '{start}' AND '{end}'
    """
    try:
        return {r.acq_date for r in bigquery.Client(project=PROJECT_ID).query(q).result()}
    except Exception as exc:  # table may not exist on the first run
        logger.info("No existing rows found (%s)", str(exc)[:80])
        return set()


def sp_max_date(session: requests.Session, key: str) -> date:
    resp = get_with_retry(session, f"{BASE}/api/data_availability/csv/{key}/MODIS_SP", timeout=60)
    avail = pd.read_csv(io.StringIO(resp.text))
    return pd.to_datetime(avail.loc[avail["data_id"] == "MODIS_SP", "max_date"].iloc[0]).date()


def fetch_day(session: requests.Session, key: str, source: str, day: date) -> pd.DataFrame:
    url = f"{BASE}/api/area/csv/{key}/{source}/world/1/{day.isoformat()}"
    resp = get_with_retry(session, url)
    if not resp.text.strip() or resp.text.startswith("Invalid"):
        logger.warning("%s %s returned no CSV: %s", source, day, resp.text[:120])
        return pd.DataFrame(columns=COLUMNS)
    df = pd.read_csv(io.StringIO(resp.text), dtype={"version": str})
    df = df.rename(columns={"type": "fire_type"})
    if "fire_type" not in df.columns:
        df["fire_type"] = pd.NA
    df["data_source"] = source
    return df[COLUMNS]


def materialize():
    key = os.environ["FIRMS_MAP_KEY"]
    start = date.fromisoformat(os.environ.get("BRUIN_START_DATE", "2026-06-01")[:10])
    end = date.fromisoformat(os.environ.get("BRUIN_END_DATE", "2026-09-20")[:10])
    logger.info("Interval: %s to %s", start, end)

    session = requests.Session()
    sp_end = sp_max_date(session, key)
    logger.info("MODIS_SP available through %s; later days use MODIS_NRT", sp_end)

    done = loaded_days(start, end)
    days = [d for d in season_days(start, end) if d not in done]
    logger.info("%d season days in interval, %d already loaded, %d to fetch", len(days) + len(done), len(done), len(days))

    frames = []
    for day in days:
        source = "MODIS_SP" if day <= sp_end else "MODIS_NRT"
        try:
            wait_for_transaction_budget(session, key)
            df = fetch_day(session, key, source, day)
        except RateLimited:
            logger.warning("Rate limit persisted at %s - returning %d days fetched so far", day, len(frames))
            break
        logger.info("%s %s: %d detections", source, day, len(df))
        frames.append(df)
        time.sleep(0.5)

    if not frames:
        logger.info("Nothing new to load")
        return empty_frame()

    out = pd.concat(frames, ignore_index=True)
    out["acq_date"] = pd.to_datetime(out["acq_date"]).dt.date
    out["acq_time"] = out["acq_time"].astype("int64")
    out["confidence"] = pd.to_numeric(out["confidence"], errors="coerce").astype("Int64")
    out["fire_type"] = pd.to_numeric(out["fire_type"], errors="coerce").astype("Int64")
    out["version"] = out["version"].astype(str)
    out["detection_id"] = (
        out["latitude"].map("{:.5f}".format) + "_" + out["longitude"].map("{:.5f}".format) + "_"
        + out["acq_date"].astype(str) + "_" + out["acq_time"].astype(str) + "_" + out["satellite"]
    )
    out = out[["detection_id"] + COLUMNS]
    out["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Fetched %d detections across %d days", len(out), len(frames))
    return out
