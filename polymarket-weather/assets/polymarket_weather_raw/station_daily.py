"""@bruin

name: polymarket_weather_raw.station_daily
description: |
  Daily aggregates for the Polymarket-relevant weather stations across the four
  investigation cities (Paris, London, Seoul, Toronto). The set of cities and
  stations is sourced from `city_manifest.yml` — the same manifest used by
  station_hourly. Use this table as a climatology baseline (multi-year history)
  to compare the Jan-Apr 2026 hourly readings against historical distributions
  per station.

  Source: Meteostat Python library (https://meteostat.net), aggregating NOAA ISD
  and Météo-France SYNOP feeds. Data quality varies by station and metric:
  temperature is nearly complete (~98.8%), precipitation sparse (~35%), and
  sunshine duration extremely sparse (~7.6%).
connection: bruin-playground-arsalan
tags:
  - domain:forensic_investigation
  - data_type:climatology_baseline
  - source:meteostat
  - source:noaa_isd
  - source:meteo_france_synop
  - geography:multi_city
  - temporal_scope:historical
  - update_pattern:snapshot
  - sensitivity:public

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
    description: City identifier from the manifest (Paris, London, Seoul, Toronto). Composite primary key with station_id and date.
    primary_key: true
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Paris
          - London
          - Seoul
          - Toronto
  - name: station_id
    type: VARCHAR
    description: Meteostat station identifier.
    primary_key: true
    checks:
      - name: not_null
  - name: date
    type: DATE
    description: Local calendar date of the observation. Climatology spans 2010-01-01 to near-present by default.
    primary_key: true
    checks:
      - name: not_null
  - name: role
    type: VARCHAR
    description: Whether this station is the Polymarket-resolution primary or a peer.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - primary
          - peer
  - name: station_name
    type: VARCHAR
    description: Human-readable station name as reported by Meteostat.
    checks:
      - name: not_null
  - name: temp_mean_c
    type: DOUBLE
    description: Daily mean 2m air temperature in degrees Celsius.
  - name: temp_min_c
    type: DOUBLE
    description: Daily minimum 2m air temperature in degrees Celsius.
  - name: temp_max_c
    type: DOUBLE
    description: Daily maximum 2m air temperature in degrees Celsius. THE METRIC Polymarket uses to resolve daily-temperature markets.
  - name: precipitation_mm
    type: DOUBLE
    description: Total daily precipitation in millimetres.
  - name: wind_speed_kmh
    type: DOUBLE
    description: Daily mean wind speed in kilometres per hour.
  - name: pressure_hpa
    type: DOUBLE
    description: Daily mean sea-level air pressure in hectopascals.
  - name: sunshine_minutes
    type: DOUBLE
    description: Total daily sunshine duration in minutes.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was fetched from the Meteostat API.
    checks:
      - name: not_null

@bruin"""

import logging
import os
import time
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yaml

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

MANIFEST_PATH = Path(__file__).parent / "city_manifest.yml"
CHUNK_DAYS = 730
MAX_RETRIES = 5


def load_stations() -> list[dict]:
    with open(MANIFEST_PATH, "r") as f:
        manifest = yaml.safe_load(f)
    out = []
    for c in manifest["cities"]:
        for s in c["stations"]:
            out.append({
                "city": c["name"],
                "station_id": s["id"],
                "configured_lat": float(s["lat"]),
                "configured_lon": float(s["lon"]),
                "role": s["role"],
                "name": s.get("name"),
            })
    return out


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.value if hasattr(c, "value") else str(c) for c in df.columns]
    return df


def fetch_chunk(station_ids, start: datetime, end: datetime) -> pd.DataFrame:
    from meteostat import daily

    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            df = daily(station_ids, start, end).fetch()
            if df is None:
                return pd.DataFrame()
            return normalize_columns(df.reset_index())
        except Exception as e:
            wait = 5 * (attempt + 1)
            logger.warning("daily fetch attempt %d/%d failed: %s — retrying in %ds", attempt + 1, MAX_RETRIES, e, wait)
            time.sleep(wait)
            last_err = e
    logger.error("All %d retries exhausted for %s..%s: %s", MAX_RETRIES, start.date(), end.date(), last_err)
    return pd.DataFrame()


def fetch_station_metadata(stations_cfg: list[dict]):
    from meteostat import stations as ms_stations

    out = {}
    for s in stations_cfg:
        sid = s["station_id"]
        meta = ms_stations.meta(sid)
        if meta is None:
            out[sid] = {"name": s.get("name") or sid}
            continue
        out[sid] = {"name": meta.name}
    return out


def materialize():
    warnings.filterwarnings("ignore", category=FutureWarning)

    start_str = os.environ.get("BRUIN_START_DATE", "2010-01-01")
    end_str = os.environ.get("BRUIN_END_DATE", (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y-%m-%d"))
    start = datetime.strptime(start_str[:10], "%Y-%m-%d")
    end = datetime.strptime(end_str[:10], "%Y-%m-%d")

    stations_cfg = load_stations()
    logger.info("Window: %s → %s, stations=%d", start.date(), end.date(), len(stations_cfg))

    metadata = fetch_station_metadata(stations_cfg)

    stations_by_city: dict[str, list[str]] = {}
    for s in stations_cfg:
        stations_by_city.setdefault(s["city"], []).append(s["station_id"])

    pieces = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=CHUNK_DAYS), end)
        for city, sids in stations_by_city.items():
            df = fetch_chunk(sids, cursor, chunk_end)
            if not df.empty:
                pieces.append(df)
                logger.info("[%s] Chunk %s..%s: %d rows", city, cursor.date(), chunk_end.date(), len(df))
            time.sleep(0.5)
        cursor = chunk_end + timedelta(days=1)

    if not pieces:
        logger.warning("No data fetched")
        return pd.DataFrame()

    raw = pd.concat(pieces, ignore_index=True)

    out = pd.DataFrame()
    out["station_id"] = raw["station"].astype(str)
    out["date"] = pd.to_datetime(raw["time"]).dt.date
    out["temp_mean_c"] = pd.to_numeric(raw.get("temp"), errors="coerce")
    out["temp_min_c"] = pd.to_numeric(raw.get("tmin"), errors="coerce")
    out["temp_max_c"] = pd.to_numeric(raw.get("tmax"), errors="coerce")
    out["precipitation_mm"] = pd.to_numeric(raw.get("prcp"), errors="coerce")
    out["wind_speed_kmh"] = pd.to_numeric(raw.get("wspd"), errors="coerce")
    out["pressure_hpa"] = pd.to_numeric(raw.get("pres"), errors="coerce")
    out["sunshine_minutes"] = pd.to_numeric(raw.get("tsun"), errors="coerce")

    station_to_cfg = {s["station_id"]: s for s in stations_cfg}
    out["city"] = None
    out["role"] = None
    out["station_name"] = None
    for sid, meta in metadata.items():
        cfg = station_to_cfg.get(sid)
        if cfg is None:
            continue
        mask = out["station_id"] == sid
        out.loc[mask, "city"] = cfg["city"]
        out.loc[mask, "role"] = cfg["role"]
        out.loc[mask, "station_name"] = meta["name"]

    out["extracted_at"] = datetime.now(timezone.utc)

    out = out[[
        "city", "station_id", "date", "role", "station_name",
        "temp_mean_c", "temp_min_c", "temp_max_c",
        "precipitation_mm", "wind_speed_kmh", "pressure_hpa", "sunshine_minutes",
        "extracted_at",
    ]]
    out = out.dropna(subset=["city", "station_id", "date"]).drop_duplicates(["city", "station_id", "date"], keep="last").reset_index(drop=True)
    logger.info("Total rows: %d", len(out))
    return out
