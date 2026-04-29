"""@bruin

name: polymarket_weather_raw.station_daily
description: |
  Daily aggregates for the same six Paris-region stations covered by station_hourly.
  Used as the climatology baseline (multi-year history) — the hourly table only goes
  back to 2024 by default, while this table covers 2010 onwards so April 2026 readings
  can be compared to historical April distributions.

  This dataset is critical for the Paris temperature sensor tampering investigation,
  providing 16+ years of historical context to assess whether the April 2026 temperature
  spikes at Charles de Gaulle airport fall outside normal climatological patterns.
  Each station represents a different microclimate: airports (CDG, Orly, Le Bourget),
  urban core (Montsouris), military (Villacoublay), and semi-rural (Trappes).

  Source: Meteostat Python library (https://meteostat.net), aggregating NOAA ISD and
  Météo-France SYNOP feeds. Station list and verification rules identical to station_hourly.
  Data quality varies by station and metric: temperature data is nearly complete (~98.8%),
  while precipitation is sparse (~35% coverage) and sunshine duration extremely sparse (~7.6% coverage).
connection: bruin-playground-arsalan
tags:
  - domain:forensic_investigation
  - data_type:climatology_baseline
  - source:meteostat
  - source:noaa_isd
  - source:meteo_france_synop
  - geography:paris_region
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
  - name: station_id
    type: VARCHAR
    description: Meteostat station identifier (5-digit WMO synoptic code). Maps to specific Paris-region locations - 07157 is the suspect Charles de Gaulle sensor.
    primary_key: true
    checks:
      - name: not_null
  - name: date
    type: DATE
    description: Local calendar date of the observation. Spans 2010-01-01 to near-present for climatological analysis.
    primary_key: true
    checks:
      - name: not_null
  - name: station_name
    type: VARCHAR
    description: Human-readable station name as reported by Meteostat. Used for dashboard display but may differ from official airport/site names.
    checks:
      - name: not_null
  - name: temp_mean_c
    type: DOUBLE
    description: Daily mean 2m air temperature in degrees Celsius. Derived from hourly observations, nearly complete (~98.8% coverage).
  - name: temp_min_c
    type: DOUBLE
    description: Daily minimum 2m air temperature in degrees Celsius. Critical for detecting overnight cooling patterns and sensor consistency.
  - name: temp_max_c
    type: DOUBLE
    description: Daily maximum 2m air temperature in degrees Celsius. THE METRIC Polymarket uses to resolve daily-temperature markets. Subject of the April 2026 tampering allegations.
  - name: precipitation_mm
    type: DOUBLE
    description: Total daily precipitation in millimetres. Sparse coverage (~35% of records) due to station equipment differences and reporting practices.
  - name: wind_speed_kmh
    type: DOUBLE
    description: Daily mean wind speed in kilometres per hour. Nearly complete coverage (~98.7%). Airport stations typically higher due to exposure.
  - name: pressure_hpa
    type: DOUBLE
    description: Daily mean sea-level air pressure in hectopascals. Nearly complete coverage (~98.2%). Standardized to sea level across all stations.
  - name: sunshine_minutes
    type: DOUBLE
    description: Total daily sunshine duration in minutes. Extremely sparse coverage (~7.6% of records) - not all stations equipped with sunshine sensors.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was fetched from the Meteostat API. Provides data provenance and refresh tracking for the investigation.
    checks:
      - name: not_null

@bruin"""

import logging
import os
import time
import warnings
from datetime import datetime, timedelta, timezone

import pandas as pd

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

STATIONS = [
    {"station_id": "07157", "configured_lat": 49.010, "configured_lon": 2.548, "role": "Paris-Charles de Gaulle"},
    {"station_id": "07150", "configured_lat": 48.969, "configured_lon": 2.441, "role": "Paris / Le Bourget"},
    {"station_id": "07156", "configured_lat": 48.822, "configured_lon": 2.338, "role": "Paris-Montsouris"},
    {"station_id": "07149", "configured_lat": 48.723, "configured_lon": 2.379, "role": "Paris-Orly"},
    {"station_id": "07147", "configured_lat": 48.774, "configured_lon": 2.197, "role": "Villacoublay"},
    {"station_id": "07145", "configured_lat": 48.774, "configured_lon": 2.009, "role": "Trappes"},
]

CHUNK_DAYS = 730  # daily series can use longer chunks
MAX_RETRIES = 5


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


def fetch_station_metadata():
    from meteostat import stations

    out = {}
    for s in STATIONS:
        meta = stations.meta(s["station_id"])
        if meta is None:
            out[s["station_id"]] = {"name": s["role"]}
            continue
        out[s["station_id"]] = {"name": meta.name}
    return out


def materialize():
    warnings.filterwarnings("ignore", category=FutureWarning)

    start_str = os.environ.get("BRUIN_START_DATE", "2010-01-01")
    end_str = os.environ.get("BRUIN_END_DATE", (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y-%m-%d"))
    start = datetime.strptime(start_str[:10], "%Y-%m-%d")
    end = datetime.strptime(end_str[:10], "%Y-%m-%d")

    logger.info("Window: %s → %s, stations=%d", start.date(), end.date(), len(STATIONS))

    metadata = fetch_station_metadata()
    station_ids = [s["station_id"] for s in STATIONS]

    pieces = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=CHUNK_DAYS), end)
        df = fetch_chunk(station_ids, cursor, chunk_end)
        if not df.empty:
            pieces.append(df)
            logger.info("Chunk %s..%s: %d rows", cursor.date(), chunk_end.date(), len(df))
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.5)

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

    out["station_name"] = None
    for sid, meta in metadata.items():
        out.loc[out["station_id"] == sid, "station_name"] = meta["name"]

    out["extracted_at"] = datetime.now(timezone.utc)

    out = out[[
        "station_id", "date", "station_name",
        "temp_mean_c", "temp_min_c", "temp_max_c",
        "precipitation_mm", "wind_speed_kmh", "pressure_hpa", "sunshine_minutes",
        "extracted_at",
    ]]
    out = out.dropna(subset=["station_id", "date"]).drop_duplicates(["station_id", "date"], keep="last").reset_index(drop=True)
    logger.info("Total rows: %d", len(out))
    return out
