"""@bruin

name: polymarket_weather_raw.station_hourly
description: |
  Hourly surface observations for the Polymarket-relevant weather stations across
  the four investigation cities — Paris, London, Seoul, and Toronto. The set of
  cities and stations is sourced from `city_manifest.yml`; each city carries a
  primary station (the airport whose ICAO Polymarket cites for resolution) plus
  3-4 nearby peer stations used for cross-station anomaly detection.

  Source: Meteostat Python library (https://meteostat.net), aggregating NOAA ISD
  METAR/SYNOP and Météo-France SYNOP feeds. Re-distributed under the Meteostat
  Terms of Service for non-commercial research.

  Returned timestamps are UTC. Local-time conversion (per city's IANA timezone)
  happens once in staging. Each station's reported lat/lon is asserted within
  0.05° of the configured value at ingestion and logged.

  Operational characteristics: refresh on demand, ~3k rows per station-month,
  create+replace materialization. Window controlled by BRUIN_START_DATE /
  BRUIN_END_DATE; default covers the four-month investigation window
  2026-01-01 - 2026-04-30.
connection: bruin-playground-arsalan
tags:
  - sensor-tampering-investigation
  - multi-city-weather
  - polymarket-resolution
  - meteorological-data
  - meteostat-source
  - hourly-observations
  - fact-table
  - external-source
  - raw-data

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: city
    type: VARCHAR
    description: City identifier from the manifest (Paris, London, Seoul, Toronto). Composite primary key with station_id and ts_utc.
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
    description: Meteostat station identifier (5-digit WMO synoptic code or alphanumeric Meteostat private ID).
    primary_key: true
    checks:
      - name: not_null
  - name: ts_utc
    type: TIMESTAMP
    description: Observation timestamp in UTC (top of the hour).
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
  - name: icao
    type: VARCHAR
    description: ICAO airport identifier where applicable, NULL for non-airport stations.
  - name: latitude
    type: DOUBLE
    description: Station latitude in decimal degrees (positive=north). Verified within 0.05° of configured value at ingestion.
    checks:
      - name: not_null
  - name: longitude
    type: DOUBLE
    description: Station longitude in decimal degrees (positive=east). Verified within 0.05° of configured value at ingestion.
    checks:
      - name: not_null
  - name: elevation_m
    type: DOUBLE
    description: Station elevation above mean sea level in metres.
  - name: temp_c
    type: DOUBLE
    description: Air temperature at 2 metres in degrees Celsius.
  - name: humidity_pct
    type: DOUBLE
    description: Relative humidity as percentage (0-100).
  - name: precipitation_mm
    type: DOUBLE
    description: Hourly precipitation total in millimetres.
  - name: wind_speed_kmh
    type: DOUBLE
    description: Mean wind speed in kilometres per hour.
  - name: wind_direction_deg
    type: DOUBLE
    description: Mean wind direction in degrees (0=North, 90=East, 180=South, 270=West).
  - name: pressure_hpa
    type: DOUBLE
    description: Sea-level air pressure in hectopascals.
  - name: cloud_cover_okta
    type: DOUBLE
    description: Cloud cover in oktas (0=clear sky, 8=overcast).
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
CHUNK_DAYS = 30
COORD_TOLERANCE_DEG = 0.05
MAX_RETRIES = 5


def load_stations() -> list[dict]:
    """Flatten city_manifest.yml into one dict per (city, station)."""
    with open(MANIFEST_PATH, "r") as f:
        manifest = yaml.safe_load(f)
    out = []
    for c in manifest["cities"]:
        for s in c["stations"]:
            out.append({
                "city": c["name"],
                "station_id": s["id"],
                "icao": s.get("icao"),
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
    """Fetch one (≤30-day) chunk of hourly data for a list of station IDs with retry."""
    from meteostat import hourly

    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            df = hourly(station_ids, start, end).fetch()
            if df is None:
                return pd.DataFrame()
            return normalize_columns(df.reset_index())
        except Exception as e:  # network / parse / SSL
            wait = 5 * (attempt + 1)
            logger.warning("hourly fetch attempt %d/%d failed: %s — retrying in %ds", attempt + 1, MAX_RETRIES, e, wait)
            time.sleep(wait)
            last_err = e
    logger.error("All %d retries exhausted for %s..%s: %s", MAX_RETRIES, start.date(), end.date(), last_err)
    return pd.DataFrame()


def fetch_station_metadata(stations_cfg: list[dict]):
    """Look up each configured station and assert lat/lon within tolerance."""
    from meteostat import stations as ms_stations

    out = {}
    for s in stations_cfg:
        sid = s["station_id"]
        meta = ms_stations.meta(sid)
        if meta is None:
            logger.error("Meteostat returned no metadata for station %s — using configured values", sid)
            out[sid] = {
                "name": s.get("name") or sid,
                "latitude": s["configured_lat"],
                "longitude": s["configured_lon"],
                "elevation": None,
                "icao": s.get("icao"),
            }
            continue

        actual_lat = float(meta.latitude)
        actual_lon = float(meta.longitude)
        d_lat = abs(actual_lat - s["configured_lat"])
        d_lon = abs(actual_lon - s["configured_lon"])

        logger.info(
            "Station %s [%s] (%s): name=%r lat=%.4f lon=%.4f elev=%s — Δlat=%.3f Δlon=%.3f",
            sid, s["city"], s["role"], meta.name, actual_lat, actual_lon, meta.elevation, d_lat, d_lon,
        )

        if d_lat > COORD_TOLERANCE_DEG or d_lon > COORD_TOLERANCE_DEG:
            raise RuntimeError(
                f"Coordinate mismatch for station {sid}: configured ({s['configured_lat']}, {s['configured_lon']}), "
                f"actual ({actual_lat}, {actual_lon}). Δlat={d_lat:.3f} Δlon={d_lon:.3f} exceeds tolerance {COORD_TOLERANCE_DEG}°."
            )

        out[sid] = {
            "name": meta.name,
            "latitude": actual_lat,
            "longitude": actual_lon,
            "elevation": meta.elevation,
            "icao": (meta.identifiers or {}).get("icao") or s.get("icao"),
        }
    return out


def materialize():
    warnings.filterwarnings("ignore", category=FutureWarning)

    start_str = os.environ.get("BRUIN_START_DATE", "2026-01-01")
    end_str = os.environ.get("BRUIN_END_DATE", "2026-04-30")
    start = datetime.strptime(start_str[:10], "%Y-%m-%d")
    end = datetime.strptime(end_str[:10], "%Y-%m-%d")

    stations_cfg = load_stations()
    logger.info("Window: %s → %s, stations=%d, chunk=%dd", start.date(), end.date(), len(stations_cfg), CHUNK_DAYS)

    metadata = fetch_station_metadata(stations_cfg)

    # Group stations by city to keep each Meteostat call ≤ 10 stations
    # (Meteostat 2.x blocks larger multi-station requests by default).
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
            else:
                logger.warning("[%s] Chunk %s..%s: no rows", city, cursor.date(), chunk_end.date())
            time.sleep(0.5)
        cursor = chunk_end + timedelta(days=1)

    if not pieces:
        logger.warning("No data fetched")
        return pd.DataFrame()

    raw = pd.concat(pieces, ignore_index=True)

    out = pd.DataFrame()
    out["station_id"] = raw["station"].astype(str)
    out["ts_utc"] = pd.to_datetime(raw["time"], utc=True)
    out["temp_c"] = pd.to_numeric(raw.get("temp"), errors="coerce")
    out["humidity_pct"] = pd.to_numeric(raw.get("rhum"), errors="coerce")
    out["precipitation_mm"] = pd.to_numeric(raw.get("prcp"), errors="coerce")
    out["wind_speed_kmh"] = pd.to_numeric(raw.get("wspd"), errors="coerce")
    out["wind_direction_deg"] = pd.to_numeric(raw.get("wdir"), errors="coerce")
    out["pressure_hpa"] = pd.to_numeric(raw.get("pres"), errors="coerce")
    out["cloud_cover_okta"] = pd.to_numeric(raw.get("cldc"), errors="coerce")

    # Attach per-station metadata. A station can only belong to one (city, role) pair.
    station_to_cfg = {s["station_id"]: s for s in stations_cfg}
    for col in ("city", "role", "station_name", "icao", "latitude", "longitude", "elevation_m"):
        out[col] = None
    for sid, meta in metadata.items():
        cfg = station_to_cfg.get(sid)
        if cfg is None:
            continue
        mask = out["station_id"] == sid
        out.loc[mask, "city"] = cfg["city"]
        out.loc[mask, "role"] = cfg["role"]
        out.loc[mask, "station_name"] = meta["name"]
        out.loc[mask, "icao"] = meta["icao"]
        out.loc[mask, "latitude"] = meta["latitude"]
        out.loc[mask, "longitude"] = meta["longitude"]
        out.loc[mask, "elevation_m"] = meta["elevation"]

    out["extracted_at"] = datetime.now(timezone.utc)

    out = out[[
        "city", "station_id", "ts_utc", "role", "station_name", "icao",
        "latitude", "longitude", "elevation_m",
        "temp_c", "humidity_pct", "precipitation_mm",
        "wind_speed_kmh", "wind_direction_deg", "pressure_hpa", "cloud_cover_okta",
        "extracted_at",
    ]]

    out = out.dropna(subset=["city", "station_id", "ts_utc"])
    out = out.drop_duplicates(subset=["city", "station_id", "ts_utc"], keep="last").reset_index(drop=True)

    logger.info(
        "Total rows: %d. Per-(city,station) counts: %s",
        len(out), out.groupby(["city", "station_id"]).size().to_dict(),
    )
    logger.info("Date range: %s..%s", out["ts_utc"].min(), out["ts_utc"].max())
    return out
