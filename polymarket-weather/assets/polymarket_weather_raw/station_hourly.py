"""@bruin

name: polymarket_weather_raw.station_hourly
description: |
  Hourly surface observations for the six Paris-region weather stations relevant to the
  alleged April 2026 Polymarket sensor-tampering investigation. The station list is fixed
  and identical for every fetch; the same Meteostat call is used for each, so cross-station
  comparison is methodologically clean.

  Source: Meteostat Python library (https://meteostat.net), which aggregates NOAA Integrated
  Surface Database (ISD) METAR/SYNOP and Météo-France SYNOP feeds. Re-distributed under the
  Meteostat Terms of Service for non-commercial research.

  Stations (Meteostat ID, ICAO, role):
    - 07157  LFPG  Paris-Charles de Gaulle      (suspect sensor)
    - 07150  LFPB  Paris / Le Bourget           (post-incident replacement)
    - 07156  --    Paris-Montsouris             (urban historical reference)
    - 07149  LFPO  Paris-Orly                   (second airport)
    - 07147  LFPV  Villacoublay                 (military)
    - 07145  LFPT  Trappes                      (semi-rural radiosonde)

  Returned timestamps are UTC. Local Paris timestamps are derived in staging.
  Verification: each station's reported lat/lon is asserted within 0.05° of the configured
  value at ingestion time and logged.

  Operational characteristics: Daily refresh, ~20k rows/month, append-only pattern.
  Reliability: 30-day chunking with 5-retry backoff, coordinate validation at 0.05° tolerance,
  duplicate removal on (station_id, ts_utc). Quality monitoring includes gap detection and
  cross-station anomaly detection in downstream staging layers.
connection: bruin-playground-arsalan
tags:
  - sensor-tampering-investigation
  - paris-weather
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
  - name: station_id
    type: VARCHAR
    description: Meteostat station identifier (5-digit WMO synoptic code for these stations). Always exactly 5 characters for these Paris stations.
    primary_key: true
    checks:
      - name: not_null
  - name: ts_utc
    type: TIMESTAMP
    description: Observation timestamp in UTC (top of the hour). Forms composite primary key with station_id for hourly uniqueness.
    primary_key: true
    checks:
      - name: not_null
  - name: station_name
    type: VARCHAR
    description: Human-readable station name as reported by Meteostat. Maps to fixed set of 6 Paris-region stations.
    checks:
      - name: not_null
  - name: icao
    type: VARCHAR
    description: ICAO airport identifier where applicable (4 characters), otherwise null for non-airport stations like Paris-Montsouris.
  - name: latitude
    type: DOUBLE
    description: Station latitude in decimal degrees (positive=north). Verified within 0.05° of configured value at ingestion. Paris region range ~48.7-49.0°N.
    checks:
      - name: not_null
  - name: longitude
    type: DOUBLE
    description: Station longitude in decimal degrees (positive=east). Verified within 0.05° of configured value at ingestion. Paris region range ~2.0-2.5°E.
    checks:
      - name: not_null
  - name: elevation_m
    type: DOUBLE
    description: Station elevation above mean sea level in metres. Paris region stations range roughly 70-170m elevation.
    checks:
      - name: not_null
  - name: temp_c
    type: DOUBLE
    description: Air temperature at 2 metres in degrees Celsius. Primary metric for the CDG sensor tampering investigation. Typical Paris range -10 to 40°C.
  - name: humidity_pct
    type: DOUBLE
    description: Relative humidity as percentage (0-100). Derived from dew point and air temperature measurements.
  - name: precipitation_mm
    type: DOUBLE
    description: Hourly precipitation total in millimetres. Frequently null during dry periods, representing zero or trace amounts.
  - name: wind_speed_kmh
    type: DOUBLE
    description: Mean wind speed in kilometres per hour over the hourly observation period.
  - name: wind_direction_deg
    type: DOUBLE
    description: Mean wind direction in degrees (0=North, 90=East, 180=South, 270=West). Occasionally null during calm conditions.
  - name: pressure_hpa
    type: DOUBLE
    description: Sea-level air pressure in hectopascals (millibars). Standard atmospheric pressure ~1013 hPa. Occasional nulls due to sensor issues.
  - name: cloud_cover_okta
    type: DOUBLE
    description: Cloud cover in oktas (0=clear sky, 8=overcast). Traditional meteorological scale with occasional nulls during observation gaps.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was fetched from the Meteostat API. Used for data lineage and refresh tracking.
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

# Six Paris-region stations. Fixed list — same call for every station so methodology is identical.
STATIONS = [
    {"station_id": "07157", "icao": "LFPG", "configured_lat": 49.010, "configured_lon": 2.548, "role": "Paris-Charles de Gaulle"},
    {"station_id": "07150", "icao": "LFPB", "configured_lat": 48.969, "configured_lon": 2.441, "role": "Paris / Le Bourget"},
    {"station_id": "07156", "icao": None,   "configured_lat": 48.822, "configured_lon": 2.338, "role": "Paris-Montsouris"},
    {"station_id": "07149", "icao": "LFPO", "configured_lat": 48.723, "configured_lon": 2.379, "role": "Paris-Orly"},
    {"station_id": "07147", "icao": "LFPV", "configured_lat": 48.774, "configured_lon": 2.197, "role": "Villacoublay"},
    {"station_id": "07145", "icao": "LFPT", "configured_lat": 48.774, "configured_lon": 2.009, "role": "Trappes"},
]

CHUNK_DAYS = 30
COORD_TOLERANCE_DEG = 0.05
MAX_RETRIES = 5


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


def fetch_station_metadata():
    """Look up each configured station and assert its lat/lon matches within tolerance."""
    from meteostat import stations

    out = {}
    for s in STATIONS:
        meta = stations.meta(s["station_id"])
        if meta is None:
            logger.error("Meteostat returned no metadata for station %s — leaving placeholder values", s["station_id"])
            out[s["station_id"]] = {
                "name": s["role"],
                "latitude": s["configured_lat"],
                "longitude": s["configured_lon"],
                "elevation": None,
                "icao": s["icao"],
            }
            continue

        actual_lat = float(meta.latitude)
        actual_lon = float(meta.longitude)
        d_lat = abs(actual_lat - s["configured_lat"])
        d_lon = abs(actual_lon - s["configured_lon"])

        logger.info(
            "Station %s (%s): name=%r lat=%.4f lon=%.4f elev=%s — Δlat=%.3f Δlon=%.3f",
            s["station_id"], s["role"], meta.name, actual_lat, actual_lon, meta.elevation, d_lat, d_lon,
        )

        if d_lat > COORD_TOLERANCE_DEG or d_lon > COORD_TOLERANCE_DEG:
            raise RuntimeError(
                f"Coordinate mismatch for station {s['station_id']}: configured ({s['configured_lat']}, {s['configured_lon']}), "
                f"actual ({actual_lat}, {actual_lon}). Δlat={d_lat:.3f} Δlon={d_lon:.3f} exceeds tolerance {COORD_TOLERANCE_DEG}°."
            )

        out[s["station_id"]] = {
            "name": meta.name,
            "latitude": actual_lat,
            "longitude": actual_lon,
            "elevation": meta.elevation,
            "icao": s["icao"],
        }
    return out


def materialize():
    warnings.filterwarnings("ignore", category=FutureWarning)

    start_str = os.environ.get("BRUIN_START_DATE", "2024-01-01")
    end_str = os.environ.get("BRUIN_END_DATE", (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y-%m-%d"))
    start = datetime.strptime(start_str[:10], "%Y-%m-%d")
    end = datetime.strptime(end_str[:10], "%Y-%m-%d")

    logger.info("Window: %s → %s, stations=%d, chunk=%dd", start.date(), end.date(), len(STATIONS), CHUNK_DAYS)

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
        else:
            logger.warning("Chunk %s..%s: no rows", cursor.date(), chunk_end.date())
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.5)

    if not pieces:
        logger.warning("No data fetched")
        return pd.DataFrame()

    raw = pd.concat(pieces, ignore_index=True)

    # Map raw fields to typed output schema. Some columns may be missing for some stations.
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

    # Attach metadata (name/lat/lon/elev/icao) per station
    for col in ("station_name", "icao", "latitude", "longitude", "elevation_m"):
        out[col] = None
    for sid, meta in metadata.items():
        mask = out["station_id"] == sid
        out.loc[mask, "station_name"] = meta["name"]
        out.loc[mask, "icao"] = meta["icao"]
        out.loc[mask, "latitude"] = meta["latitude"]
        out.loc[mask, "longitude"] = meta["longitude"]
        out.loc[mask, "elevation_m"] = meta["elevation"]

    out["extracted_at"] = datetime.now(timezone.utc)

    out = out[[
        "station_id", "ts_utc", "station_name", "icao",
        "latitude", "longitude", "elevation_m",
        "temp_c", "humidity_pct", "precipitation_mm",
        "wind_speed_kmh", "wind_direction_deg", "pressure_hpa", "cloud_cover_okta",
        "extracted_at",
    ]]

    out = out.dropna(subset=["station_id", "ts_utc"])
    out = out.drop_duplicates(subset=["station_id", "ts_utc"], keep="last").reset_index(drop=True)

    logger.info(
        "Total rows: %d. Per-station counts: %s",
        len(out), out.groupby("station_id").size().to_dict(),
    )
    logger.info("Date range: %s..%s", out["ts_utc"].min(), out["ts_utc"].max())
    return out
