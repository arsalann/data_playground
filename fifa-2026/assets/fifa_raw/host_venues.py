"""@bruin

name: fifa_raw.host_venues
description: |
  16 host venues for the 2026 FIFA World Cup (USA / Canada / Mexico) with full
  metadata: city, country, IANA timezone, lat/lon, elevation, capacity, roof type,
  and the primary METAR ICAO + two peer ICAOs used for Meteostat sanity-checks
  alongside the Open-Meteo gridded reanalysis.

  Source: `tournament_manifest.yml` in this folder, seeded from FIFA's host city
  announcement and Wikipedia per-stadium pages. The manifest is the single source
  of truth for tournament structure (per `AGENTS.md`); Wikipedia layout is too
  fragile to scrape on a recurring basis.

  License: data points are factual and public.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - reference
  - raw_data
  - manifest_backed

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
    description: Stable internal identifier (e.g., V_AZTECA).
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: city
    type: VARCHAR
    description: Host city name.
    checks:
      - name: not_null
  - name: country
    type: VARCHAR
    description: ISO-3 country code (USA, CAN, MEX).
    checks:
      - name: not_null
      - name: accepted_values
        value: [USA, CAN, MEX]
  - name: stadium
    type: VARCHAR
    description: Official stadium name.
    checks:
      - name: not_null
  - name: timezone
    type: VARCHAR
    description: IANA timezone of the venue (e.g., America/New_York).
    checks:
      - name: not_null
  - name: latitude
    type: DOUBLE
    description: Latitude in decimal degrees.
    checks:
      - name: not_null
  - name: longitude
    type: DOUBLE
    description: Longitude in decimal degrees.
    checks:
      - name: not_null
  - name: elevation_m
    type: DOUBLE
    description: Approximate venue elevation in metres (Mexico City Azteca = 2240, Guadalajara Akron = 1566).
    checks:
      - name: not_null
  - name: capacity
    type: INT64
    description: Stated FIFA-2026 seating capacity.
    checks:
      - name: not_null
  - name: roof_type
    type: VARCHAR
    description: Roof configuration (open / fixed_canopy / retractable).
  - name: primary_icao
    type: VARCHAR
    description: ICAO of the primary METAR-fed weather station serving this venue.
    checks:
      - name: not_null
  - name: peer_icaos_csv
    type: VARCHAR
    description: Comma-separated ICAOs of two peer stations used for Meteostat cross-checking.
  - name: openmeteo_grid_lat
    type: DOUBLE
    description: Latitude of the Open-Meteo grid point queried for this venue.
    checks:
      - name: not_null
  - name: openmeteo_grid_lon
    type: DOUBLE
    description: Longitude of the Open-Meteo grid point queried for this venue.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was loaded from the manifest.
    checks:
      - name: not_null

@bruin"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

MANIFEST_PATH = Path(__file__).parent / "tournament_manifest.yml"


def materialize():
    with open(MANIFEST_PATH, "r") as f:
        manifest = yaml.safe_load(f)

    rows = []
    snap_ts = datetime.now(timezone.utc)
    for v in manifest["venues"]:
        rows.append({
            "venue_id": v["venue_id"],
            "city": v["city"],
            "country": v["country"],
            "stadium": v["stadium"],
            "timezone": v["timezone"],
            "latitude": float(v["lat"]),
            "longitude": float(v["lon"]),
            "elevation_m": float(v["elevation_m"]),
            "capacity": int(v["capacity"]),
            "roof_type": v.get("roof_type"),
            "primary_icao": v["primary_icao"],
            "peer_icaos_csv": ",".join(v.get("peer_icaos") or []),
            "openmeteo_grid_lat": float(v["openmeteo_grid"]["lat"]),
            "openmeteo_grid_lon": float(v["openmeteo_grid"]["lon"]),
            "extracted_at": snap_ts,
        })
    df = pd.DataFrame(rows)
    logger.info("host_venues rows: %d", len(df))
    return df
