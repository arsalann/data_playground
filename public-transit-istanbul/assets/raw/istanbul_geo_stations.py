"""@bruin
name: raw.istanbul_geo_stations
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Rail system station points and rail line geometries from IBB GeoJSON layers.
  Source: IBB Open Data Portal
  - Station points: https://data.ibb.gov.tr/en/dataset/rayli-sistem-istasyon-noktalari-verisi
  - Rail lines: https://data.ibb.gov.tr/en/dataset/rayli-ulasim-hatlari-vektor-verisi
  License: Istanbul Metropolitan Municipality Open Data License.
  Coordinate system: WGS84 (EPSG:4326).

materialization:
  type: table
  strategy: create+replace

columns:
  - name: station_name
    type: VARCHAR
    description: Name of the rail station
    primary_key: true
  - name: line_name
    type: VARCHAR
    description: Rail line project name (e.g. M1 Yenikapi-Ataturk Havalimani)
    primary_key: true
  - name: project_phase
    type: VARCHAR
    description: Project phase (Mevcut - existing, Yapim Asamasinda - under construction, etc.)
  - name: line_type
    type: VARCHAR
    description: Type of rail system (Metro, Tramvay, Teleferik, Funikuler, etc.)
  - name: directorate
    type: VARCHAR
    description: Managing directorate (European or Asian side rail systems)
  - name: longitude
    type: DOUBLE
    description: Station longitude in WGS84 (EPSG:4326)
  - name: latitude
    type: DOUBLE
    description: Station latitude in WGS84 (EPSG:4326)
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was extracted

@bruin"""

import json
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

STATION_POINTS_URL = "https://data.ibb.gov.tr/dataset/04ec9805-2483-46c7-914f-30c50857a846/resource/3dc8203f-3613-48a8-85e9-24fffb7821ad/download/rayli_sistem_istasyon_poi_verisi.geojson"


def fetch_station_points() -> pd.DataFrame:
    logger.info("Downloading station points GeoJSON ...")
    resp = requests.get(STATION_POINTS_URL, timeout=60)
    resp.raise_for_status()
    geojson = resp.json()

    rows = []
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        coords = feature.get("geometry", {}).get("coordinates", [None, None])
        rows.append({
            "station_name": props.get("ISTASYON"),
            "line_name": props.get("PROJE_ADI"),
            "project_phase": props.get("PROJE_ASAMA"),
            "line_type": props.get("HAT_TURU"),
            "directorate": props.get("MUDURLUK"),
            "longitude": coords[0] if len(coords) >= 2 else None,
            "latitude": coords[1] if len(coords) >= 2 else None,
        })

    df = pd.DataFrame(rows)
    logger.info("Parsed %d station points", len(df))
    return df


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2020-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2025-12-31")
    logger.info("Interval: %s to %s", start_date, end_date)

    df = fetch_station_points()
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Total geo station rows: %d", len(df))
    return df
