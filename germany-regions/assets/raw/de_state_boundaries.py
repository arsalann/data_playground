"""@bruin
name: raw.de_state_boundaries
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Boundaries of the 16 German states as NUTS 1 regions (NUTS 2024 classification),
  1:10 million scale, WGS84 (EPSG:4326), from Eurostat GISCO:
  https://gisco-services.ec.europa.eu/distribution/v2/nuts/geojson/NUTS_RG_10M_2024_4326_LEVL_1.geojson

  Geometries are normalised to GeoJSON MultiPolygon so staging can unnest parts and
  rings uniformly. Small reference table, fully replaced on each run.
  License: (c) EuroGeographics for the administrative boundaries; Eurostat GISCO
  terms of use (https://ec.europa.eu/eurostat/web/gisco/geodata/statistical-units).

materialization:
  type: table
  strategy: create+replace

columns:
  - name: nuts1_id
    type: VARCHAR
    description: NUTS 1 code (DE1 Baden-Wuerttemberg ... DEG Thueringen).
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: nuts_name
    type: VARCHAR
    description: Region name in the national language as published by GISCO.
  - name: geometry_geojson
    type: VARCHAR
    description: GeoJSON MultiPolygon geometry (lon/lat, WGS84) as a JSON string.
  - name: n_parts
    type: INTEGER
    description: Number of polygons in the MultiPolygon (islands and exclaves count separately).
  - name: n_vertices
    type: INTEGER
    description: Total number of vertices across all exterior and interior rings.
  - name: source_url
    type: VARCHAR
    description: GISCO download URL.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp of ingestion.

@bruin"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

GISCO_URL = "https://gisco-services.ec.europa.eu/distribution/v2/nuts/geojson/NUTS_RG_10M_2024_4326_LEVL_1.geojson"
MAX_RETRIES = 5


def fetch() -> dict:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(GISCO_URL, timeout=180)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            wait = 2 ** attempt
            logger.warning("GISCO download failed (%s), attempt %d; retry in %ds", e, attempt, wait)
            time.sleep(wait)
    raise RuntimeError("Could not download GISCO NUTS boundaries")


def materialize():
    features = [f for f in fetch()["features"] if f["properties"]["CNTR_CODE"] == "DE"]
    rows = []
    for f in features:
        geom = f["geometry"]
        polygons = geom["coordinates"] if geom["type"] == "MultiPolygon" else [geom["coordinates"]]
        rows.append({
            "nuts1_id": f["properties"]["NUTS_ID"],
            "nuts_name": f["properties"]["NUTS_NAME"],
            "geometry_geojson": json.dumps({"type": "MultiPolygon", "coordinates": polygons}),
            "n_parts": len(polygons),
            "n_vertices": sum(len(ring) for poly in polygons for ring in poly),
            "source_url": GISCO_URL,
        })
    if len(rows) != 16:
        raise RuntimeError(f"Expected 16 German NUTS 1 regions, got {len(rows)}")
    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Boundaries: %d states, %d parts, %d vertices", len(df), df["n_parts"].sum(), df["n_vertices"].sum())
    return df
