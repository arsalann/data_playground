"""@bruin

name: eu_mortality_raw.nuts3_reference
description: |
  NUTS3 reference table for the EU-27, drawn from the Eurostat GISCO 2024 release.

  Source: NUTS_LB_2024_4326_LEVL_3.geojson -- one Point feature per NUTS3 region
  in EPSG:4326, providing the label point (representative inside-polygon coordinate)
  plus classification attributes:
    - LEVL_CODE = 3, CNTR_CODE = ISO 3166-1 alpha-2
    - NAME_LATN, NUTS_NAME (native script)
    - URBN_TYPE (1 cities, 2 towns/suburbs, 3 rural -- DEGURBA classification)
    - MOUNT_TYPE (mountain area classification)
    - COAST_TYPE (coastal classification)

  GISCO label points are representative inside-polygon coordinates: they sit inside
  the actual region polygon (unlike a naive area centroid, which can fall outside
  concave shapes such as Italian provinces with deep indentations). These are the
  sampling points used downstream for Open-Meteo temperature reanalysis.

  EU-27 scope (27 member states; UK explicitly excluded since 2020).
  NUTS classification 2024, in force 2021-2027.

  Source URL: https://gisco-services.ec.europa.eu/distribution/v2/nuts/geojson/NUTS_LB_2024_4326_LEVL_3.geojson
  Licence: © European Union, attribution-only, no auth.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace
image: python:3.11

tags:
  - eu-27
  - mortality
  - raw
  - gisco
  - nuts3
  - reference

columns:
  - name: nuts_id
    type: VARCHAR
    description: NUTS3 code (e.g., FR101, DE111, IT102). Primary key.
    primary_key: true
  - name: level_code
    type: INTEGER
    description: NUTS level. Always 3 here.
  - name: nuts1_id
    type: VARCHAR
    description: Parent NUTS1 code (first 3 characters of nuts_id).
  - name: nuts2_id
    type: VARCHAR
    description: Parent NUTS2 code (first 4 characters of nuts_id).
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2 country code (CNTR_CODE in GISCO).
  - name: name_latn
    type: VARCHAR
    description: Latin-script regional name.
  - name: name_native
    type: VARCHAR
    description: Native-script regional name.
  - name: degurba
    type: INTEGER
    description: DEGURBA urbanisation class. 1=cities (densely populated), 2=towns and suburbs (intermediate), 3=rural areas (thinly populated).
  - name: mount_type
    type: INTEGER
    description: GISCO mountain-area classification. 1=>50% population in mountains, 2=>50% area in mountains, 3=both, 4=neither. Null where unspecified.
  - name: coast_type
    type: INTEGER
    description: GISCO coastal classification. 1=coastline, 2=>50% population <50km from coast, 3=neither. Null where unspecified.
  - name: centroid_lat
    type: DOUBLE
    description: GISCO label-point latitude in decimal degrees (EPSG:4326).
  - name: centroid_lon
    type: DOUBLE
    description: GISCO label-point longitude in decimal degrees (EPSG:4326).
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp of ingestion.

@bruin"""

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

GISCO_URL = (
    "https://gisco-services.ec.europa.eu/distribution/v2/nuts/geojson/"
    "NUTS_LB_2024_4326_LEVL_3.geojson"
)

EU27_CODES = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
    "DE", "EL", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
    "PL", "PT", "RO", "SK", "SI", "ES", "SE",
}


def fetch_geojson(url: str, retries: int = 5) -> dict:
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=120)
            if r.status_code in (429, 502, 503):
                wait = 10 * (attempt + 1)
                logger.warning("HTTP %d, retry in %ds", r.status_code, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            logger.warning("Network error attempt %d: %s", attempt + 1, e)
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts")


def _to_int_or_none(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def materialize():
    logger.info("Fetching NUTS3 label-point GeoJSON from GISCO 2024")
    payload = fetch_geojson(GISCO_URL)

    features = payload.get("features", [])
    logger.info("Total NUTS3 features: %d", len(features))

    rows = []
    for f in features:
        p = f["properties"]
        cc = p.get("CNTR_CODE")
        if cc not in EU27_CODES:
            continue
        coords = f["geometry"]["coordinates"]
        nuts_id = p["NUTS_ID"]
        rows.append({
            "nuts_id": nuts_id,
            "level_code": int(p.get("LEVL_CODE", 3)),
            "nuts1_id": nuts_id[:3],
            "nuts2_id": nuts_id[:4],
            "country_code": cc,
            "name_latn": p.get("NAME_LATN"),
            "name_native": p.get("NUTS_NAME"),
            "degurba": _to_int_or_none(p.get("URBN_TYPE")),
            "mount_type": _to_int_or_none(p.get("MOUNT_TYPE")),
            "coast_type": _to_int_or_none(p.get("COAST_TYPE")),
            "centroid_lat": float(coords[1]),
            "centroid_lon": float(coords[0]),
        })

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("EU-27 NUTS3 rows retained: %d", len(df))
    if len(df) < 1000:
        raise RuntimeError(f"Unexpectedly few NUTS3 rows ({len(df)}); GISCO schema may have changed")

    by_country = df.groupby("country_code").size().sort_values(ascending=False)
    logger.info("NUTS3 per country (top 5): %s", by_country.head(5).to_dict())

    return df
