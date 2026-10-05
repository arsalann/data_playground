"""@bruin
name: raw.osm_madrid_amenities
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Ingests OpenStreetMap bar, cafe, restaurant, fast-food, bakery, nightclub and bookshop
  objects for the Madrid municipal bounding box via the Overpass API.

  This asset exists to answer the question the reference project could not: how complete is
  OpenStreetMap for retail premises? Madrid is the right test case because it has the
  strongest official register in the shortlist, so the register can serve as the reference
  universe rather than being just another estimate. `report.osm_register_gap` turns these
  rows into the published comparison.

  Tags follow the standard OSM amenity and shop schema so the query is directly comparable
  to what the reference project used: amenity=bar/cafe/restaurant/fast_food/nightclub/pub
  and shop=bakery/books.

  Source: https://overpass-api.de/api/interpreter
  Licence: ODbL (OpenStreetMap contributors)

materialization:
  type: table
  strategy: append

columns:
  - name: osm_type
    type: VARCHAR
    description: OpenStreetMap element type, node or way.
    primary_key: true
  - name: osm_id
    type: INTEGER
    description: OpenStreetMap element identifier, unique within osm_type.
    primary_key: true
  - name: osm_key
    type: VARCHAR
    description: Tag key that matched, either amenity or shop.
  - name: osm_value
    type: VARCHAR
    description: Tag value that matched, for example bar, cafe, bakery or books.
  - name: shop_type
    type: VARCHAR
    description: Canonical shop type this OSM tag maps to, matching staging.shop_type_crosswalk.
  - name: name
    type: VARCHAR
    description: Object name as tagged, blank where untagged.
  - name: lon
    type: DOUBLE
    description: Longitude in decimal degrees, WGS84. For ways this is the centroid Overpass returns.
  - name: lat
    type: DOUBLE
    description: Latitude in decimal degrees, WGS84. For ways this is the centroid Overpass returns.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was extracted.

@bruin"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
# Overpass returns HTTP 406 for requests without an identifying User-Agent.
OVERPASS_HEADERS = {"User-Agent": "cities-shops-map/1.0 (bruin pipeline; data-playground)"}
REQUEST_TIMEOUT = 300
MAX_RETRIES = 5

# Madrid municipal bounding box as (min_lat, min_lon, max_lat, max_lon), Overpass ordering.
# Identical to the box used by every other Madrid asset in this pipeline.
MADRID_BBOX = (40.312, -3.889, 40.644, -3.518)

# OSM tag to canonical shop type. Deliberately the tags a naive OSM-only study would reach
# for, so the comparison measures that method rather than a tuned version of it.
TAG_MAP: dict[tuple[str, str], str] = {
    ("amenity", "bar"): "bar_pub",
    ("amenity", "pub"): "bar_pub",
    ("amenity", "nightclub"): "nightclub",
    ("amenity", "cafe"): "cafe",
    ("amenity", "restaurant"): "restaurant",
    ("amenity", "fast_food"): "fast_food",
    ("shop", "bakery"): "bakery",
    ("shop", "pastry"): "bakery",
    ("shop", "books"): "bookstore",
}


def build_query() -> str:
    bounds = ",".join(str(v) for v in MADRID_BBOX)
    clauses = "\n".join(
        f'  node["{key}"="{value}"]({bounds});\n  way["{key}"="{value}"]({bounds});'
        for key, value in TAG_MAP
    )
    return f"[out:json][timeout:240];\n(\n{clauses}\n);\nout tags center;"


def run_query(query: str) -> list[dict[str, Any]]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                OVERPASS_URL,
                data={"data": query},
                headers=OVERPASS_HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
            if response.status_code in (429, 502, 503, 504):
                raise requests.RequestException(f"HTTP {response.status_code}")
            response.raise_for_status()
            return response.json().get("elements", [])
        except (requests.RequestException, ValueError) as exc:
            if attempt == MAX_RETRIES:
                raise
            backoff = 5 * 2 ** (attempt - 1)
            logger.warning(
                "Overpass query failed on attempt %d/%d (%s), retrying in %ds",
                attempt,
                MAX_RETRIES,
                exc,
                backoff,
            )
            time.sleep(backoff)
    return []


def flatten(element: dict[str, Any]) -> dict[str, Any] | None:
    tags = element.get("tags") or {}
    matched: tuple[str, str] | None = None
    for key, value in TAG_MAP:
        if tags.get(key) == value:
            matched = (key, value)
            break
    if matched is None:
        return None

    if element["type"] == "node":
        lon, lat = element.get("lon"), element.get("lat")
    else:
        center = element.get("center") or {}
        lon, lat = center.get("lon"), center.get("lat")
    if lon is None or lat is None:
        return None

    key, value = matched
    return {
        "osm_type": element["type"],
        "osm_id": element["id"],
        "osm_key": key,
        "osm_value": value,
        "shop_type": TAG_MAP[matched],
        "name": tags.get("name"),
        "lon": lon,
        "lat": lat,
    }


def materialize() -> pd.DataFrame:
    elements = run_query(build_query())
    logger.info("Overpass returned %d elements", len(elements))

    rows = [flatten(e) for e in elements]
    rows = [r for r in rows if r is not None]
    logger.info("Kept %d elements with a mapped tag and coordinates", len(rows))

    df = pd.DataFrame(rows)

    before = len(df)
    df = df.drop_duplicates(subset=["osm_type", "osm_id"], keep="first")
    if len(df) != before:
        logger.info("Dropped %d duplicate elements", before - len(df))

    df["osm_id"] = pd.to_numeric(df["osm_id"], errors="coerce").astype("Int64")
    for column in ("lon", "lat"):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["name"] = df["name"].astype("string").str.strip().replace({"": None})

    logger.info("Counts by shop type: %s", df["shop_type"].value_counts().to_dict())
    logger.info("Counts by OSM tag: %s", df.groupby(["osm_key", "osm_value"]).size().to_dict())
    logger.info(
        "Named objects: %d of %d (%.1f%%)",
        int(df["name"].notna().sum()),
        len(df),
        100.0 * df["name"].notna().sum() / max(len(df), 1),
    )

    df["extracted_at"] = datetime.now(timezone.utc)

    ordered = [
        "osm_type",
        "osm_id",
        "osm_key",
        "osm_value",
        "shop_type",
        "name",
        "lon",
        "lat",
        "extracted_at",
    ]
    df = df[ordered]

    logger.info("Returning %d rows", len(df))
    return df
