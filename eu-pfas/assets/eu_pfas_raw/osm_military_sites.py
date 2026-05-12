"""@bruin

name: eu_pfas_raw.osm_military_sites
description: |
  Military installations across EU-27 from OpenStreetMap (OSM) via the Overpass
  API.

  Why military matters for PFAS: aqueous film-forming foam (AFFF) used for
  fire-training and aircraft-incident response is a documented major PFAS source.
  Bases active or decommissioned since the 1950s leak PFAS into groundwater for
  decades.

  Overpass query: nodes, ways, and relations with `landuse=military` OR
  `military=*` within EU-27 bounding box. Result is reduced to centroid
  coordinates (representative point) plus type and country.

  Endpoint: https://overpass-api.de/api/interpreter (rotating mirror).
  License: ODbL (OpenStreetMap contributors).
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: osm_id
    type: VARCHAR
    description: OSM element id, prefixed with element type (n/w/r). Primary key.
    primary_key: true
    checks:
      - name: not_null
  - name: name
    type: VARCHAR
    description: Site name from OSM (often null for relations).
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2 country code (derived from bbox-then-country tagging where present).
  - name: military_type
    type: VARCHAR
    description: OSM tag value (e.g., airfield, base, training_area, barracks, naval_base, range).
  - name: lat
    type: DOUBLE
    description: Representative latitude (centroid for ways/relations).
  - name: lon
    type: DOUBLE
    description: Representative longitude.
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

OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.osm.ch/api/interpreter",
]

EU27_ISO2 = [
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
    "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
    "PL", "PT", "RO", "SK", "SI", "ES", "SE",
]

QUERY_TEMPLATE = """
[out:json][timeout:180];
area["ISO3166-1"="{iso2}"]->.country;
(
  node(area.country)["military"];
  way(area.country)["military"];
  relation(area.country)["military"];
);
out tags center 1000;
"""


def fetch_country(iso2: str, retries_per_mirror: int = 2) -> dict | None:
    query = QUERY_TEMPLATE.format(iso2=iso2)
    for mirror in OVERPASS_MIRRORS:
        for attempt in range(retries_per_mirror):
            try:
                r = requests.post(mirror, data={"data": query}, timeout=240)
                if r.status_code == 200:
                    return r.json()
                if r.status_code in (429, 502, 503, 504):
                    wait = 20 * (attempt + 1)
                    logger.warning("[%s] %s -> HTTP %d, retry in %ds",
                                   iso2, mirror, r.status_code, wait)
                    time.sleep(wait)
                    continue
                logger.error("[%s] %s -> HTTP %d, content=%s",
                             iso2, mirror, r.status_code, r.text[:200])
                break
            except requests.RequestException as e:
                wait = 15 * (attempt + 1)
                logger.warning("[%s] %s err %s, retry %ds", iso2, mirror, e, wait)
                time.sleep(wait)
        logger.info("[%s] Moving to next mirror after exhausted retries", iso2)
    return None


def parse_country(payload: dict, iso2: str) -> list[dict]:
    rows = []
    for el in payload.get("elements", []):
        el_id = f"{el['type'][0]}/{el['id']}"
        tags = el.get("tags", {})
        mil_type = tags.get("military") or tags.get("landuse")
        if el["type"] == "node":
            lat = el.get("lat")
            lon = el.get("lon")
        else:
            center = el.get("center", {})
            lat = center.get("lat")
            lon = center.get("lon")
        if lat is None or lon is None:
            continue
        rows.append({
            "osm_id": el_id,
            "name": tags.get("name"),
            "country_code": iso2,
            "military_type": mil_type,
            "lat": float(lat),
            "lon": float(lon),
        })
    return rows


def materialize():
    logger.info("Fetching OSM military for %d EU-27 countries via Overpass", len(EU27_ISO2))
    all_rows = []
    for iso2 in EU27_ISO2:
        logger.info("Querying %s", iso2)
        payload = fetch_country(iso2)
        if not payload:
            logger.warning("[%s] no payload", iso2)
            continue
        rows = parse_country(payload, iso2)
        logger.info("  %s rows=%d", iso2, len(rows))
        all_rows.extend(rows)
        time.sleep(2)

    if not all_rows:
        raise RuntimeError("Overpass returned no military rows -- all mirrors failed?")

    df = pd.DataFrame(all_rows)
    df = df.drop_duplicates(subset=["osm_id"], keep="first")
    df["extracted_at"] = datetime.now(timezone.utc)

    by_country = df.groupby("country_code").size().sort_values(ascending=False)
    logger.info("Total military sites: %d, top countries: %s",
                len(df), by_country.head(8).to_dict())

    return df
