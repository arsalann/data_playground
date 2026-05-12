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

  Processing approach: Iterates through each EU-27 country individually to avoid
  timeout on large multi-country queries. Complex geometries (ways/relations) are
  reduced to representative centroid points. Deduplicates by OSM element ID to
  handle cross-boundary installations.

  Data coverage: Includes active military bases, decommissioned sites, training
  areas, naval facilities, airfields, and ranges. Coverage varies by country
  based on OSM contributor activity and military transparency policies.

  Operational characteristics: Weekly refresh cadence matches OSM update frequency.
  Expected size ~2,000-5,000 rows (varies by OSM mapping activity). Create+replace
  strategy suitable given reference data nature and manageable size. Query timeout
  set to 180s per country with 2-second delays between requests to respect API limits.

  Endpoint: https://overpass-api.de/api/interpreter (rotating mirror).
  License: ODbL (OpenStreetMap contributors).
connection: bruin-playground-arsalan
tags:
  - eu-27
  - pfas
  - raw
  - openstreetmap
  - military
  - overlay
  - geospatial
  - reference_data
  - environmental_risk
  - external_source
  - point_data

materialization:
  type: table
  strategy: create+replace
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: osm_id
    type: VARCHAR
    description: OSM element identifier prefixed with type (n/node, w/way, r/relation) - unique globally and used as primary key.
    primary_key: true
    checks:
      - name: not_null
  - name: name
    type: VARCHAR
    description: Official site name from OSM tags (frequently null for relations and classified installations).
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2 country code derived from query bounding box. Geographic dimension for analysis.
    checks:
      - name: not_null
  - name: military_type
    type: VARCHAR
    description: OSM military tag value indicating facility function (airfield, base, training_area, barracks, naval_base, range).
  - name: lat
    type: DOUBLE
    description: Latitude coordinate in WGS84 decimal degrees. Centroid point for complex geometries (ways/relations).
    checks:
      - name: not_null
  - name: lon
    type: DOUBLE
    description: Longitude coordinate in WGS84 decimal degrees. Centroid point for complex geometries.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp of data ingestion from Overpass API. Used for deduplication and data lineage.

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
