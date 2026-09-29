"""@bruin
name: raw.edc_osm_data_centres
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  US features tagged as data centres in OpenStreetMap, fetched live from the
  Overpass API. This is the same underlying data behind the PNNL IM3 Open Source
  Data Center Atlas, taken directly so it can be refreshed without a repository
  login.

  Source: OpenStreetMap via https://overpass-api.de/api/interpreter
  Tags: telecom=data_center, building=data_center
  License: ODbL, attribution to OpenStreetMap contributors

  Coverage follows crowd-sourced tagging, so it under-represents facilities in
  areas with fewer OSM contributors and small non-obvious buildings. Centroids
  are computed from the Overpass "center" output for ways and relations.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: osm_id
    type: VARCHAR
    description: OSM element type and id, e.g. way/123456789
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: osm_type
    type: VARCHAR
    description: "OSM element type: node, way or relation"
  - name: name
    type: VARCHAR
    description: OSM name tag
  - name: operator
    type: VARCHAR
    description: OSM operator tag
  - name: lat
    type: DOUBLE
    description: Latitude in WGS84 decimal degrees (centroid for ways and relations)
  - name: lon
    type: DOUBLE
    description: Longitude in WGS84 decimal degrees (centroid for ways and relations)
  - name: telecom_tag
    type: VARCHAR
    description: Value of the telecom tag where present
  - name: building_tag
    type: VARCHAR
    description: Value of the building tag where present
  - name: state_tag
    type: VARCHAR
    description: addr:state tag where present
  - name: city_tag
    type: VARCHAR
    description: addr:city tag where present
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this snapshot was pulled

@bruin"""

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

OVERPASS_URL = os.environ.get("EDC_OVERPASS_URL", "https://overpass-api.de/api/interpreter")
USER_AGENT = "bruin-data-playground/1.0 (research; environment-data-centres pipeline)"
TIMEOUT = 300

QUERY = """
[out:json][timeout:280];
area["ISO3166-1"="US"][admin_level=2]->.us;
(
  nwr["telecom"="data_center"](area.us);
  nwr["building"="data_center"](area.us);
);
out center tags;
"""


def materialize():
    logger.info("Querying Overpass for US data centre features")
    response = requests.post(
        OVERPASS_URL, data={"data": QUERY}, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT
    )
    response.raise_for_status()
    elements = response.json().get("elements", [])

    if not elements:
        raise RuntimeError("Overpass returned no elements; refusing to replace the table")

    rows = []
    for element in elements:
        tags = element.get("tags") or {}
        center = element.get("center") or {}
        rows.append(
            {
                "osm_id": f"{element.get('type')}/{element.get('id')}",
                "osm_type": element.get("type"),
                "name": tags.get("name"),
                "operator": tags.get("operator"),
                "lat": element.get("lat", center.get("lat")),
                "lon": element.get("lon", center.get("lon")),
                "telecom_tag": tags.get("telecom"),
                "building_tag": tags.get("building"),
                "state_tag": tags.get("addr:state"),
                "city_tag": tags.get("addr:city"),
            }
        )

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "OSM: %d features (%s) | with coordinates: %d",
        len(df),
        df["osm_type"].value_counts().to_dict(),
        df["lat"].notna().sum(),
    )
    return df
