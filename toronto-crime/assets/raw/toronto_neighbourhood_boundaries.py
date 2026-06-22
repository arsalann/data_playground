"""@bruin
name: raw.toronto_neighbourhood_boundaries
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Ingests current 158-neighbourhood and historical 140-neighbourhood boundary
  GeoJSON resources from the City of Toronto Open Data CKAN package.

  Source: https://open.toronto.ca/dataset/neighbourhoods/

materialization:
  type: table
  strategy: create+replace

columns:
  - name: neighbourhood_model
    type: INTEGER
    description: Toronto neighbourhood model number, either 158 or 140.
    primary_key: true
  - name: neighbourhood_id
    type: VARCHAR
    description: Neighbourhood identifier within the model.
    primary_key: true
  - name: neighbourhood_name
    type: VARCHAR
    description: Neighbourhood name.
  - name: geojson_geometry
    type: VARCHAR
    description: Polygon or multipolygon geometry serialized as GeoJSON in WGS84.
  - name: source_resource_name
    type: VARCHAR
    description: CKAN resource name used for this row.
  - name: source_resource_url
    type: VARCHAR
    description: CKAN resource URL used for this row.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was extracted.

@bruin"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

PACKAGE_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=neighbourhoods"
REQUEST_TIMEOUT = 120


def _get_package_resources() -> list[dict[str, Any]]:
    response = requests.get(PACKAGE_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()["result"]["resources"]


def _resource(resources: list[dict[str, Any]], name: str) -> dict[str, Any]:
    for resource in resources:
        if resource.get("name") == name and resource.get("format", "").lower() == "geojson":
            return resource
    raise ValueError(f"Could not find CKAN GeoJSON resource {name!r}")


def _clean_id(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return f"{int(float(text)):03d}"
    except ValueError:
        return text


def _clean_name(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return re.sub(r"\s+\(\d+\)$", "", text) or None


def _load_geojson(resource: dict[str, Any], model: int, extracted_at: datetime) -> list[dict[str, Any]]:
    response = requests.get(resource["url"], timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()

    rows = []
    for feature in payload.get("features", []):
        props = feature.get("properties") or {}
        rows.append(
            {
                "neighbourhood_model": model,
                "neighbourhood_id": _clean_id(props.get("AREA_SHORT_CODE") or props.get("AREA_LONG_CODE")),
                "neighbourhood_name": _clean_name(props.get("AREA_NAME") or props.get("AREA_DESC")),
                "geojson_geometry": json.dumps(feature.get("geometry"), separators=(",", ":")),
                "source_resource_name": resource["name"],
                "source_resource_url": resource["url"],
                "extracted_at": extracted_at,
            }
        )
    return rows


def materialize():
    extracted_at = datetime.now(timezone.utc)
    resources = _get_package_resources()
    current = _resource(resources, "Neighbourhoods - 4326.geojson")
    historical = _resource(resources, "Neighbourhoods - historical 140 - 4326.geojson")

    rows = _load_geojson(current, 158, extracted_at)
    rows.extend(_load_geojson(historical, 140, extracted_at))

    logger.info("Fetched %d neighbourhood boundary rows", len(rows))
    return pd.DataFrame(rows)
