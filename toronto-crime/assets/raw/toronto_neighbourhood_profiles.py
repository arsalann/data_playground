"""@bruin
name: raw.toronto_neighbourhood_profiles
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Ingests Toronto neighbourhood profile population data for the current
  158-neighbourhood model and historical 140-neighbourhood model from the City
  of Toronto Open Data CKAN package.

  Source: https://open.toronto.ca/dataset/neighbourhood-profiles/

materialization:
  type: table
  strategy: create+replace

columns:
  - name: neighbourhood_model
    type: INTEGER
    description: Toronto neighbourhood model number, either 158 or 140.
    primary_key: true
  - name: profile_year
    type: INTEGER
    description: Census/profile year used for the profile values.
    primary_key: true
  - name: neighbourhood_id
    type: VARCHAR
    description: Neighbourhood identifier within the model.
    primary_key: true
  - name: neighbourhood_name
    type: VARCHAR
    description: Neighbourhood name.
  - name: population
    type: INTEGER
    description: Resident population count from the profile census year.
  - name: land_area_km2
    type: DOUBLE
    description: Land area in square kilometres when included in the profile source; current 158 model is filled from boundary geometry in staging.
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

import io
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

PACKAGE_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=neighbourhood-profiles"
REQUEST_TIMEOUT = 120


def _get_package_resources() -> list[dict[str, Any]]:
    response = requests.get(PACKAGE_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()["result"]["resources"]


def _resource(resources: list[dict[str, Any]], name: str, fmt: str | None = None) -> dict[str, Any]:
    for resource in resources:
        if resource.get("name") == name and (fmt is None or resource.get("format", "").lower() == fmt.lower()):
            return resource
    raise ValueError(f"Could not find CKAN resource {name!r}")


def _clean_number(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().replace(",", "").replace("%", "")
    if text in {"", "nan", "NaN"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _clean_name(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return re.sub(r"\s+\(\d+\)$", "", text) or None


def _load_158(resource: dict[str, Any], extracted_at: datetime) -> list[dict[str, Any]]:
    response = requests.get(resource["url"], timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    profile = pd.read_excel(io.BytesIO(response.content), sheet_name="hd2021_census_profile", header=None)

    names = profile.iloc[0, 1:]
    ids = profile.iloc[1, 1:]
    population = profile.iloc[3, 1:]

    rows = []
    for name, neighbourhood_id, pop in zip(names, ids, population):
        clean_id = _clean_number(neighbourhood_id)
        clean_pop = _clean_number(pop)
        if clean_id is None or clean_pop is None:
            continue
        rows.append(
            {
                "neighbourhood_model": 158,
                "profile_year": 2021,
                "neighbourhood_id": f"{int(clean_id):03d}",
                "neighbourhood_name": _clean_name(name),
                "population": int(clean_pop),
                "land_area_km2": None,
                "source_resource_name": resource["name"],
                "source_resource_url": resource["url"],
                "extracted_at": extracted_at,
            }
        )
    return rows


def _row_value(df: pd.DataFrame, characteristic: str) -> pd.Series:
    matches = df["Characteristic"].astype(str).str.strip().eq(characteristic)
    if not matches.any():
        raise ValueError(f"Missing characteristic {characteristic!r}")
    return df.loc[matches].iloc[0]


def _load_140(resource: dict[str, Any], extracted_at: datetime) -> list[dict[str, Any]]:
    profile = pd.read_csv(resource["url"])
    number_row = _row_value(profile, "Neighbourhood Number")
    population_row = _row_value(profile, "Population, 2016")
    area_row = _row_value(profile, "Land area in square kilometres")

    rows = []
    metadata_columns = {"_id", "Category", "Topic", "Data Source", "Characteristic", "City of Toronto"}
    for column in profile.columns:
        if column in metadata_columns:
            continue
        clean_id = _clean_number(number_row[column])
        clean_pop = _clean_number(population_row[column])
        clean_area = _clean_number(area_row[column])
        if clean_id is None or clean_pop is None:
            continue
        rows.append(
            {
                "neighbourhood_model": 140,
                "profile_year": 2016,
                "neighbourhood_id": f"{int(clean_id):03d}",
                "neighbourhood_name": _clean_name(column),
                "population": int(clean_pop),
                "land_area_km2": clean_area,
                "source_resource_name": resource["name"],
                "source_resource_url": resource["url"],
                "extracted_at": extracted_at,
            }
        )
    return rows


def materialize():
    extracted_at = datetime.now(timezone.utc)
    resources = _get_package_resources()
    resource_158 = _resource(resources, "neighbourhood-profiles-2021-158-model", "XLSX")
    resource_140 = _resource(resources, "neighbourhood-profiles-2016-140-model.csv", "CSV")

    rows = _load_158(resource_158, extracted_at)
    rows.extend(_load_140(resource_140, extracted_at))

    logger.info("Fetched %d neighbourhood profile rows", len(rows))
    return pd.DataFrame(rows)
