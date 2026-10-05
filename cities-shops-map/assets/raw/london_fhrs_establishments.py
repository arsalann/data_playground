"""@bruin
name: raw.london_fhrs_establishments
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Ingests every food establishment in the 33 Greater London local authorities from the
  Food Standards Agency's Food Hygiene Rating Scheme API.

  FHRS is a statutory register: any business selling food to the public must be on it, which
  makes coverage of food premises close to complete. The cost is category granularity.
  BusinessTypeID 1 is a single bucket labelled "Restaurant/Cafe/Canteen", so cafes cannot be
  separated from restaurants, and 7843 merges pubs, bars and nightclubs into one class.

  Two implementation notes:
  - The `x-api-version: 2` header is mandatory; without it the API returns a different shape.
  - The `/Authorities/basic` endpoint carries no RegionName field, so London authorities
    cannot be identified from it. `/Authorities?pageSize=500` does carry RegionName and is
    used to resolve the 33 London LocalAuthorityIds.

  Source: https://api.ratings.food.gov.uk/help
  Licence: Open Government Licence v3.0 (Food Standards Agency)

materialization:
  type: table
  strategy: append

columns:
  - name: fhrs_id
    type: INTEGER
    description: FHRS establishment identifier, unique nationally.
    primary_key: true
  - name: business_name
    type: VARCHAR
    description: Trading name of the establishment.
  - name: business_type
    type: VARCHAR
    description: FHRS business type label, for example "Restaurant/Cafe/Canteen" or "Pub/bar/nightclub".
  - name: business_type_id
    type: INTEGER
    description: FHRS business type identifier. 1 restaurant/cafe/canteen, 7843 pub/bar/nightclub, 7844 takeaway/sandwich shop.
  - name: local_authority_id
    type: INTEGER
    description: FHRS local authority identifier, one of the 33 London boroughs.
  - name: local_authority_name
    type: VARCHAR
    description: London borough name.
  - name: address_line_1
    type: VARCHAR
    description: First address line as published, frequently blank.
  - name: address_line_2
    type: VARCHAR
    description: Second address line as published, usually the street.
  - name: address_line_3
    type: VARCHAR
    description: Third address line as published.
  - name: address_line_4
    type: VARCHAR
    description: Fourth address line as published, usually the locality.
  - name: postcode
    type: VARCHAR
    description: UK postcode.
  - name: lon
    type: DOUBLE
    description: Longitude in decimal degrees, WGS84. Address-derived, with postcode-centroid fallbacks and nulls.
  - name: lat
    type: DOUBLE
    description: Latitude in decimal degrees, WGS84. Address-derived, with postcode-centroid fallbacks and nulls.
  - name: rating_value
    type: VARCHAR
    description: Food hygiene rating as published, 0-5 for FHRS or a pass/improvement label for FHIS.
  - name: rating_date
    type: DATE
    description: Date of the most recent hygiene inspection that produced the rating.
  - name: scheme_type
    type: VARCHAR
    description: Rating scheme, FHRS in England or FHIS in Scotland. London is entirely FHRS.
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

BASE_URL = "https://api.ratings.food.gov.uk"
HEADERS = {"x-api-version": "2", "accept": "application/json"}
TARGET_REGION = "London"
PAGE_SIZE = 5000
REQUEST_TIMEOUT = 180
MAX_RETRIES = 5
SLEEP_BETWEEN_REQUESTS = 0.5

# Greater London bounding box, used only to flag out-of-range coordinates.
LONDON_BBOX = (-0.60, 51.24, 0.40, 51.73)


def get_json(path: str, params: dict[str, Any]) -> dict[str, Any]:
    url = f"{BASE_URL}/{path}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT
            )
            if response.status_code in (429, 502, 503, 504):
                raise requests.RequestException(f"HTTP {response.status_code}")
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            if attempt == MAX_RETRIES:
                raise
            backoff = 2**attempt
            logger.warning(
                "%s %s failed on attempt %d/%d (%s), retrying in %ds",
                path,
                params,
                attempt,
                MAX_RETRIES,
                exc,
                backoff,
            )
            time.sleep(backoff)
    return {}


def london_authorities() -> list[dict[str, Any]]:
    body = get_json("Authorities", {"pageSize": 500, "pageNumber": 1})
    authorities = body.get("authorities", [])
    london = [a for a in authorities if a.get("RegionName") == TARGET_REGION]
    logger.info(
        "Resolved %d %s authorities out of %d nationally",
        len(london),
        TARGET_REGION,
        len(authorities),
    )
    expected = sum(int(a.get("EstablishmentCount") or 0) for a in london)
    logger.info("Publisher reports %d establishments across those authorities", expected)
    return london


def fetch_authority(authority_id: int, name: str) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    page = 1
    while True:
        body = get_json(
            "Establishments",
            {"localAuthorityId": authority_id, "pageSize": PAGE_SIZE, "pageNumber": page},
        )
        establishments = body.get("establishments", [])
        collected.extend(establishments)
        total_pages = int(body.get("meta", {}).get("totalPages") or 1)
        if page >= total_pages or not establishments:
            break
        page += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    logger.info("%s (id %d): %d establishments", name, authority_id, len(collected))
    return collected


def flatten(record: dict[str, Any]) -> dict[str, Any]:
    geocode = record.get("geocode") or {}
    return {
        "fhrs_id": record.get("FHRSID"),
        "business_name": record.get("BusinessName"),
        "business_type": record.get("BusinessType"),
        "business_type_id": record.get("BusinessTypeID"),
        "local_authority_id": record.get("LocalAuthorityCode"),
        "local_authority_name": record.get("LocalAuthorityName"),
        "address_line_1": record.get("AddressLine1"),
        "address_line_2": record.get("AddressLine2"),
        "address_line_3": record.get("AddressLine3"),
        "address_line_4": record.get("AddressLine4"),
        "postcode": record.get("PostCode"),
        "lon": geocode.get("longitude"),
        "lat": geocode.get("latitude"),
        "rating_value": record.get("RatingValue"),
        "rating_date": record.get("RatingDate"),
        "scheme_type": record.get("SchemeType"),
    }


def materialize() -> pd.DataFrame:
    authorities = london_authorities()

    records: list[dict[str, Any]] = []
    for authority in authorities:
        authority_id = int(authority["LocalAuthorityId"])
        rows = fetch_authority(authority_id, authority.get("Name", str(authority_id)))
        for row in rows:
            flat = flatten(row)
            # LocalAuthorityCode is a scheme code, not the id used to query; keep the query id.
            flat["local_authority_id"] = authority_id
            records.append(flat)
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    df = pd.DataFrame(records)
    logger.info("Assembled %d establishment rows", len(df))

    df["fhrs_id"] = pd.to_numeric(df["fhrs_id"], errors="coerce").astype("Int64")
    df["business_type_id"] = pd.to_numeric(df["business_type_id"], errors="coerce").astype(
        "Int64"
    )
    df["local_authority_id"] = pd.to_numeric(
        df["local_authority_id"], errors="coerce"
    ).astype("Int64")
    for column in ("lon", "lat"):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["rating_date"] = pd.to_datetime(df["rating_date"], errors="coerce").dt.date

    for column in df.columns:
        if df[column].dtype == object:
            df[column] = df[column].astype("string").str.strip().replace({"": None})

    before = len(df)
    df = df.dropna(subset=["fhrs_id"]).drop_duplicates(subset=["fhrs_id"], keep="first")
    if len(df) != before:
        logger.info("Dropped %d rows with a null or duplicate FHRSID", before - len(df))

    geocoded = int((df["lon"].notna() & df["lat"].notna()).sum())
    logger.info(
        "Geocoding: %d of %d rows carry lon/lat (%.2f%%), %d null",
        geocoded,
        len(df),
        100.0 * geocoded / max(len(df), 1),
        len(df) - geocoded,
    )
    min_lon, min_lat, max_lon, max_lat = LONDON_BBOX
    inside = df["lon"].between(min_lon, max_lon) & df["lat"].between(min_lat, max_lat)
    outside = int((df["lon"].notna() & ~inside).sum())
    if outside:
        logger.warning(
            "%d rows carry coordinates outside the Greater London bounding box", outside
        )
    logger.info("Boroughs covered: %d", df["local_authority_name"].nunique())
    logger.info(
        "Business types: %s", df["business_type"].value_counts(dropna=False).to_dict()
    )

    df["extracted_at"] = datetime.now(timezone.utc)

    ordered = [
        "fhrs_id",
        "business_name",
        "business_type",
        "business_type_id",
        "local_authority_id",
        "local_authority_name",
        "address_line_1",
        "address_line_2",
        "address_line_3",
        "address_line_4",
        "postcode",
        "lon",
        "lat",
        "rating_value",
        "rating_date",
        "scheme_type",
        "extracted_at",
    ]
    df = df[ordered]

    logger.info("Returning %d rows", len(df))
    return df
