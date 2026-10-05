"""@bruin
name: raw.chicago_business_licenses
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Ingests the Business Licenses - Current Active dataset (uupf-x98q) from
  data.cityofchicago.org, published by the Department of Business Affairs and Consumer
  Protection. One row per currently active licence across all 77 community areas.

  Chicago is the strongest bar and nightclub source in this project and the weakest source
  for everything else. `license_description = 'Tavern'` is a clean bar universe, and
  `Public Place of Amusement` plus `Late Hour` cover nightclubs. But `Retail Food
  Establishment` is a single bucket in which cafes, bakeries and grocers are
  indistinguishable, and `Limited Business License` plus `Regulated Business License`
  hide all non-food retail including bookshops.

  Implementation note: the plan called for `type: ingestr` via the repo's
  `socrata-chicago-open-data` connection, but that connection's app_token is rejected by
  Socrata with HTTP 403 "Invalid app_token specified". Socrata serves this dataset
  unauthenticated at the volume needed here (roughly 55k rows), so this asset pages the
  SoQL endpoint directly with $limit and $offset. No credential is read or required.

  One business can hold several licences, so staging deduplicates on account_number plus
  site_number. `license_id` is unique across the snapshot; the dataset's own `id` is not.

  Source: https://data.cityofchicago.org/Community-Economic-Development/Business-Licenses-Current-Active/uupf-x98q
  Licence: City of Chicago open data terms

materialization:
  type: table
  strategy: append

columns:
  - name: license_id
    type: VARCHAR
    description: Unique identifier for the licence record.
    primary_key: true
  - name: account_number
    type: VARCHAR
    description: Business account number. One account can hold several licences at several sites.
  - name: site_number
    type: VARCHAR
    description: Site number within the account. account_number plus site_number identifies a physical premise.
  - name: license_number
    type: VARCHAR
    description: Licence number as issued.
  - name: legal_name
    type: VARCHAR
    description: Registered legal name of the licensee.
  - name: doing_business_as_name
    type: VARCHAR
    description: Trading name, the name on the premise.
  - name: license_code
    type: VARCHAR
    description: Numeric licence type code.
  - name: license_description
    type: VARCHAR
    description: Licence type label, for example "Tavern" or "Retail Food Establishment".
  - name: business_activity
    type: VARCHAR
    description: Pipe-delimited list of permitted business activities, more specific than license_description but frequently null.
  - name: license_status
    type: VARCHAR
    description: Licence status. This view carries active licences only.
  - name: address
    type: VARCHAR
    description: Street address of the licensed site.
  - name: zip_code
    type: VARCHAR
    description: ZIP code of the licensed site.
  - name: community_area
    type: VARCHAR
    description: Chicago community area number, 1-77.
  - name: community_area_name
    type: VARCHAR
    description: Chicago community area name.
  - name: neighborhood
    type: VARCHAR
    description: Neighbourhood name as recorded by the city.
  - name: ward
    type: VARCHAR
    description: City ward number.
  - name: precinct
    type: VARCHAR
    description: Precinct number within the ward.
  - name: police_district
    type: VARCHAR
    description: Police district number.
  - name: lon
    type: DOUBLE
    description: Longitude in decimal degrees, WGS84, as published. Null for sites the city could not geocode.
  - name: lat
    type: DOUBLE
    description: Latitude in decimal degrees, WGS84, as published. Null for sites the city could not geocode.
  - name: date_issued
    type: DATE
    description: Date the licence was issued.
  - name: license_start_date
    type: DATE
    description: Date the licence term begins.
  - name: expiration_date
    type: DATE
    description: Date the licence term ends.
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

DATASET_ID = "uupf-x98q"
BASE_URL = f"https://data.cityofchicago.org/resource/{DATASET_ID}.json"
PAGE_SIZE = 20000
REQUEST_TIMEOUT = 180
MAX_RETRIES = 5
SLEEP_BETWEEN_PAGES = 0.5

SELECT_FIELDS = [
    "license_id",
    "account_number",
    "site_number",
    "license_number",
    "legal_name",
    "doing_business_as_name",
    "license_code",
    "license_description",
    "business_activity",
    "license_status",
    "address",
    "zip_code",
    "community_area",
    "community_area_name",
    "neighborhood",
    "ward",
    "precinct",
    "police_district",
    "latitude",
    "longitude",
    "date_issued",
    "license_start_date",
    "expiration_date",
]

DATE_COLUMNS = ["date_issued", "license_start_date", "expiration_date"]

# City of Chicago bounding box, used only to flag out-of-range coordinates.
CHICAGO_BBOX = (-88.00, 41.60, -87.45, 42.10)


def get_json(params: dict[str, Any]) -> Any:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            if response.status_code in (429, 502, 503, 504):
                raise requests.RequestException(f"HTTP {response.status_code}")
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            if attempt == MAX_RETRIES:
                raise
            backoff = 2**attempt
            logger.warning(
                "Request %s failed on attempt %d/%d (%s), retrying in %ds",
                params,
                attempt,
                MAX_RETRIES,
                exc,
                backoff,
            )
            time.sleep(backoff)
    return []


def fetch_count() -> int:
    body = get_json({"$select": "count(1)"})
    return int(body[0]["count_1"])


def materialize() -> pd.DataFrame:
    expected = fetch_count()
    logger.info("Dataset %s reports %d active licences", DATASET_ID, expected)

    records: list[dict[str, Any]] = []
    offset = 0
    while True:
        page = get_json(
            {
                "$select": ",".join(SELECT_FIELDS),
                "$order": "license_id",
                "$limit": PAGE_SIZE,
                "$offset": offset,
            }
        )
        if not page:
            break
        records.extend(page)
        logger.info("Fetched %d / %d rows", len(records), expected)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(SLEEP_BETWEEN_PAGES)

    df = pd.DataFrame(records)
    logger.info("Assembled %d rows, %d columns", len(df), len(df.columns))
    if len(df) != expected:
        logger.warning(
            "Row count %d does not match the reported count %d", len(df), expected
        )

    # The API omits keys entirely for null values, so absent columns must be added back.
    for column in SELECT_FIELDS:
        if column not in df.columns:
            logger.warning("Column %s absent from the response; filling with nulls", column)
            df[column] = None

    df = df.rename(columns={"latitude": "lat", "longitude": "lon"})

    for column in ("lon", "lat"):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    for column in DATE_COLUMNS:
        df[column] = pd.to_datetime(df[column], errors="coerce").dt.date

    for column in df.columns:
        if df[column].dtype == object:
            df[column] = df[column].astype("string").str.strip().replace({"": None})

    before = len(df)
    df = df.dropna(subset=["license_id"]).drop_duplicates(
        subset=["license_id"], keep="first"
    )
    if len(df) != before:
        logger.info("Dropped %d rows with a null or duplicate license_id", before - len(df))

    geocoded = int((df["lon"].notna() & df["lat"].notna()).sum())
    logger.info(
        "Geocoding: %d of %d rows carry lon/lat (%.2f%%), %d null",
        geocoded,
        len(df),
        100.0 * geocoded / max(len(df), 1),
        len(df) - geocoded,
    )
    min_lon, min_lat, max_lon, max_lat = CHICAGO_BBOX
    inside = df["lon"].between(min_lon, max_lon) & df["lat"].between(min_lat, max_lat)
    outside = int((df["lon"].notna() & ~inside).sum())
    if outside:
        logger.warning("%d rows carry coordinates outside the Chicago bounding box", outside)
    logger.info(
        "Bounding box: lon %.4f..%.4f, lat %.4f..%.4f",
        df["lon"].min(),
        df["lon"].max(),
        df["lat"].min(),
        df["lat"].max(),
    )

    logger.info("Community areas covered: %d", df["community_area"].nunique())
    logger.info("Distinct premises (account_number + site_number): %d",
                df.groupby(["account_number", "site_number"]).ngroups)
    logger.info(
        "Licence types: %s",
        df["license_description"].value_counts(dropna=False).head(12).to_dict(),
    )

    df["extracted_at"] = datetime.now(timezone.utc)

    ordered = [
        "license_id",
        "account_number",
        "site_number",
        "license_number",
        "legal_name",
        "doing_business_as_name",
        "license_code",
        "license_description",
        "business_activity",
        "license_status",
        "address",
        "zip_code",
        "community_area",
        "community_area_name",
        "neighborhood",
        "ward",
        "precinct",
        "police_district",
        "lon",
        "lat",
        "date_issued",
        "license_start_date",
        "expiration_date",
        "extracted_at",
    ]
    df = df[ordered]

    logger.info("Returning %d rows", len(df))
    return df
