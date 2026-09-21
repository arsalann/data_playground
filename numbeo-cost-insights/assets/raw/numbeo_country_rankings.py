"""@bruin
name: numbeo_raw.numbeo_country_rankings
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Current country-level cost-of-living, rent, food, restaurant, and local
  purchasing-power indices from Numbeo's public rankings page.
  Source: https://www.numbeo.com/cost-of-living/rankings_by_country.jsp

materialization:
  type: table
  strategy: append

columns:
  - name: country_snapshot_key
    type: VARCHAR
    description: Stable country and snapshot-date key used for deduplication.
    primary_key: true
  - name: country
    type: VARCHAR
    description: Country or territory name as published by Numbeo.
  - name: snapshot_date
    type: DATE
    description: UTC date on which the current rankings page was captured.
  - name: cost_of_living_index
    type: DOUBLE
    description: Consumer-goods and services cost index, with New York City set to 100.
  - name: rent_index
    type: DOUBLE
    description: Residential rent index, with New York City set to 100.
  - name: cost_of_living_plus_rent_index
    type: DOUBLE
    description: Combined consumer-cost and rent index, with New York City set to 100.
  - name: groceries_index
    type: DOUBLE
    description: Groceries price index, with New York City set to 100.
  - name: restaurant_price_index
    type: DOUBLE
    description: Restaurant price index, with New York City set to 100.
  - name: local_purchasing_power_index
    type: DOUBLE
    description: Local purchasing-power index, with New York City set to 100.
  - name: source_rank
    type: INTEGER
    description: Rank displayed by Numbeo on the captured page.
  - name: source_url
    type: VARCHAR
    description: Public Numbeo page used for the capture.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when the page was fetched.

@bruin"""

import logging
import os
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

SOURCE_URL = "https://www.numbeo.com/cost-of-living/rankings_by_country.jsp"


def _snapshot_date() -> str:
    return os.environ.get("BRUIN_END_DATE") or datetime.now(timezone.utc).date().isoformat()


def fetch_data(snapshot_date: str) -> pd.DataFrame:
    response = requests.get(
        SOURCE_URL,
        headers={"User-Agent": "Mozilla/5.0 (compatible; Bruin pipeline)"},
        timeout=45,
    )
    response.raise_for_status()
    tables = pd.read_html(StringIO(response.text))
    table = next(
        table for table in tables
        if {"Country", "Cost of Living Index", "Local Purchasing Power Index"}.issubset(table.columns)
    ).copy()
    table = table.rename(
        columns={
            "Rank": "source_rank",
            "Country": "country",
            "Cost of Living Index": "cost_of_living_index",
            "Rent Index": "rent_index",
            "Cost of Living Plus Rent Index": "cost_of_living_plus_rent_index",
            "Groceries Index": "groceries_index",
            "Restaurant Price Index": "restaurant_price_index",
            "Local Purchasing Power Index": "local_purchasing_power_index",
        }
    )
    table["source_rank"] = pd.to_numeric(table["source_rank"], errors="coerce").astype("Int64")
    for column in [
        "cost_of_living_index",
        "rent_index",
        "cost_of_living_plus_rent_index",
        "groceries_index",
        "restaurant_price_index",
        "local_purchasing_power_index",
    ]:
        table[column] = pd.to_numeric(table[column], errors="coerce")
    table["snapshot_date"] = pd.to_datetime(snapshot_date).date()
    table["country_snapshot_key"] = (
        table["country"].str.lower().str.replace(r"[^a-z0-9]+", "-", regex=True).str.strip("-")
        + "|"
        + table["snapshot_date"].astype(str)
    )
    table["source_url"] = SOURCE_URL
    table["extracted_at"] = datetime.now(timezone.utc)
    return table[
        [
            "country_snapshot_key", "country", "snapshot_date",
            "cost_of_living_index", "rent_index", "cost_of_living_plus_rent_index",
            "groceries_index", "restaurant_price_index", "local_purchasing_power_index",
            "source_rank", "source_url", "extracted_at",
        ]
    ]


def materialize() -> pd.DataFrame:
    snapshot_date = _snapshot_date()
    logger.info("Fetching Numbeo country rankings for snapshot date %s", snapshot_date)
    dataframe = fetch_data(snapshot_date)
    logger.info("Fetched %d country rows", len(dataframe))
    return dataframe
