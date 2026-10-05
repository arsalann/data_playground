"""@bruin

name: raw.world_bank_context
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  World Bank World Development Indicators used to contextualize transit
  construction projects by country and year.

  Indicators: GDP per capita, PPP (constant 2021 international dollars), total
  population, and urban population as a percentage of total population.
  Source: https://data.worldbank.org/ and https://api.worldbank.org/v2/
  License: World Bank Open Data, CC BY 4.0.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: country_code
    type: VARCHAR
    description: World Bank country or aggregate two-character code.
    primary_key: true
  - name: country_name
    type: VARCHAR
    description: World Bank country or aggregate name.
  - name: year
    type: INTEGER
    description: Observation year.
    primary_key: true
  - name: gdp_per_capita_ppp_2021
    type: DOUBLE
    description: GDP per capita in constant 2021 international dollars at purchasing power parity.
  - name: population
    type: DOUBLE
    description: Total population in persons.
  - name: urban_population_pct
    type: DOUBLE
    description: Urban population as a percentage of total population.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this batch was fetched.

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

API_URL = (
    "https://api.worldbank.org/v2/country/all/indicator/"
    "NY.GDP.PCAP.PP.KD;SP.POP.TOTL;SP.URB.TOTL.IN.ZS"
)


def fetch_window(start_year: int, end_year: int) -> list[dict]:
    params = {
        "date": f"{start_year}:{end_year}",
        "format": "json",
        "per_page": 20000,
        "source": 2,
    }
    for attempt in range(5):
        try:
            response = requests.get(API_URL, params=params, timeout=120)
            if response.status_code == 429 or response.status_code >= 500:
                raise requests.HTTPError(f"retryable HTTP {response.status_code}")
            response.raise_for_status()
            payload = response.json()
            return payload[1] if len(payload) > 1 and payload[1] else []
        except (requests.RequestException, ValueError) as exc:
            wait_seconds = 2 ** attempt
            logger.warning(
                "World Bank window %d-%d failed; retrying in %ds: %s",
                start_year, end_year, wait_seconds, exc,
            )
            time.sleep(wait_seconds)
    logger.error("World Bank window %d-%d failed after five attempts", start_year, end_year)
    return []


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "1960-01-01")
    end_date = os.environ.get(
        "BRUIN_END_DATE", f"{datetime.now(timezone.utc).year - 1}-12-31"
    )
    start_year = max(1960, int(start_date[:4]))
    end_year = min(datetime.now(timezone.utc).year, int(end_date[:4]))
    extracted_at = datetime.now(timezone.utc)

    records = []
    for window_start in range(start_year, end_year + 1, 10):
        window_end = min(window_start + 9, end_year)
        rows = fetch_window(window_start, window_end)
        records.extend(rows)
        logger.info(
            "World Bank window %d-%d returned %d records",
            window_start, window_end, len(rows),
        )

    values: dict[tuple[str, int], dict] = {}
    for row in records:
        country = row.get("country", {})
        country_code = (country.get("id") or "").upper()
        country_name = country.get("value") or ""
        try:
            year = int(row.get("date"))
        except (TypeError, ValueError):
            continue

        key = (country_code, year)
        record = values.setdefault(
            key,
            {
                "country_code": country_code,
                "country_name": country_name,
                "year": year,
                "gdp_per_capita_ppp_2021": None,
                "population": None,
                "urban_population_pct": None,
                "extracted_at": extracted_at,
            },
        )
        indicator = row.get("indicator", {}).get("id")
        value = row.get("value")
        if indicator == "NY.GDP.PCAP.PP.KD":
            record["gdp_per_capita_ppp_2021"] = value
        elif indicator == "SP.POP.TOTL":
            record["population"] = value
        elif indicator == "SP.URB.TOTL.IN.ZS":
            record["urban_population_pct"] = value

    frame = pd.DataFrame(values.values())
    logger.info("Prepared %d country-year context rows", len(frame))
    return frame
