"""@bruin
name: raw.aep_owid_energy
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Downloads the Our World in Data energy dataset CSV (~40MB).
  Filters to relevant columns for the AI Energy Paradox analysis:
  generation by source, shares, per-capita metrics, carbon intensity,
  GDP, and population.

  Data source: https://github.com/owid/energy-data
  License: CC-BY

materialization:
  type: table
  strategy: create+replace

columns:
  - name: country
    type: VARCHAR
    description: Country or region name
    primary_key: true
  - name: year
    type: INTEGER
    description: Year of the observation
    primary_key: true
  - name: iso_code
    type: VARCHAR
    description: ISO 3166-1 alpha-3 country code (null for aggregates like World)
  - name: population
    type: DOUBLE
    description: Total population
  - name: gdp
    type: DOUBLE
    description: GDP in international dollars (PPP, 2017 prices)
  - name: electricity_generation
    type: DOUBLE
    description: Total electricity generation (TWh)
  - name: electricity_demand
    type: DOUBLE
    description: Total electricity demand (TWh)
  - name: coal_electricity
    type: DOUBLE
    description: Electricity from coal (TWh)
  - name: gas_electricity
    type: DOUBLE
    description: Electricity from gas (TWh)
  - name: oil_electricity
    type: DOUBLE
    description: Electricity from oil (TWh)
  - name: nuclear_electricity
    type: DOUBLE
    description: Electricity from nuclear (TWh)
  - name: hydro_electricity
    type: DOUBLE
    description: Electricity from hydro (TWh)
  - name: wind_electricity
    type: DOUBLE
    description: Electricity from wind (TWh)
  - name: solar_electricity
    type: DOUBLE
    description: Electricity from solar (TWh)
  - name: biofuel_electricity
    type: DOUBLE
    description: Electricity from biofuels (TWh)
  - name: other_renewable_electricity
    type: DOUBLE
    description: Electricity from other renewables excluding hydro (TWh)
  - name: other_renewable_exc_biofuel_electricity
    type: DOUBLE
    description: Electricity from other renewables excluding biofuel (TWh)
  - name: renewables_share_elec
    type: DOUBLE
    description: Share of electricity from renewables (%)
  - name: fossil_share_elec
    type: DOUBLE
    description: Share of electricity from fossil fuels (%)
  - name: nuclear_share_elec
    type: DOUBLE
    description: Share of electricity from nuclear (%)
  - name: carbon_intensity_elec
    type: DOUBLE
    description: Carbon intensity of electricity (gCO2/kWh)
  - name: primary_energy_consumption
    type: DOUBLE
    description: Primary energy consumption (TWh)
  - name: per_capita_electricity
    type: DOUBLE
    description: Per capita electricity generation (kWh)
  - name: energy_per_capita
    type: DOUBLE
    description: Per capita primary energy consumption (kWh)
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was fetched

@bruin"""

import io
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

OWID_URL = "https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-data.csv"

KEEP_COLUMNS = [
    "country", "year", "iso_code", "population", "gdp",
    "electricity_generation", "electricity_demand",
    "coal_electricity", "gas_electricity", "oil_electricity",
    "nuclear_electricity", "hydro_electricity", "wind_electricity",
    "solar_electricity", "biofuel_electricity",
    "other_renewable_electricity", "other_renewable_exc_biofuel_electricity",
    "renewables_share_elec", "fossil_share_elec", "nuclear_share_elec",
    "carbon_intensity_elec", "primary_energy_consumption",
    "per_capita_electricity", "energy_per_capita",
]


def fetch_owid_csv() -> pd.DataFrame:
    """Download OWID energy CSV with retry logic."""
    for attempt in range(5):
        try:
            logger.info("Downloading OWID energy CSV (attempt %d)...", attempt + 1)
            resp = requests.get(OWID_URL, timeout=120)
            resp.raise_for_status()
            logger.info("Downloaded %.1f MB", len(resp.content) / 1e6)
            df = pd.read_csv(io.StringIO(resp.text), usecols=KEEP_COLUMNS)
            return df
        except requests.RequestException as e:
            wait = 15 * (attempt + 1)
            logger.warning("Download failed, retrying in %ds: %s", wait, e)
            time.sleep(wait)
        except ValueError as e:
            logger.warning("Column mismatch, downloading all columns: %s", e)
            resp = requests.get(OWID_URL, timeout=120)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text))
            available = [c for c in KEEP_COLUMNS if c in df.columns]
            logger.info("Using %d of %d requested columns", len(available), len(KEEP_COLUMNS))
            return df[available]

    raise RuntimeError("Failed to download OWID energy CSV after 5 attempts")


def materialize():
    logger.info("Starting OWID energy data ingestion")

    df = fetch_owid_csv()
    logger.info("Raw CSV: %d rows, %d columns", len(df), len(df.columns))

    df = df[df["year"].notna()].copy()
    df["year"] = df["year"].astype(int)
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Final: %d rows for %d countries/regions", len(df), df["country"].nunique())
    return df
