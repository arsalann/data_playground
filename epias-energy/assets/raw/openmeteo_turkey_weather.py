"""@bruin
name: epias_raw.openmeteo_turkey_weather
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches daily weather data from the Open-Meteo Historical Weather API for
  five Turkey locations covering major hydroelectric basins. Weather variables
  include precipitation and temperature, which drive hydro generation capacity.

  Stations:
    - Artvin (41.18N, 41.82E) — Black Sea / Coruh River dams
    - Elazig (38.67N, 39.22E) — Keban/Karakaya dams on Euphrates
    - Diyarbakir (37.91N, 40.24E) — Tigris basin
    - Antalya (36.89N, 30.71E) — Mediterranean / southern hydro
    - Ankara (39.93N, 32.86E) — Central Anatolia reference

  Data source: https://archive-api.open-meteo.com/v1/archive
  No authentication required (Open-Meteo is a free API).

materialization:
  type: table
  strategy: create+replace

columns:
  - name: date
    type: DATE
    description: Date of the weather observation
    primary_key: true
  - name: station_name
    type: VARCHAR
    description: Name of the weather station / city
    primary_key: true
  - name: latitude
    type: DOUBLE
    description: Latitude of the station (decimal degrees)
  - name: longitude
    type: DOUBLE
    description: Longitude of the station (decimal degrees)
  - name: precipitation_mm
    type: DOUBLE
    description: Total daily precipitation in millimeters
  - name: temp_mean_c
    type: DOUBLE
    description: Mean daily temperature at 2m in Celsius
  - name: temp_max_c
    type: DOUBLE
    description: Maximum daily temperature at 2m in Celsius
  - name: temp_min_c
    type: DOUBLE
    description: Minimum daily temperature at 2m in Celsius
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was fetched from the API

@bruin"""

import logging
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

STATIONS = [
    {"name": "Artvin", "latitude": 41.18, "longitude": 41.82},
    {"name": "Elazig", "latitude": 38.67, "longitude": 39.22},
    {"name": "Diyarbakir", "latitude": 37.91, "longitude": 40.24},
    {"name": "Antalya", "latitude": 36.89, "longitude": 30.71},
    {"name": "Ankara", "latitude": 39.93, "longitude": 32.86},
]

DAILY_PARAMS = "precipitation_sum,temperature_2m_mean,temperature_2m_max,temperature_2m_min"


def fetch_weather(station, start_date, end_date):
    """Fetch daily weather for one station, chunked in 365-day windows."""
    all_rows = []
    current = start_date

    while current < end_date:
        chunk_end = min(current + timedelta(days=365), end_date)
        params = {
            "latitude": station["latitude"],
            "longitude": station["longitude"],
            "start_date": current.strftime("%Y-%m-%d"),
            "end_date": (chunk_end - timedelta(days=1)).strftime("%Y-%m-%d"),
            "daily": DAILY_PARAMS,
            "timezone": "Europe/Istanbul",
        }

        logger.info(
            "Fetching %s weather %s to %s",
            station["name"],
            params["start_date"],
            params["end_date"],
        )

        data = api_get(params)
        daily = data.get("daily", {})
        dates = daily.get("time", [])
        precipitation = daily.get("precipitation_sum", [])
        temp_mean = daily.get("temperature_2m_mean", [])
        temp_max = daily.get("temperature_2m_max", [])
        temp_min = daily.get("temperature_2m_min", [])

        for i, date_str in enumerate(dates):
            row = {
                "date": date_str,
                "station_name": station["name"],
                "latitude": station["latitude"],
                "longitude": station["longitude"],
                "precipitation_mm": precipitation[i] if i < len(precipitation) else None,
                "temp_mean_c": temp_mean[i] if i < len(temp_mean) else None,
                "temp_max_c": temp_max[i] if i < len(temp_max) else None,
                "temp_min_c": temp_min[i] if i < len(temp_min) else None,
            }
            all_rows.append(row)

        logger.info("  Got %d daily records", len(dates))
        current = chunk_end
        time.sleep(0.5)

    return all_rows


def api_get(params):
    """GET request to Open-Meteo with retry and exponential backoff for transient errors."""
    for attempt in range(5):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=60)
            if resp.status_code in (429, 502, 503):
                wait = 15 * (attempt + 1)
                logger.warning("HTTP %d, retrying in %ds...", resp.status_code, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            wait = 10 * (attempt + 1)
            logger.warning("Timeout on attempt %d, retrying in %ds...", attempt + 1, wait)
            time.sleep(wait)
        except requests.exceptions.ConnectionError:
            wait = 10 * (attempt + 1)
            logger.warning("Connection error on attempt %d, retrying in %ds...", attempt + 1, wait)
            time.sleep(wait)
    raise RuntimeError(f"Failed after 5 retries: {BASE_URL}")


def materialize():
    start_str = os.environ.get("BRUIN_START_DATE", "2015-01-01")
    end_str = os.environ.get("BRUIN_END_DATE", "2026-04-16")
    logger.info("Interval: %s to %s", start_str, end_str)

    start_date = datetime.strptime(start_str[:10], "%Y-%m-%d")
    end_date = datetime.strptime(end_str[:10], "%Y-%m-%d") + timedelta(days=1)

    all_rows = []
    for station in STATIONS:
        rows = fetch_weather(station, start_date, end_date)
        all_rows.extend(rows)
        logger.info("Station %s: %d rows fetched", station["name"], len(rows))

    if not all_rows:
        raise RuntimeError("No data fetched from Open-Meteo API")

    df = pd.DataFrame(all_rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["extracted_at"] = datetime.now()

    logger.info("Total records: %d", len(df))
    logger.info(
        "Date range: %s to %s",
        df["date"].min(),
        df["date"].max(),
    )
    return df
