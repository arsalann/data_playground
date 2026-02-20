"""@bruin
name: raw.weather_raw
type: python
image: python:3.11
connection: duckdb-default
description: |
  Ingests Berlin daily weather data from the Open-Meteo Historical Weather API.
  Fetches daily observations including weather codes, temperature, precipitation,
  snowfall, wind speed, and sunshine duration for Berlin (lat 52.52, lon 13.41).

  Uses Bruin Python materialization with create+replace strategy since historical
  weather data is immutable and the full dataset is small (~3650 rows for 10 years).

  Data source: https://open-meteo.com/en/docs/historical-weather-api
  License: CC BY 4.0 (free for non-commercial use, no API key required)

materialization:
  type: table
  strategy: create+replace

columns:
  - name: time
    type: VARCHAR
    description: Date in ISO 8601 format (YYYY-MM-DD)
    primary_key: true
  - name: weather_code
    type: INTEGER
    description: WMO weather interpretation code indicating dominant weather condition
  - name: temperature_2m_max
    type: DOUBLE
    description: Maximum daily air temperature at 2m height in degrees Celsius
  - name: temperature_2m_min
    type: DOUBLE
    description: Minimum daily air temperature at 2m height in degrees Celsius
  - name: temperature_2m_mean
    type: DOUBLE
    description: Mean daily air temperature at 2m height in degrees Celsius
  - name: precipitation_sum
    type: DOUBLE
    description: Total daily precipitation (rain + snow) in millimeters
  - name: rain_sum
    type: DOUBLE
    description: Total daily rain in millimeters
  - name: snowfall_sum
    type: DOUBLE
    description: Total daily snowfall in centimeters
  - name: precipitation_hours
    type: DOUBLE
    description: Number of hours with precipitation during the day
  - name: wind_speed_10m_max
    type: DOUBLE
    description: Maximum daily wind speed at 10m height in km/h
  - name: sunshine_duration
    type: DOUBLE
    description: Total daily sunshine duration in seconds
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the data was extracted from the API

@bruin"""

import pandas as pd
import requests
import os
from datetime import datetime


BERLIN_LAT = 52.52
BERLIN_LON = 13.41
API_URL = "https://archive-api.open-meteo.com/v1/archive"

DAILY_VARIABLES = [
    "weather_code",
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "precipitation_hours",
    "wind_speed_10m_max",
    "sunshine_duration",
]


def fetch_weather_data(start_date: str, end_date: str) -> pd.DataFrame:
    params = {
        "latitude": BERLIN_LAT,
        "longitude": BERLIN_LON,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": "Europe/Berlin",
    }

    print(f"Fetching weather data: {start_date} to {end_date}")
    response = requests.get(API_URL, params=params, timeout=120)
    response.raise_for_status()

    data = response.json()

    if "daily" not in data:
        raise ValueError(f"Unexpected API response: missing 'daily' key. Response: {data}")

    df = pd.DataFrame(data["daily"])
    print(f"Fetched {len(df)} days of weather data")
    return df


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2009-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2026-12-31")

    # Clamp end_date to yesterday to avoid requesting future data
    yesterday = (datetime.now().date() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    if end_date > yesterday:
        end_date = yesterday
        print(f"Clamped end_date to {end_date} (yesterday)")

    df = fetch_weather_data(start_date, end_date)
    df["extracted_at"] = datetime.now()

    print(f"Total rows to materialize: {len(df)}")
    print(f"Date range: {df['time'].min()} to {df['time'].max()}")
    print(f"Columns: {list(df.columns)}")

    return df
