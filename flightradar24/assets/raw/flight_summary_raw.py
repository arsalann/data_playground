"""@bruin
name: raw.flight_summary_raw
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Ingests flight summary data from the Flightradar24 API (Flight Summary Full endpoint).
  Queries recent flights at the world's busiest airport hubs to compare traffic patterns,
  fleet composition, and route reach across global mega-hubs in 2026.

  Uses the Explorer plan with a response limit of 20 per request and 3 credits per
  historic result (<30 days). Initial configuration fetches only ~10 rows per airport
  to validate the pipeline before scaling up.

  Data source: https://fr24api.flightradar24.com/api/flight-summary/full
  License: Flightradar24 API Terms & Conditions (paid subscription required)

secrets:
    - key: FR24_API_TOKEN

materialization:
  type: table
  strategy: create+replace

columns:
  - name: fr24_id
    type: VARCHAR
    description: Unique Flightradar24 identifier for each flight leg
    primary_key: true
  - name: flight
    type: VARCHAR
    description: Commercial flight number interpreted from callsign
  - name: callsign
    type: VARCHAR
    description: Up to 8-character transponder callsign
  - name: operated_as
    type: VARCHAR
    description: ICAO code of the airline operating the flight
  - name: painted_as
    type: VARCHAR
    description: ICAO code of the airline marketing the flight (livery)
  - name: aircraft_type
    type: VARCHAR
    description: ICAO aircraft type designator (e.g. A320, B738)
  - name: reg
    type: VARCHAR
    description: Aircraft registration number
  - name: orig_iata
    type: VARCHAR
    description: IATA code for the origin airport
  - name: orig_icao
    type: VARCHAR
    description: ICAO code for the origin airport
  - name: datetime_takeoff
    type: TIMESTAMP
    description: Takeoff datetime in UTC
  - name: dest_iata
    type: VARCHAR
    description: IATA code for the intended destination airport
  - name: dest_icao
    type: VARCHAR
    description: ICAO code for the intended destination airport
  - name: dest_iata_actual
    type: VARCHAR
    description: IATA code for the actual destination (differs if diverted)
  - name: dest_icao_actual
    type: VARCHAR
    description: ICAO code for the actual destination (differs if diverted)
  - name: datetime_landed
    type: TIMESTAMP
    description: Landing datetime in UTC
  - name: flight_time
    type: INTEGER
    description: Flight duration from takeoff to landing in seconds
  - name: actual_distance
    type: DOUBLE
    description: Actual ground distance traveled in kilometers
  - name: circle_distance
    type: DOUBLE
    description: Great-circle distance between origin and destination in kilometers
  - name: category
    type: VARCHAR
    description: Flight service type (e.g. Passenger, Cargo)
  - name: hex
    type: VARCHAR
    description: 24-bit Mode-S transponder identifier in hexadecimal
  - name: runway_takeoff
    type: VARCHAR
    description: Runway identifier used for takeoff
  - name: runway_landed
    type: VARCHAR
    description: Runway identifier used for landing
  - name: first_seen
    type: TIMESTAMP
    description: Datetime when the aircraft was first detected for this flight leg (UTC)
  - name: last_seen
    type: TIMESTAMP
    description: Datetime when the aircraft was last detected for this flight leg (UTC)
  - name: query_airport
    type: VARCHAR
    description: IATA code of the hub airport this row was fetched for
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the data was extracted from the API

@bruin"""

import json
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

API_BASE = "https://fr24api.flightradar24.com/api/flight-summary/full"

TARGET_AIRPORTS = ["DXB", "ATL", "LHR", "PVG", "CAN", "HND", "CDG", "ORD"]

LIMIT_PER_AIRPORT = 10


def fetch_flights_for_airport(airport: str, date_from: str, date_to: str, token: str) -> list[dict]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Accept-Version": "v1",
    }
    params = {
        "flight_datetime_from": f"{date_from} 00:00:00",
        "flight_datetime_to": f"{date_to} 23:59:59",
        "airports": f"both:{airport}",
        "limit": LIMIT_PER_AIRPORT,
    }

    print(f"Fetching flights for {airport} ({date_from} to {date_to}), limit={LIMIT_PER_AIRPORT}")
    response = requests.get(API_BASE, params=params, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()
    flights = data.get("data", [])
    print(f"  -> Got {len(flights)} flights for {airport}")

    for flight in flights:
        flight["query_airport"] = airport

    return flights


def materialize():
    token_raw = os.environ.get("FR24_API_TOKEN", "")
    try:
        token_data = json.loads(token_raw)
        token = token_data.get("value", token_raw)
    except (json.JSONDecodeError, TypeError):
        token = token_raw

    if not token:
        raise ValueError("FR24_API_TOKEN not set. Add a generic connection in .bruin.yml.")

    start_date = os.environ.get("BRUIN_START_DATE", "")
    end_date = os.environ.get("BRUIN_END_DATE", "")

    if not start_date or not end_date:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = start_date or yesterday
        end_date = end_date or yesterday

    all_flights = []
    for airport in TARGET_AIRPORTS:
        flights = fetch_flights_for_airport(airport, start_date, end_date, token)
        all_flights.extend(flights)
        time.sleep(0.2)

    if not all_flights:
        print("No flights returned from API. Returning empty DataFrame.")
        return pd.DataFrame(columns=[
            "fr24_id", "flight", "callsign", "operated_as", "painted_as",
            "aircraft_type", "reg", "orig_iata", "orig_icao",
            "datetime_takeoff", "dest_iata", "dest_icao",
            "dest_iata_actual", "dest_icao_actual", "datetime_landed",
            "flight_time", "actual_distance", "circle_distance", "category",
            "hex", "runway_takeoff", "runway_landed", "first_seen", "last_seen",
            "query_airport", "extracted_at",
        ])

    df = pd.DataFrame(all_flights)

    column_mapping = {
        "type": "aircraft_type",
        "origin_icao": "orig_icao",
        "destination_icao": "dest_icao",
        "destination_icao_actual": "dest_icao_actual",
        "dest_iata_actual": "dest_iata_actual",
    }
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns and new_name not in df.columns:
            df.rename(columns={old_name: new_name}, inplace=True)

    keep_columns = [
        "fr24_id", "flight", "callsign", "operated_as", "painted_as",
        "aircraft_type", "reg", "orig_iata", "orig_icao",
        "datetime_takeoff", "dest_iata", "dest_icao",
        "dest_iata_actual", "dest_icao_actual", "datetime_landed",
        "flight_time", "actual_distance", "circle_distance", "category",
        "hex", "runway_takeoff", "runway_landed", "first_seen", "last_seen",
        "query_airport",
    ]
    for col in keep_columns:
        if col not in df.columns:
            df[col] = None

    df = df[keep_columns].copy()

    df = df.drop_duplicates(subset=["fr24_id"])

    df["extracted_at"] = datetime.now()

    print(f"Total rows to materialize: {len(df)}")
    print(f"Airports queried: {df['query_airport'].nunique()}")
    print(f"Columns: {list(df.columns)}")

    return df
