/* @bruin
name: staging.flights_by_hub
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Transforms raw Flightradar24 flight summary data into an analysis-ready table
  for comparing the world's busiest airport hubs. Enriches each flight with:
  - Airport region classification (Middle East, North America, Europe, East Asia)
  - Aircraft manufacturer (Airbus, Boeing, Embraer, Other)
  - Aircraft size category (Widebody, Narrowbody, Regional)
  - Distance category (Short-haul, Medium-haul, Long-haul)
  - Temporal dimensions (date, hour, day of week)
  - Diversion flag and flight duration in minutes

depends:
  - raw.flight_summary_raw

materialization:
  type: table
  strategy: create+replace

columns:
  - name: fr24_id
    type: VARCHAR
    description: Unique Flightradar24 flight leg identifier
    primary_key: true
    nullable: false
  - name: flight_date
    type: DATE
    description: Date of takeoff in UTC
  - name: hour_utc
    type: INTEGER
    description: Hour of takeoff in UTC (0-23)
  - name: day_of_week
    type: VARCHAR
    description: Day of the week of takeoff (Monday-Sunday)
  - name: flight
    type: VARCHAR
    description: Commercial flight number
  - name: airline_icao
    type: VARCHAR
    description: ICAO code of the operating airline
  - name: airline_painted
    type: VARCHAR
    description: ICAO code of the marketing airline (livery)
  - name: aircraft_type
    type: VARCHAR
    description: ICAO aircraft type designator
  - name: manufacturer
    type: VARCHAR
    description: Aircraft manufacturer (Airbus, Boeing, Embraer, Other)
  - name: aircraft_size
    type: VARCHAR
    description: Aircraft size category (Widebody, Narrowbody, Regional)
  - name: registration
    type: VARCHAR
    description: Aircraft registration number
  - name: origin_iata
    type: VARCHAR
    description: IATA code for the origin airport
  - name: origin_region
    type: VARCHAR
    description: Geographic region of the origin airport
  - name: destination_iata
    type: VARCHAR
    description: IATA code for the intended destination airport
  - name: destination_region
    type: VARCHAR
    description: Geographic region of the destination airport
  - name: destination_actual_iata
    type: VARCHAR
    description: IATA code for the actual destination (differs if diverted)
  - name: is_diverted
    type: BOOLEAN
    description: Whether the flight landed at a different airport than planned
  - name: datetime_takeoff
    type: TIMESTAMP
    description: Takeoff datetime in UTC
  - name: datetime_landed
    type: TIMESTAMP
    description: Landing datetime in UTC
  - name: flight_duration_min
    type: DOUBLE
    description: Flight duration from takeoff to landing in minutes
  - name: actual_distance_km
    type: DOUBLE
    description: Actual ground distance traveled in kilometers
  - name: circle_distance_km
    type: DOUBLE
    description: Great-circle distance between origin and destination in kilometers
  - name: distance_category
    type: VARCHAR
    description: Distance classification (Short-haul <1500km, Medium-haul 1500-4000km, Long-haul >4000km)
  - name: flight_category
    type: VARCHAR
    description: Flight service type (Passenger, Cargo, Other)
  - name: query_airport
    type: VARCHAR
    description: IATA code of the hub airport this row was fetched for
  - name: query_airport_region
    type: VARCHAR
    description: Geographic region of the queried hub airport

@bruin */

WITH region_lookup AS (
  SELECT
    fr24_id,
    flight,
    operated_as AS airline_icao,
    painted_as AS airline_painted,
    aircraft_type,
    reg AS registration,
    orig_iata AS origin_iata,
    dest_iata AS destination_iata,
    dest_iata_actual AS destination_actual_iata,
    datetime_takeoff,
    datetime_landed,
    COALESCE(flight_time, 0) AS flight_time_sec,
    COALESCE(actual_distance, 0) AS actual_distance_km,
    COALESCE(circle_distance, 0) AS circle_distance_km,
    COALESCE(category, 'Unknown') AS flight_category,
    query_airport,

    CASE orig_iata
      WHEN 'DXB' THEN 'Middle East'
      WHEN 'DOH' THEN 'Middle East'
      WHEN 'AUH' THEN 'Middle East'
      WHEN 'JED' THEN 'Middle East'
      WHEN 'RUH' THEN 'Middle East'
      WHEN 'ATL' THEN 'North America'
      WHEN 'ORD' THEN 'North America'
      WHEN 'DFW' THEN 'North America'
      WHEN 'DEN' THEN 'North America'
      WHEN 'LAX' THEN 'North America'
      WHEN 'JFK' THEN 'North America'
      WHEN 'SFO' THEN 'North America'
      WHEN 'MIA' THEN 'North America'
      WHEN 'EWR' THEN 'North America'
      WHEN 'IAH' THEN 'North America'
      WHEN 'YYZ' THEN 'North America'
      WHEN 'LHR' THEN 'Europe'
      WHEN 'CDG' THEN 'Europe'
      WHEN 'FRA' THEN 'Europe'
      WHEN 'AMS' THEN 'Europe'
      WHEN 'IST' THEN 'Europe'
      WHEN 'MAD' THEN 'Europe'
      WHEN 'BCN' THEN 'Europe'
      WHEN 'FCO' THEN 'Europe'
      WHEN 'MUC' THEN 'Europe'
      WHEN 'LGW' THEN 'Europe'
      WHEN 'ZRH' THEN 'Europe'
      WHEN 'VIE' THEN 'Europe'
      WHEN 'PVG' THEN 'East Asia'
      WHEN 'CAN' THEN 'East Asia'
      WHEN 'HND' THEN 'East Asia'
      WHEN 'NRT' THEN 'East Asia'
      WHEN 'ICN' THEN 'East Asia'
      WHEN 'PEK' THEN 'East Asia'
      WHEN 'PKX' THEN 'East Asia'
      WHEN 'HKG' THEN 'East Asia'
      WHEN 'TPE' THEN 'East Asia'
      WHEN 'SIN' THEN 'Southeast Asia'
      WHEN 'BKK' THEN 'Southeast Asia'
      WHEN 'KUL' THEN 'Southeast Asia'
      WHEN 'CGK' THEN 'Southeast Asia'
      WHEN 'MNL' THEN 'Southeast Asia'
      WHEN 'DEL' THEN 'South Asia'
      WHEN 'BOM' THEN 'South Asia'
      WHEN 'BLR' THEN 'South Asia'
      WHEN 'SYD' THEN 'Oceania'
      WHEN 'MEL' THEN 'Oceania'
      WHEN 'AKL' THEN 'Oceania'
      WHEN 'GRU' THEN 'South America'
      WHEN 'BOG' THEN 'South America'
      WHEN 'SCL' THEN 'South America'
      WHEN 'JNB' THEN 'Africa'
      WHEN 'CAI' THEN 'Africa'
      WHEN 'ADD' THEN 'Africa'
      ELSE 'Other'
    END AS origin_region,

    CASE dest_iata
      WHEN 'DXB' THEN 'Middle East'
      WHEN 'DOH' THEN 'Middle East'
      WHEN 'AUH' THEN 'Middle East'
      WHEN 'JED' THEN 'Middle East'
      WHEN 'RUH' THEN 'Middle East'
      WHEN 'ATL' THEN 'North America'
      WHEN 'ORD' THEN 'North America'
      WHEN 'DFW' THEN 'North America'
      WHEN 'DEN' THEN 'North America'
      WHEN 'LAX' THEN 'North America'
      WHEN 'JFK' THEN 'North America'
      WHEN 'SFO' THEN 'North America'
      WHEN 'MIA' THEN 'North America'
      WHEN 'EWR' THEN 'North America'
      WHEN 'IAH' THEN 'North America'
      WHEN 'YYZ' THEN 'North America'
      WHEN 'LHR' THEN 'Europe'
      WHEN 'CDG' THEN 'Europe'
      WHEN 'FRA' THEN 'Europe'
      WHEN 'AMS' THEN 'Europe'
      WHEN 'IST' THEN 'Europe'
      WHEN 'MAD' THEN 'Europe'
      WHEN 'BCN' THEN 'Europe'
      WHEN 'FCO' THEN 'Europe'
      WHEN 'MUC' THEN 'Europe'
      WHEN 'LGW' THEN 'Europe'
      WHEN 'ZRH' THEN 'Europe'
      WHEN 'VIE' THEN 'Europe'
      WHEN 'PVG' THEN 'East Asia'
      WHEN 'CAN' THEN 'East Asia'
      WHEN 'HND' THEN 'East Asia'
      WHEN 'NRT' THEN 'East Asia'
      WHEN 'ICN' THEN 'East Asia'
      WHEN 'PEK' THEN 'East Asia'
      WHEN 'PKX' THEN 'East Asia'
      WHEN 'HKG' THEN 'East Asia'
      WHEN 'TPE' THEN 'East Asia'
      WHEN 'SIN' THEN 'Southeast Asia'
      WHEN 'BKK' THEN 'Southeast Asia'
      WHEN 'KUL' THEN 'Southeast Asia'
      WHEN 'CGK' THEN 'Southeast Asia'
      WHEN 'MNL' THEN 'Southeast Asia'
      WHEN 'DEL' THEN 'South Asia'
      WHEN 'BOM' THEN 'South Asia'
      WHEN 'BLR' THEN 'South Asia'
      WHEN 'SYD' THEN 'Oceania'
      WHEN 'MEL' THEN 'Oceania'
      WHEN 'AKL' THEN 'Oceania'
      WHEN 'GRU' THEN 'South America'
      WHEN 'BOG' THEN 'South America'
      WHEN 'SCL' THEN 'South America'
      WHEN 'JNB' THEN 'Africa'
      WHEN 'CAI' THEN 'Africa'
      WHEN 'ADD' THEN 'Africa'
      ELSE 'Other'
    END AS destination_region,

    CASE query_airport
      WHEN 'DXB' THEN 'Middle East'
      WHEN 'ATL' THEN 'North America'
      WHEN 'LHR' THEN 'Europe'
      WHEN 'PVG' THEN 'East Asia'
      WHEN 'CAN' THEN 'East Asia'
      WHEN 'HND' THEN 'East Asia'
      WHEN 'CDG' THEN 'Europe'
      WHEN 'ORD' THEN 'North America'
      ELSE 'Other'
    END AS query_airport_region,

    CASE
      WHEN aircraft_type LIKE 'A3%' THEN 'Airbus'
      WHEN aircraft_type LIKE 'A2%' THEN 'Airbus'
      WHEN aircraft_type IN ('A388', 'A389') THEN 'Airbus'
      WHEN aircraft_type LIKE 'B7%' THEN 'Boeing'
      WHEN aircraft_type LIKE 'B38%' THEN 'Boeing'
      WHEN aircraft_type LIKE 'B39%' THEN 'Boeing'
      WHEN aircraft_type IN ('B461', 'B462', 'B463') THEN 'BAe'
      WHEN aircraft_type LIKE 'E1%' THEN 'Embraer'
      WHEN aircraft_type LIKE 'E2%' THEN 'Embraer'
      WHEN aircraft_type LIKE 'E7%' THEN 'Embraer'
      WHEN aircraft_type LIKE 'CRJ%' THEN 'Bombardier'
      WHEN aircraft_type LIKE 'DH%' THEN 'De Havilland'
      WHEN aircraft_type LIKE 'AT%' THEN 'ATR'
      WHEN aircraft_type LIKE 'C%' AND aircraft_type IN ('C919', 'C929') THEN 'COMAC'
      ELSE 'Other'
    END AS manufacturer,

    CASE
      WHEN aircraft_type IN (
        'A332', 'A333', 'A338', 'A339',
        'A342', 'A343', 'A345', 'A346',
        'A359', 'A35K',
        'A388',
        'B762', 'B763', 'B764',
        'B772', 'B773', 'B77L', 'B77W', 'B778', 'B779',
        'B788', 'B789', 'B78X',
        'B744', 'B748'
      ) THEN 'Widebody'
      WHEN aircraft_type IN (
        'A318', 'A319', 'A320', 'A321', 'A19N', 'A20N', 'A21N',
        'B731', 'B732', 'B733', 'B734', 'B735', 'B736', 'B737', 'B738', 'B739',
        'B38M', 'B39M', 'B3XM',
        'B752', 'B753',
        'C919'
      ) THEN 'Narrowbody'
      WHEN aircraft_type IN (
        'E170', 'E175', 'E190', 'E195', 'E75L', 'E75S', 'E290', 'E295',
        'CRJ1', 'CRJ2', 'CRJ7', 'CRJ9', 'CRJX',
        'DH8A', 'DH8B', 'DH8C', 'DH8D',
        'AT43', 'AT45', 'AT72', 'AT76'
      ) THEN 'Regional'
      ELSE 'Other'
    END AS aircraft_size

  FROM raw.flight_summary_raw
  WHERE fr24_id IS NOT NULL
)

SELECT
  fr24_id,
  CAST(datetime_takeoff AS DATE) AS flight_date,
  EXTRACT(HOUR FROM datetime_takeoff) AS hour_utc,
  FORMAT_DATE('%A', CAST(datetime_takeoff AS DATE)) AS day_of_week,
  flight,
  airline_icao,
  airline_painted,
  aircraft_type,
  manufacturer,
  aircraft_size,
  registration,
  origin_iata,
  origin_region,
  destination_iata,
  destination_region,
  destination_actual_iata,
  CASE
    WHEN destination_actual_iata IS NOT NULL
      AND destination_actual_iata != destination_iata
    THEN TRUE
    ELSE FALSE
  END AS is_diverted,
  datetime_takeoff,
  datetime_landed,
  ROUND(flight_time_sec / 60.0, 1) AS flight_duration_min,
  actual_distance_km,
  circle_distance_km,
  CASE
    WHEN circle_distance_km < 1500 THEN 'Short-haul'
    WHEN circle_distance_km BETWEEN 1500 AND 4000 THEN 'Medium-haul'
    WHEN circle_distance_km > 4000 THEN 'Long-haul'
    ELSE 'Unknown'
  END AS distance_category,
  flight_category,
  query_airport,
  query_airport_region

FROM region_lookup
WHERE datetime_takeoff IS NOT NULL
ORDER BY query_airport, datetime_takeoff
