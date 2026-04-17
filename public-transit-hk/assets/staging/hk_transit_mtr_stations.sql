/* @bruin
name: staging.hk_transit_mtr_stations
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Combines MTR heavy rail stations and Light Rail stops into a unified
  station reference table. Normalizes column names to snake_case.
  Filters out records with null station identifiers.

  Sources: raw.hk_transit_mtr_lines_stations (heavy rail),
           raw.hk_transit_mtr_light_rail_stops (Light Rail)

depends:
  - raw.hk_transit_mtr_csv

materialization:
  type: table
  strategy: create+replace

columns:
  - name: station_id
    type: VARCHAR
    description: Unique station identifier
    primary_key: true
    nullable: false
  - name: station_name_en
    type: VARCHAR
    description: Station name in English
  - name: station_name_tc
    type: VARCHAR
    description: Station name in Traditional Chinese
  - name: line_code
    type: VARCHAR
    description: Line code the station belongs to
    primary_key: true
    nullable: false
  - name: line_name_en
    type: VARCHAR
    description: Line name in English
  - name: station_type
    type: VARCHAR
    description: Type of station (Heavy Rail or Light Rail)
  - name: latitude
    type: DOUBLE
    description: Latitude of the station in WGS84 decimal degrees (where available)
  - name: longitude
    type: DOUBLE
    description: Longitude of the station in WGS84 decimal degrees (where available)
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the data was extracted

@bruin */

WITH heavy_rail AS (
    SELECT
        CAST(`Station Code` AS STRING) AS station_id,
        CAST(`English Name` AS STRING) AS station_name_en,
        CAST(`Chinese Name` AS STRING) AS station_name_tc,
        CAST(`Line Code` AS STRING) AS line_code,
        CAST(`Line Code` AS STRING) AS line_name_en,
        'Heavy Rail' AS station_type,
        CAST(NULL AS FLOAT64) AS latitude,
        CAST(NULL AS FLOAT64) AS longitude,
        extracted_at
    FROM raw.hk_transit_mtr_lines_stations
    WHERE `Station Code` IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY `Station Code`, `Line Code`
        ORDER BY extracted_at DESC
    ) = 1
),

light_rail AS (
    SELECT
        CAST(`Stop Code` AS STRING) AS station_id,
        CAST(`English Name` AS STRING) AS station_name_en,
        CAST(`Chinese Name` AS STRING) AS station_name_tc,
        CAST(`Line Code` AS STRING) AS line_code,
        CAST(`Line Code` AS STRING) AS line_name_en,
        'Light Rail' AS station_type,
        CAST(NULL AS FLOAT64) AS latitude,
        CAST(NULL AS FLOAT64) AS longitude,
        extracted_at
    FROM raw.hk_transit_mtr_light_rail_stops
    WHERE `Stop Code` IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY `Stop Code`, `Line Code`
        ORDER BY extracted_at DESC
    ) = 1
)

SELECT * FROM heavy_rail
UNION ALL
SELECT * FROM light_rail
ORDER BY station_type, line_code, station_id
