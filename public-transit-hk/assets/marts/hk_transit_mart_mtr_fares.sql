/* @bruin
name: marts.hk_transit_mart_mtr_fares
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Station-to-station fare lookup table for MTR heavy rail.
  Casts fare columns to numeric types for analysis.
  Adds station names from the MTR stations reference for readability.

depends:
  - raw.hk_transit_mtr_csv
  - staging.hk_transit_mtr_stations

materialization:
  type: table
  strategy: create+replace

columns:
  - name: from_station_id
    type: VARCHAR
    description: Origin station code
    primary_key: true
    nullable: false
  - name: to_station_id
    type: VARCHAR
    description: Destination station code
    primary_key: true
    nullable: false
  - name: from_station_name
    type: VARCHAR
    description: Origin station name in English
  - name: to_station_name
    type: VARCHAR
    description: Destination station name in English
  - name: adult_octopus_fare
    type: DOUBLE
    description: Adult Octopus card fare in HKD
  - name: adult_single_journey_fare
    type: DOUBLE
    description: Adult single journey ticket fare in HKD
  - name: child_octopus_fare
    type: DOUBLE
    description: Child Octopus card fare in HKD
  - name: elderly_octopus_fare
    type: DOUBLE
    description: Elderly/disabled Octopus card fare in HKD

@bruin */

WITH fares_cleaned AS (
    SELECT
        CAST(SRC_STATION_ID AS STRING) AS from_station_id,
        CAST(DEST_STATION_ID AS STRING) AS to_station_id,
        SAFE_CAST(OCT_ADT_FARE AS FLOAT64) AS adult_octopus_fare,
        SAFE_CAST(SINGLE_ADT_FARE AS FLOAT64) AS adult_single_journey_fare,
        SAFE_CAST(OCT_CON_CHILD_FARE AS FLOAT64) AS child_octopus_fare,
        SAFE_CAST(OCT_CON_ELDERLY_FARE AS FLOAT64) AS elderly_octopus_fare
    FROM raw.hk_transit_mtr_fares
    WHERE SRC_STATION_ID IS NOT NULL
      AND DEST_STATION_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SRC_STATION_ID, DEST_STATION_ID
        ORDER BY extracted_at DESC
    ) = 1
)

SELECT
    f.from_station_id,
    f.to_station_id,
    src.station_name_en AS from_station_name,
    dst.station_name_en AS to_station_name,
    f.adult_octopus_fare,
    f.adult_single_journey_fare,
    f.child_octopus_fare,
    f.elderly_octopus_fare
FROM fares_cleaned f
LEFT JOIN staging.hk_transit_mtr_stations src
    ON f.from_station_id = src.station_id
LEFT JOIN staging.hk_transit_mtr_stations dst
    ON f.to_station_id = dst.station_id
ORDER BY f.from_station_id, f.to_station_id
