/* @bruin
name: staging.hk_transit_stops
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Cleans and type-casts GTFS stops data. Filters out records with null stop_id.
  Casts latitude/longitude to FLOAT64 for spatial analysis.
  Adds location_type label for parent station vs stop classification.

depends:
  - raw.hk_transit_gtfs_static

materialization:
  type: table
  strategy: create+replace

columns:
  - name: stop_id
    type: VARCHAR
    description: Unique stop identifier from GTFS
    primary_key: true
    nullable: false
  - name: stop_name
    type: VARCHAR
    description: Name of the stop
  - name: stop_lat
    type: DOUBLE
    description: Latitude of the stop in WGS84 decimal degrees
  - name: stop_lon
    type: DOUBLE
    description: Longitude of the stop in WGS84 decimal degrees
  - name: zone_id
    type: VARCHAR
    description: Fare zone identifier
  - name: location_type
    type: INTEGER
    description: GTFS location type (0=stop, 1=station, 2=entrance)
  - name: location_type_name
    type: VARCHAR
    description: Human-readable location type label
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the data was extracted

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.hk_transit_stops
    WHERE stop_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY stop_id ORDER BY extracted_at DESC) = 1
)

SELECT
    CAST(stop_id AS STRING) AS stop_id,
    CAST(stop_name AS STRING) AS stop_name,
    CAST(stop_lat AS FLOAT64) AS stop_lat,
    CAST(stop_lon AS FLOAT64) AS stop_lon,
    CAST(zone_id AS STRING) AS zone_id,
    CAST(location_type AS INT64) AS location_type,
    CASE CAST(location_type AS INT64)
        WHEN 0 THEN 'Stop'
        WHEN 1 THEN 'Station'
        WHEN 2 THEN 'Entrance/Exit'
        WHEN 3 THEN 'Generic Node'
        WHEN 4 THEN 'Boarding Area'
        ELSE 'Unknown'
    END AS location_type_name,
    extracted_at
FROM deduped
ORDER BY stop_id
