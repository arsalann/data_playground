/* @bruin
name: staging.hk_transit_routes
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Cleans and type-casts GTFS routes data. Adds human-readable route type labels
  based on GTFS route_type codes (bus, tram, ferry, etc.).
  Filters out records with null route_id.

depends:
  - raw.hk_transit_gtfs_static

materialization:
  type: table
  strategy: create+replace

columns:
  - name: route_id
    type: VARCHAR
    description: Unique route identifier from GTFS
    primary_key: true
    nullable: false
  - name: agency_id
    type: VARCHAR
    description: Transit agency operating this route
  - name: route_short_name
    type: VARCHAR
    description: Short public-facing route name (e.g. route number)
  - name: route_long_name
    type: VARCHAR
    description: Full descriptive route name
  - name: route_type
    type: INTEGER
    description: GTFS route type code
  - name: route_type_name
    type: VARCHAR
    description: Human-readable transport mode (Bus, Tram, Ferry, etc.)
  - name: route_url
    type: VARCHAR
    description: URL for the route information page
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the data was extracted

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.hk_transit_routes
    WHERE route_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY extracted_at DESC) = 1
)

SELECT
    CAST(route_id AS STRING) AS route_id,
    CAST(agency_id AS STRING) AS agency_id,
    CAST(route_short_name AS STRING) AS route_short_name,
    CAST(route_long_name AS STRING) AS route_long_name,
    CAST(route_type AS INT64) AS route_type,
    CASE CAST(route_type AS INT64)
        WHEN 0 THEN 'Tram'
        WHEN 1 THEN 'Metro'
        WHEN 2 THEN 'Rail'
        WHEN 3 THEN 'Bus'
        WHEN 4 THEN 'Ferry'
        WHEN 5 THEN 'Cable Tram'
        WHEN 6 THEN 'Aerial Lift'
        WHEN 7 THEN 'Funicular'
        WHEN 11 THEN 'Trolleybus'
        WHEN 12 THEN 'Monorail'
        ELSE 'Other'
    END AS route_type_name,
    CAST(route_url AS STRING) AS route_url,
    extracted_at
FROM deduped
ORDER BY route_id
