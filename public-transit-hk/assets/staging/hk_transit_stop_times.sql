/* @bruin
name: staging.hk_transit_stop_times
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Cleans and type-casts GTFS stop_times data. Parses arrival and departure time
  strings into hours/minutes for aggregation. Extracts the departure hour for
  peak analysis. Filters out records with null trip_id or stop_id.

  Note: GTFS times can exceed 24:00:00 for trips running past midnight.
  These are preserved as-is in the time strings but the departure_hour
  is computed modulo 24 for hourly analysis.

depends:
  - raw.hk_transit_gtfs_static

materialization:
  type: table
  strategy: create+replace

columns:
  - name: trip_id
    type: VARCHAR
    description: Trip this stop time belongs to
    primary_key: true
    nullable: false
  - name: stop_id
    type: VARCHAR
    description: Stop where this arrival/departure occurs
    primary_key: true
    nullable: false
  - name: stop_sequence
    type: INTEGER
    description: Order of this stop within the trip (1-based)
    primary_key: true
    nullable: false
  - name: arrival_time
    type: VARCHAR
    description: Arrival time as HH:MM:SS (may exceed 24:00:00 for post-midnight trips)
  - name: departure_time
    type: VARCHAR
    description: Departure time as HH:MM:SS (may exceed 24:00:00 for post-midnight trips)
  - name: departure_hour
    type: INTEGER
    description: Hour of departure (0-23, modulo 24 for post-midnight trips)
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the data was extracted

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.hk_transit_stop_times
    WHERE trip_id IS NOT NULL
      AND stop_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY trip_id, stop_id, stop_sequence
        ORDER BY extracted_at DESC
    ) = 1
)

SELECT
    CAST(trip_id AS STRING) AS trip_id,
    CAST(stop_id AS STRING) AS stop_id,
    CAST(stop_sequence AS INT64) AS stop_sequence,
    CAST(arrival_time AS STRING) AS arrival_time,
    CAST(departure_time AS STRING) AS departure_time,
    MOD(
        SAFE_CAST(SPLIT(CAST(departure_time AS STRING), ':')[OFFSET(0)] AS INT64),
        24
    ) AS departure_hour,
    extracted_at
FROM deduped
ORDER BY trip_id, stop_sequence
