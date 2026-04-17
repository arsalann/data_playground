/* @bruin
name: staging.hk_transit_trips
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Cleans and type-casts GTFS trips data. Joins with calendar to attach
  weekday/weekend service patterns for each trip's service_id.
  Filters out records with null trip_id.

depends:
  - raw.hk_transit_gtfs_static

materialization:
  type: table
  strategy: create+replace

columns:
  - name: trip_id
    type: VARCHAR
    description: Unique trip identifier from GTFS
    primary_key: true
    nullable: false
  - name: route_id
    type: VARCHAR
    description: Route this trip belongs to
    nullable: false
  - name: service_id
    type: VARCHAR
    description: Service calendar pattern identifier
    nullable: false
  - name: runs_monday
    type: BOOLEAN
    description: Whether this trip runs on Mondays
  - name: runs_tuesday
    type: BOOLEAN
    description: Whether this trip runs on Tuesdays
  - name: runs_wednesday
    type: BOOLEAN
    description: Whether this trip runs on Wednesdays
  - name: runs_thursday
    type: BOOLEAN
    description: Whether this trip runs on Thursdays
  - name: runs_friday
    type: BOOLEAN
    description: Whether this trip runs on Fridays
  - name: runs_saturday
    type: BOOLEAN
    description: Whether this trip runs on Saturdays
  - name: runs_sunday
    type: BOOLEAN
    description: Whether this trip runs on Sundays
  - name: is_weekday_service
    type: BOOLEAN
    description: True if trip runs on all five weekdays
  - name: is_weekend_service
    type: BOOLEAN
    description: True if trip runs on Saturday or Sunday
  - name: service_start_date
    type: DATE
    description: First date this service pattern is active
  - name: service_end_date
    type: DATE
    description: Last date this service pattern is active
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the data was extracted

@bruin */

WITH trips_deduped AS (
    SELECT *
    FROM raw.hk_transit_trips
    WHERE trip_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY extracted_at DESC) = 1
),

calendar_deduped AS (
    SELECT *
    FROM raw.hk_transit_calendar
    WHERE service_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY service_id ORDER BY extracted_at DESC) = 1
)

SELECT
    CAST(t.trip_id AS STRING) AS trip_id,
    CAST(t.route_id AS STRING) AS route_id,
    CAST(t.service_id AS STRING) AS service_id,
    CAST(c.monday AS INT64) = 1 AS runs_monday,
    CAST(c.tuesday AS INT64) = 1 AS runs_tuesday,
    CAST(c.wednesday AS INT64) = 1 AS runs_wednesday,
    CAST(c.thursday AS INT64) = 1 AS runs_thursday,
    CAST(c.friday AS INT64) = 1 AS runs_friday,
    CAST(c.saturday AS INT64) = 1 AS runs_saturday,
    CAST(c.sunday AS INT64) = 1 AS runs_sunday,
    (CAST(c.monday AS INT64) = 1
        AND CAST(c.tuesday AS INT64) = 1
        AND CAST(c.wednesday AS INT64) = 1
        AND CAST(c.thursday AS INT64) = 1
        AND CAST(c.friday AS INT64) = 1
    ) AS is_weekday_service,
    (CAST(c.saturday AS INT64) = 1 OR CAST(c.sunday AS INT64) = 1) AS is_weekend_service,
    SAFE.PARSE_DATE('%Y%m%d', CAST(c.start_date AS STRING)) AS service_start_date,
    SAFE.PARSE_DATE('%Y%m%d', CAST(c.end_date AS STRING)) AS service_end_date,
    t.extracted_at
FROM trips_deduped t
LEFT JOIN calendar_deduped c
    ON CAST(t.service_id AS STRING) = CAST(c.service_id AS STRING)
ORDER BY trip_id
