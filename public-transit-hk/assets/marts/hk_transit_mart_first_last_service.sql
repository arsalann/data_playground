/* @bruin
name: marts.hk_transit_mart_first_last_service
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Finds the earliest and latest departure time for each route.
  Answers: When does the first and last service run on each route?
  Uses departure_time strings for comparison (GTFS times sort lexically
  for same-day services; post-midnight times like 25:00 naturally sort after 23:59).

depends:
  - staging.hk_transit_stop_times
  - staging.hk_transit_trips
  - staging.hk_transit_routes

materialization:
  type: table
  strategy: create+replace

columns:
  - name: route_id
    type: VARCHAR
    description: Route identifier
    primary_key: true
    nullable: false
  - name: route_short_name
    type: VARCHAR
    description: Short public-facing route name
  - name: route_type_name
    type: VARCHAR
    description: Transport mode
  - name: first_departure
    type: VARCHAR
    description: Earliest departure time on this route (HH:MM:SS)
  - name: last_departure
    type: VARCHAR
    description: Latest departure time on this route (HH:MM:SS)
  - name: service_span_minutes
    type: INTEGER
    description: Minutes between first and last departure
  - name: total_departures
    type: INTEGER
    description: Total number of departures on this route

@bruin */

WITH route_times AS (
    SELECT
        t.route_id,
        st.departure_time,
        -- Parse time to minutes for span calculation
        SAFE_CAST(SPLIT(st.departure_time, ':')[OFFSET(0)] AS INT64) * 60
            + SAFE_CAST(SPLIT(st.departure_time, ':')[OFFSET(1)] AS INT64) AS departure_minutes
    FROM staging.hk_transit_stop_times st
    INNER JOIN staging.hk_transit_trips t
        ON st.trip_id = t.trip_id
    WHERE st.departure_time IS NOT NULL
)

SELECT
    r.route_id,
    r.route_short_name,
    r.route_type_name,
    MIN(rt.departure_time) AS first_departure,
    MAX(rt.departure_time) AS last_departure,
    MAX(rt.departure_minutes) - MIN(rt.departure_minutes) AS service_span_minutes,
    COUNT(*) AS total_departures
FROM route_times rt
INNER JOIN staging.hk_transit_routes r
    ON rt.route_id = r.route_id
GROUP BY r.route_id, r.route_short_name, r.route_type_name
ORDER BY r.route_type_name, r.route_short_name
