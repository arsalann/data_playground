/* @bruin
name: marts.hk_transit_mart_route_summary
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Route-level aggregated metrics combining trip counts, stop counts,
  departure volumes, and service patterns into a single summary table.
  Serves as the main route reference for dashboards.

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
  - name: agency_id
    type: VARCHAR
    description: Transit agency operating this route
  - name: route_short_name
    type: VARCHAR
    description: Short public-facing route name
  - name: route_long_name
    type: VARCHAR
    description: Full descriptive route name
  - name: route_type_name
    type: VARCHAR
    description: Transport mode
  - name: total_trips
    type: INTEGER
    description: Total number of trips on this route
  - name: total_departures
    type: INTEGER
    description: Total stop-level departures across all trips
  - name: distinct_stops
    type: INTEGER
    description: Number of distinct stops served
  - name: weekday_trips
    type: INTEGER
    description: Trips running on weekdays
  - name: weekend_trips
    type: INTEGER
    description: Trips running on weekends
  - name: avg_stops_per_trip
    type: DOUBLE
    description: Average number of stops per trip

@bruin */

WITH trip_stats AS (
    SELECT
        t.trip_id,
        t.route_id,
        t.is_weekday_service,
        t.is_weekend_service,
        COUNT(*) AS stops_in_trip
    FROM staging.hk_transit_trips t
    INNER JOIN staging.hk_transit_stop_times st
        ON t.trip_id = st.trip_id
    GROUP BY t.trip_id, t.route_id, t.is_weekday_service, t.is_weekend_service
)

SELECT
    r.route_id,
    r.agency_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type_name,
    COUNT(DISTINCT ts.trip_id) AS total_trips,
    SUM(ts.stops_in_trip) AS total_departures,
    COUNT(DISTINCT st.stop_id) AS distinct_stops,
    COUNTIF(ts.is_weekday_service) AS weekday_trips,
    COUNTIF(ts.is_weekend_service) AS weekend_trips,
    ROUND(AVG(ts.stops_in_trip), 1) AS avg_stops_per_trip
FROM trip_stats ts
INNER JOIN staging.hk_transit_routes r
    ON ts.route_id = r.route_id
INNER JOIN staging.hk_transit_stop_times st
    ON ts.trip_id = st.trip_id
GROUP BY r.route_id, r.agency_id, r.route_short_name, r.route_long_name, r.route_type_name
ORDER BY total_departures DESC
