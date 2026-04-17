/* @bruin
name: marts.hk_transit_mart_longest_routes
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Ranks routes by the number of distinct stops they serve.
  Answers: Which are the longest routes by number of stops?
  Uses a representative trip per route (the one with the most stops).

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
  - name: route_long_name
    type: VARCHAR
    description: Full descriptive route name
  - name: route_type_name
    type: VARCHAR
    description: Transport mode
  - name: total_stops
    type: INTEGER
    description: Number of distinct stops on this route across all trips
  - name: max_stops_per_trip
    type: INTEGER
    description: Maximum stops in a single trip on this route
  - name: total_trips
    type: INTEGER
    description: Total number of trips on this route
  - name: length_rank
    type: INTEGER
    description: Rank by total distinct stops (1 = longest)

@bruin */

WITH route_stats AS (
    SELECT
        t.route_id,
        COUNT(DISTINCT st.stop_id) AS total_stops,
        MAX(trip_stop_count) AS max_stops_per_trip,
        COUNT(DISTINCT t.trip_id) AS total_trips
    FROM staging.hk_transit_trips t
    INNER JOIN staging.hk_transit_stop_times st
        ON t.trip_id = st.trip_id
    INNER JOIN (
        SELECT trip_id, COUNT(*) AS trip_stop_count
        FROM staging.hk_transit_stop_times
        GROUP BY trip_id
    ) tc ON t.trip_id = tc.trip_id
    GROUP BY t.route_id
)

SELECT
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type_name,
    rs.total_stops,
    rs.max_stops_per_trip,
    rs.total_trips,
    RANK() OVER (ORDER BY rs.total_stops DESC) AS length_rank
FROM route_stats rs
INNER JOIN staging.hk_transit_routes r
    ON rs.route_id = r.route_id
ORDER BY rs.total_stops DESC
