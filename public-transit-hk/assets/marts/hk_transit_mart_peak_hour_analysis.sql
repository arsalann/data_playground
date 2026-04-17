/* @bruin
name: marts.hk_transit_mart_peak_hour_analysis
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Departures grouped by hour of day across the entire transit network.
  Answers: What are the peak hours for departures across the network?
  Also breaks down by transport mode (bus, tram, ferry) for mode-specific
  peak identification.

depends:
  - staging.hk_transit_stop_times
  - staging.hk_transit_trips
  - staging.hk_transit_routes

materialization:
  type: table
  strategy: create+replace

columns:
  - name: departure_hour
    type: INTEGER
    description: Hour of day (0-23)
    primary_key: true
    nullable: false
  - name: route_type_name
    type: VARCHAR
    description: Transport mode (Bus, Tram, Ferry, etc.)
    primary_key: true
    nullable: false
  - name: departure_count
    type: INTEGER
    description: Total number of departures in this hour for this mode
  - name: distinct_routes
    type: INTEGER
    description: Number of distinct routes operating in this hour
  - name: distinct_stops
    type: INTEGER
    description: Number of distinct stops served in this hour

@bruin */

SELECT
    st.departure_hour,
    r.route_type_name,
    COUNT(*) AS departure_count,
    COUNT(DISTINCT t.route_id) AS distinct_routes,
    COUNT(DISTINCT st.stop_id) AS distinct_stops
FROM staging.hk_transit_stop_times st
INNER JOIN staging.hk_transit_trips t
    ON st.trip_id = t.trip_id
INNER JOIN staging.hk_transit_routes r
    ON t.route_id = r.route_id
WHERE st.departure_hour IS NOT NULL
GROUP BY st.departure_hour, r.route_type_name
ORDER BY st.departure_hour, r.route_type_name
