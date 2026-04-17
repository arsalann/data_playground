/* @bruin
name: marts.hk_transit_mart_trip_trajectories
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Joins trip stop sequences with stop coordinates to reconstruct route paths.
  Answers: What do the geographic trajectories of trips look like?
  Each row is a stop visit within a trip, ordered by stop_sequence,
  with lat/lon for mapping.

depends:
  - staging.hk_transit_stop_times
  - staging.hk_transit_stops
  - staging.hk_transit_trips
  - staging.hk_transit_routes

materialization:
  type: table
  strategy: create+replace

columns:
  - name: trip_id
    type: VARCHAR
    description: Trip identifier
    primary_key: true
    nullable: false
  - name: stop_sequence
    type: INTEGER
    description: Order of this stop within the trip
    primary_key: true
    nullable: false
  - name: route_id
    type: VARCHAR
    description: Route this trip belongs to
  - name: route_short_name
    type: VARCHAR
    description: Short route name
  - name: route_type_name
    type: VARCHAR
    description: Transport mode
  - name: stop_id
    type: VARCHAR
    description: Stop identifier
  - name: stop_name
    type: VARCHAR
    description: Name of the stop
  - name: stop_lat
    type: DOUBLE
    description: Stop latitude in WGS84
  - name: stop_lon
    type: DOUBLE
    description: Stop longitude in WGS84
  - name: arrival_time
    type: VARCHAR
    description: Arrival time at this stop (HH:MM:SS)
  - name: departure_time
    type: VARCHAR
    description: Departure time from this stop (HH:MM:SS)

@bruin */

SELECT
    st.trip_id,
    st.stop_sequence,
    t.route_id,
    r.route_short_name,
    r.route_type_name,
    st.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    st.arrival_time,
    st.departure_time
FROM staging.hk_transit_stop_times st
INNER JOIN staging.hk_transit_stops s
    ON st.stop_id = s.stop_id
INNER JOIN staging.hk_transit_trips t
    ON st.trip_id = t.trip_id
INNER JOIN staging.hk_transit_routes r
    ON t.route_id = r.route_id
WHERE s.stop_lat IS NOT NULL
  AND s.stop_lon IS NOT NULL
ORDER BY st.trip_id, st.stop_sequence
