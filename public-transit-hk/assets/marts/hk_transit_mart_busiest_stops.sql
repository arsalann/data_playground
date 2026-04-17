/* @bruin
name: marts.hk_transit_mart_busiest_stops
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Ranks stops by total departure count across all routes and trips.
  Answers: Which stops and routes carry the most traffic (by departure count)?
  Includes stop coordinates and route type breakdown.

depends:
  - staging.hk_transit_stop_times
  - staging.hk_transit_stops
  - staging.hk_transit_trips
  - staging.hk_transit_routes

materialization:
  type: table
  strategy: create+replace

columns:
  - name: stop_id
    type: VARCHAR
    description: Stop identifier
    primary_key: true
    nullable: false
  - name: stop_name
    type: VARCHAR
    description: Name of the stop
  - name: stop_lat
    type: DOUBLE
    description: Latitude of the stop in WGS84
  - name: stop_lon
    type: DOUBLE
    description: Longitude of the stop in WGS84
  - name: total_departures
    type: INTEGER
    description: Total number of departures from this stop
  - name: distinct_routes
    type: INTEGER
    description: Number of distinct routes serving this stop
  - name: bus_departures
    type: INTEGER
    description: Departures from bus routes only
  - name: tram_departures
    type: INTEGER
    description: Departures from tram routes only
  - name: ferry_departures
    type: INTEGER
    description: Departures from ferry routes only
  - name: busy_rank
    type: INTEGER
    description: Rank by total departures (1 = busiest)

@bruin */

WITH stop_stats AS (
    SELECT
        st.stop_id,
        COUNT(*) AS total_departures,
        COUNT(DISTINCT t.route_id) AS distinct_routes,
        COUNTIF(r.route_type_name = 'Bus') AS bus_departures,
        COUNTIF(r.route_type_name = 'Tram') AS tram_departures,
        COUNTIF(r.route_type_name = 'Ferry') AS ferry_departures
    FROM staging.hk_transit_stop_times st
    INNER JOIN staging.hk_transit_trips t
        ON st.trip_id = t.trip_id
    INNER JOIN staging.hk_transit_routes r
        ON t.route_id = r.route_id
    GROUP BY st.stop_id
)

SELECT
    ss.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    ss.total_departures,
    ss.distinct_routes,
    ss.bus_departures,
    ss.tram_departures,
    ss.ferry_departures,
    RANK() OVER (ORDER BY ss.total_departures DESC) AS busy_rank
FROM stop_stats ss
INNER JOIN staging.hk_transit_stops s
    ON ss.stop_id = s.stop_id
ORDER BY ss.total_departures DESC
