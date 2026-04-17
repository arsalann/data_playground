/* @bruin
name: marts.hk_transit_mart_transfer_hubs
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Identifies top transfer hubs — stops served by the most distinct routes.
  Answers: Which stops are the top transfer hubs (most distinct routes served)?
  Includes stop coordinates for geographic mapping.

depends:
  - staging.hk_transit_stop_times
  - staging.hk_transit_stops
  - staging.hk_transit_trips

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
  - name: distinct_route_count
    type: INTEGER
    description: Number of distinct routes serving this stop
  - name: total_departures
    type: INTEGER
    description: Total number of departures from this stop
  - name: hub_rank
    type: INTEGER
    description: Rank by number of distinct routes (1 = most connected)

@bruin */

WITH stop_routes AS (
    SELECT
        st.stop_id,
        COUNT(DISTINCT t.route_id) AS distinct_route_count,
        COUNT(*) AS total_departures
    FROM staging.hk_transit_stop_times st
    INNER JOIN staging.hk_transit_trips t
        ON st.trip_id = t.trip_id
    GROUP BY st.stop_id
)

SELECT
    sr.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    sr.distinct_route_count,
    sr.total_departures,
    RANK() OVER (ORDER BY sr.distinct_route_count DESC) AS hub_rank
FROM stop_routes sr
INNER JOIN staging.hk_transit_stops s
    ON sr.stop_id = s.stop_id
ORDER BY sr.distinct_route_count DESC
