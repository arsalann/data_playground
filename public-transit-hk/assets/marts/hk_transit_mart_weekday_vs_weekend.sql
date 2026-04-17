/* @bruin
name: marts.hk_transit_mart_weekday_vs_weekend
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Compares service volume between weekdays and weekends by route and mode.
  Answers: How does service volume differ between weekdays and weekends?
  Uses calendar service patterns to classify trips.

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
    description: Transport mode (Bus, Tram, Ferry, etc.)
  - name: weekday_trips
    type: INTEGER
    description: Number of trips running on weekdays
  - name: weekend_trips
    type: INTEGER
    description: Number of trips running on weekends
  - name: weekday_departures
    type: INTEGER
    description: Total stop-level departures on weekday trips
  - name: weekend_departures
    type: INTEGER
    description: Total stop-level departures on weekend trips
  - name: weekend_weekday_ratio
    type: DOUBLE
    description: Ratio of weekend to weekday departures (1.0 = equal service)

@bruin */

WITH trip_departures AS (
    SELECT
        t.trip_id,
        t.route_id,
        t.is_weekday_service,
        t.is_weekend_service,
        COUNT(*) AS stop_count
    FROM staging.hk_transit_trips t
    INNER JOIN staging.hk_transit_stop_times st
        ON t.trip_id = st.trip_id
    GROUP BY t.trip_id, t.route_id, t.is_weekday_service, t.is_weekend_service
)

SELECT
    r.route_id,
    r.route_short_name,
    r.route_type_name,
    COUNTIF(td.is_weekday_service) AS weekday_trips,
    COUNTIF(td.is_weekend_service) AS weekend_trips,
    SUM(CASE WHEN td.is_weekday_service THEN td.stop_count ELSE 0 END) AS weekday_departures,
    SUM(CASE WHEN td.is_weekend_service THEN td.stop_count ELSE 0 END) AS weekend_departures,
    SAFE_DIVIDE(
        SUM(CASE WHEN td.is_weekend_service THEN td.stop_count ELSE 0 END),
        SUM(CASE WHEN td.is_weekday_service THEN td.stop_count ELSE 0 END)
    ) AS weekend_weekday_ratio
FROM trip_departures td
INNER JOIN staging.hk_transit_routes r
    ON td.route_id = r.route_id
GROUP BY r.route_id, r.route_short_name, r.route_type_name
ORDER BY weekday_departures DESC
