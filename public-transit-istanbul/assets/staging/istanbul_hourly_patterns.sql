/* @bruin
name: staging.istanbul_hourly_patterns
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Hourly ridership patterns aggregated by hour-of-day and day-of-week.
  Creates a matrix showing average ridership for each hour/day combination by transport mode.
  Used for the daily rhythm heatmap visualization.

depends:
  - raw.istanbul_hourly_transport

materialization:
  type: table
  strategy: create+replace

columns:
  - name: road_type
    type: VARCHAR
    description: Transport system type (RAYLI, DENIZYOLU, KARAYOLU, METROBUS, etc.)
    primary_key: true
  - name: transition_hour
    type: INTEGER
    description: Hour of day (0-23)
    primary_key: true
  - name: day_of_week
    type: INTEGER
    description: Day of week (1=Sunday, 7=Saturday in BigQuery)
    primary_key: true
  - name: day_name
    type: VARCHAR
    description: Day of week name (Monday, Tuesday, etc.)
  - name: avg_passages
    type: DOUBLE
    description: Average daily Istanbulkart tap-ins for this hour/day combination
  - name: avg_passengers
    type: DOUBLE
    description: Average daily unique passengers for this hour/day combination
  - name: total_passages
    type: INTEGER
    description: Total Istanbulkart tap-ins across all dates
  - name: num_days
    type: INTEGER
    description: Number of distinct dates in the average

@bruin */

WITH hourly_totals AS (
    SELECT
        road_type,
        transition_hour,
        transition_date,
        EXTRACT(DAYOFWEEK FROM transition_date) AS day_of_week,
        SUM(COALESCE(number_of_passage, 0)) AS daily_passages,
        SUM(COALESCE(number_of_passenger, 0)) AS daily_passengers
    FROM raw.istanbul_hourly_transport
    WHERE transition_date IS NOT NULL
    GROUP BY road_type, transition_hour, transition_date
)

SELECT
    road_type,
    transition_hour,
    day_of_week,
    FORMAT_DATE('%A', DATE(2024, 1, day_of_week)) AS day_name,
    ROUND(AVG(daily_passages), 1) AS avg_passages,
    ROUND(AVG(daily_passengers), 1) AS avg_passengers,
    SUM(daily_passages) AS total_passages,
    COUNT(DISTINCT transition_date) AS num_days
FROM hourly_totals
GROUP BY road_type, transition_hour, day_of_week
ORDER BY road_type, day_of_week, transition_hour
