/* @bruin
name: staging.istanbul_transit_traffic
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Joins daily transit ridership totals with the Istanbul traffic congestion index.
  Enables correlation analysis between public transit usage and road congestion.

depends:
  - raw.istanbul_hourly_transport
  - raw.istanbul_traffic_index

materialization:
  type: table
  strategy: create+replace

columns:
  - name: traffic_date
    type: DATE
    description: Date of observation
    primary_key: true
  - name: total_passages
    type: INTEGER
    description: Total Istanbulkart tap-ins across all modes for the day
  - name: total_passengers
    type: INTEGER
    description: Total unique passengers across all modes for the day
  - name: min_traffic_index
    type: DOUBLE
    description: Minimum traffic congestion index for the day (0-100)
  - name: max_traffic_index
    type: DOUBLE
    description: Maximum traffic congestion index for the day (0-100)
  - name: avg_traffic_index
    type: DOUBLE
    description: Average traffic congestion index for the day (0-100)
  - name: year
    type: INTEGER
    description: Calendar year
  - name: month
    type: INTEGER
    description: Calendar month (1-12)
  - name: day_of_week
    type: INTEGER
    description: Day of week (1=Sunday, 7=Saturday)
  - name: day_name
    type: VARCHAR
    description: Day of week name
  - name: is_weekend
    type: BOOLEAN
    description: Whether the day is Saturday or Sunday

@bruin */

WITH daily_transit AS (
    SELECT
        transition_date AS traffic_date,
        SUM(COALESCE(number_of_passage, 0)) AS total_passages,
        SUM(COALESCE(number_of_passenger, 0)) AS total_passengers
    FROM raw.istanbul_hourly_transport
    WHERE transition_date IS NOT NULL
    GROUP BY transition_date
),
traffic AS (
    SELECT DISTINCT
        traffic_date,
        min_traffic_index,
        max_traffic_index,
        avg_traffic_index
    FROM raw.istanbul_traffic_index
    WHERE traffic_date IS NOT NULL
)

SELECT
    COALESCE(t.traffic_date, tr.traffic_date) AS traffic_date,
    t.total_passages,
    t.total_passengers,
    tr.min_traffic_index,
    tr.max_traffic_index,
    tr.avg_traffic_index,
    EXTRACT(YEAR FROM COALESCE(t.traffic_date, tr.traffic_date)) AS year,
    EXTRACT(MONTH FROM COALESCE(t.traffic_date, tr.traffic_date)) AS month,
    EXTRACT(DAYOFWEEK FROM COALESCE(t.traffic_date, tr.traffic_date)) AS day_of_week,
    FORMAT_DATE('%A', COALESCE(t.traffic_date, tr.traffic_date)) AS day_name,
    EXTRACT(DAYOFWEEK FROM COALESCE(t.traffic_date, tr.traffic_date)) IN (1, 7) AS is_weekend
FROM daily_transit t
FULL OUTER JOIN traffic tr
    ON t.traffic_date = tr.traffic_date
ORDER BY traffic_date
