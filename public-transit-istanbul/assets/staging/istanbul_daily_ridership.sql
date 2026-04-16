/* @bruin
name: staging.istanbul_daily_ridership
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Daily ridership aggregated from hourly Istanbulkart data.
  Aggregates by date, transport mode (road_type), line, district (town), and transfer type.
  Deduplicates overlapping months from the append strategy.
  Adds temporal dimensions: day_of_week, is_weekend, month_name, year, month.

depends:
  - raw.istanbul_hourly_transport

materialization:
  type: table
  strategy: create+replace

columns:
  - name: transition_date
    type: DATE
    description: Date of transit activity
    primary_key: true
  - name: road_type
    type: VARCHAR
    description: Transport system type (RAYLI, DENIZYOLU, KARAYOLU, METROBUS, etc.)
    primary_key: true
  - name: line
    type: VARCHAR
    description: Route/line identifier
    primary_key: true
  - name: town
    type: VARCHAR
    description: Istanbul district (ilce)
    primary_key: true
  - name: transfer_type
    type: VARCHAR
    description: Trip type (Normal or Aktarma/transfer)
    primary_key: true
  - name: total_passages
    type: INTEGER
    description: Total Istanbulkart tap-ins for the day
  - name: total_passengers
    type: INTEGER
    description: Total unique passengers for the day
  - name: year
    type: INTEGER
    description: Calendar year
  - name: month
    type: INTEGER
    description: Calendar month (1-12)
  - name: day_of_week
    type: INTEGER
    description: Day of week (1=Sunday, 7=Saturday in BigQuery)
  - name: day_name
    type: VARCHAR
    description: Day of week name (Monday, Tuesday, etc.)
  - name: month_name
    type: VARCHAR
    description: Month name (January, February, etc.)
  - name: is_weekend
    type: BOOLEAN
    description: Whether the day is Saturday or Sunday

@bruin */

SELECT
    transition_date,
    road_type,
    line,
    town,
    transfer_type,
    SUM(COALESCE(number_of_passage, 0)) AS total_passages,
    SUM(COALESCE(number_of_passenger, 0)) AS total_passengers,
    EXTRACT(YEAR FROM transition_date) AS year,
    EXTRACT(MONTH FROM transition_date) AS month,
    EXTRACT(DAYOFWEEK FROM transition_date) AS day_of_week,
    FORMAT_DATE('%A', transition_date) AS day_name,
    FORMAT_DATE('%B', transition_date) AS month_name,
    EXTRACT(DAYOFWEEK FROM transition_date) IN (1, 7) AS is_weekend
FROM raw.istanbul_hourly_transport
WHERE transition_date IS NOT NULL
GROUP BY
    transition_date,
    road_type,
    line,
    town,
    transfer_type
ORDER BY transition_date, road_type, line
