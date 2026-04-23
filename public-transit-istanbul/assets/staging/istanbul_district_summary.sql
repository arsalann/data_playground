/* @bruin
name: staging.istanbul_district_summary
type: bq.sql
connection: bruin-playground-arsalan
description: |
  District-level ridership summary aggregated by town, transport mode, and year.
  Calculates per-district totals and mode splits.
  Used for the underserved districts analysis.

depends:
  - raw.istanbul_hourly_transport

materialization:
  type: table
  strategy: create+replace

columns:
  - name: town
    type: VARCHAR
    description: Istanbul district (ilce)
    primary_key: true
  - name: road_type
    type: VARCHAR
    description: Transport system type
    primary_key: true
  - name: year
    type: INTEGER
    description: Calendar year
    primary_key: true
  - name: total_passages
    type: INTEGER
    description: Total Istanbulkart tap-ins for this district/mode/year
  - name: total_passengers
    type: INTEGER
    description: Total unique passengers for this district/mode/year
  - name: district_total_passages
    type: INTEGER
    description: Total passages across all modes for the district and year
  - name: mode_share_pct
    type: DOUBLE
    description: Percentage of district ridership by this mode

@bruin */

WITH by_district_mode_year AS (
    SELECT
        town,
        road_type,
        EXTRACT(YEAR FROM transition_date) AS year,
        SUM(COALESCE(number_of_passage, 0)) AS total_passages,
        SUM(COALESCE(number_of_passenger, 0)) AS total_passengers
    FROM raw.istanbul_hourly_transport
    WHERE transition_date IS NOT NULL
      AND town IS NOT NULL
      AND town != ''
    GROUP BY town, road_type, EXTRACT(YEAR FROM transition_date)
),
district_totals AS (
    SELECT
        town,
        year,
        SUM(total_passages) AS district_total_passages
    FROM by_district_mode_year
    GROUP BY town, year
)

SELECT
    d.town,
    d.road_type,
    d.year,
    d.total_passages,
    d.total_passengers,
    dt.district_total_passages,
    ROUND(SAFE_DIVIDE(d.total_passages, dt.district_total_passages) * 100, 2) AS mode_share_pct
FROM by_district_mode_year d
JOIN district_totals dt
    ON d.town = dt.town AND d.year = dt.year
ORDER BY d.town, d.year, d.total_passages DESC
