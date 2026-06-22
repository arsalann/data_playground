/* @bruin
name: report.temporal_heatmap
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Latest-complete-year CSI rows by day of week and hour for DAC heatmap
  analysis.

depends:
  - staging.crime_temporal_patterns

materialization:
  type: table
  strategy: create+replace

columns:
  - name: day_of_week
    type: VARCHAR
    description: Occurrence day of week.
    primary_key: true
  - name: day_of_week_num
    type: INTEGER
    description: BigQuery day-of-week number, Sunday=1.
    primary_key: true
  - name: hour
    type: INTEGER
    description: Occurrence hour of day, 0-23.
    primary_key: true
  - name: crime_count
    type: INTEGER
    description: CSI rows in this day/hour cell.
  - name: share_of_week
    type: DOUBLE
    description: Share of latest-complete-year weekly pattern represented by this day/hour cell.
  - name: latest_complete_year
    type: INTEGER
    description: Latest year treated as complete for yearly comparisons.

@bruin */

WITH latest_year AS (
    SELECT COALESCE(
        MAX(IF(occurrence_year < EXTRACT(YEAR FROM CURRENT_DATE()), occurrence_year, NULL)),
        MAX(occurrence_year)
    ) AS latest_complete_year
    FROM staging.crime_temporal_patterns
),

grouped AS (
    SELECT
        day_of_week,
        day_of_week_num,
        hour,
        SUM(crime_count) AS crime_count,
        ly.latest_complete_year
    FROM staging.crime_temporal_patterns AS t
    CROSS JOIN latest_year AS ly
    WHERE t.occurrence_year = ly.latest_complete_year
    GROUP BY 1, 2, 3, 5
),

days AS (
    SELECT *
    FROM UNNEST([
        STRUCT(1 AS day_of_week_num, 'Sunday' AS day_of_week),
        STRUCT(2 AS day_of_week_num, 'Monday' AS day_of_week),
        STRUCT(3 AS day_of_week_num, 'Tuesday' AS day_of_week),
        STRUCT(4 AS day_of_week_num, 'Wednesday' AS day_of_week),
        STRUCT(5 AS day_of_week_num, 'Thursday' AS day_of_week),
        STRUCT(6 AS day_of_week_num, 'Friday' AS day_of_week),
        STRUCT(7 AS day_of_week_num, 'Saturday' AS day_of_week)
    ])
),

hours AS (
    SELECT hour
    FROM UNNEST(GENERATE_ARRAY(0, 23)) AS hour
),

complete_grid AS (
    SELECT
        d.day_of_week,
        d.day_of_week_num,
        h.hour,
        COALESCE(g.crime_count, 0) AS crime_count,
        ly.latest_complete_year
    FROM days AS d
    CROSS JOIN hours AS h
    CROSS JOIN latest_year AS ly
    LEFT JOIN grouped AS g
        ON d.day_of_week_num = g.day_of_week_num
       AND h.hour = g.hour
)

SELECT
    day_of_week,
    day_of_week_num,
    hour,
    crime_count,
    ROUND(SAFE_DIVIDE(crime_count, SUM(crime_count) OVER ()), 5) AS share_of_week,
    latest_complete_year
FROM complete_grid
ORDER BY day_of_week_num, hour
