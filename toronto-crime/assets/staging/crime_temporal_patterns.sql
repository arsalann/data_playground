/* @bruin
name: staging.crime_temporal_patterns
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Aggregates CSI rows by year, category, day of week, and hour to support
  temporal concentration analysis in the dashboard.

depends:
  - staging.crime_events

materialization:
  type: table
  strategy: create+replace

columns:
  - name: occurrence_year
    type: INTEGER
    description: Occurrence year.
    primary_key: true
  - name: csi_category
    type: VARCHAR
    description: Community Safety Indicator category.
    primary_key: true
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
  - name: is_weekend
    type: BOOLEAN
    description: Whether occurrence day is Saturday or Sunday.
  - name: crime_count
    type: INTEGER
    description: Count of CSI offence/victim-level rows.
  - name: share_of_year_category
    type: DOUBLE
    description: Share of rows for this year and category in this day/hour cell.
  - name: hour_rank_within_category
    type: INTEGER
    description: Rank of this day/hour cell within year and category by row count.

@bruin */

WITH grouped AS (
    SELECT
        occurrence_year,
        csi_category,
        occurrence_day_of_week AS day_of_week,
        occurrence_day_of_week_num AS day_of_week_num,
        occurrence_hour AS hour,
        is_weekend,
        COUNT(*) AS crime_count
    FROM staging.crime_events
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    occurrence_year,
    csi_category,
    day_of_week,
    day_of_week_num,
    hour,
    is_weekend,
    crime_count,
    ROUND(SAFE_DIVIDE(
        crime_count,
        SUM(crime_count) OVER (PARTITION BY occurrence_year, csi_category)
    ), 5) AS share_of_year_category,
    RANK() OVER (
        PARTITION BY occurrence_year, csi_category
        ORDER BY crime_count DESC
    ) AS hour_rank_within_category
FROM grouped
ORDER BY occurrence_year, csi_category, day_of_week_num, hour
