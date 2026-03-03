/* @bruin
name: staging.searches_daily
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Aggregates raw Google searches to daily counts and enriches them with
  calendar features for pre/post-ChatGPT comparisons. Dates are derived
  from the UTC search timestamp.

depends:
  - raw.google_search_history

materialization:
  type: table
  strategy: create+replace

columns:
  - name: search_date
    type: DATE
    description: UTC date of the search activity
    primary_key: true
    nullable: false
  - name: search_count
    type: INTEGER
    description: Number of searches recorded on the date
  - name: year
    type: INTEGER
    description: Calendar year of the search date
  - name: month
    type: INTEGER
    description: Calendar month number (1-12)
  - name: month_name
    type: VARCHAR
    description: Calendar month name
  - name: day_of_week
    type: INTEGER
    description: Day of week number (1=Sunday, 7=Saturday)
  - name: day_name
    type: VARCHAR
    description: Day of week name
  - name: week_start
    type: DATE
    description: Week start date (Sunday)
  - name: season
    type: VARCHAR
    description: Meteorological season derived from the month
  - name: is_weekend
    type: BOOLEAN
    description: True if search date is Saturday or Sunday
  - name: is_post_chatgpt
    type: BOOLEAN
    description: True if date is on/after 2022-11-30 (ChatGPT launch)
  - name: era
    type: VARCHAR
    description: Pre-ChatGPT or Post-ChatGPT label

@bruin */

WITH cleaned AS (
    SELECT
        DATE(search_timestamp) AS search_date
    FROM raw.google_search_history
    WHERE search_timestamp IS NOT NULL
      AND search_phrase IS NOT NULL
      AND TRIM(search_phrase) != ''
),

daily AS (
    SELECT
        search_date,
        COUNT(1) AS search_count
    FROM cleaned
    GROUP BY 1
)

SELECT
    search_date,
    search_count,
    EXTRACT(YEAR FROM search_date) AS year,
    EXTRACT(MONTH FROM search_date) AS month,
    FORMAT_DATE('%B', search_date) AS month_name,
    EXTRACT(DAYOFWEEK FROM search_date) AS day_of_week,
    FORMAT_DATE('%A', search_date) AS day_name,
    DATE_TRUNC(search_date, WEEK(SUNDAY)) AS week_start,
    CASE
        WHEN EXTRACT(MONTH FROM search_date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM search_date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM search_date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS season,
    EXTRACT(DAYOFWEEK FROM search_date) IN (1, 7) AS is_weekend,
    search_date >= '2022-11-30' AS is_post_chatgpt,
    CASE
        WHEN search_date >= '2022-11-30' THEN 'Post-ChatGPT'
        ELSE 'Pre-ChatGPT'
    END AS era

FROM daily
ORDER BY search_date
