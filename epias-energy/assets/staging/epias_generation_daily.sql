/* @bruin
name: staging.epias_generation_daily
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Unpivots hourly real-time generation data from wide format (one column per
  source) into long format (source_name + generation_mwh), then aggregates to
  daily totals. Adds temporal dimensions (year, month, season, day_of_week)
  and computes each source's share of total daily generation.

depends:
  - raw.epias_realtime_generation

materialization:
  type: table
  strategy: create+replace

columns:
  - name: date
    type: DATE
    description: Calendar date of generation
    primary_key: true
    nullable: false
  - name: source_name
    type: VARCHAR
    description: Energy source (natural_gas, wind, solar, lignite, etc.)
    primary_key: true
    nullable: false
  - name: generation_mwh
    type: DOUBLE
    description: Total generation for this source on this date (MWh)
  - name: share_pct
    type: DOUBLE
    description: Percentage of total daily generation from this source
  - name: year
    type: INTEGER
    description: Year extracted from date
  - name: month
    type: INTEGER
    description: Month number (1-12)
  - name: season
    type: VARCHAR
    description: Season label (Winter, Spring, Summer, Autumn)
  - name: day_of_week
    type: VARCHAR
    description: Day of week name (Monday-Sunday)

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.epias_realtime_generation
    WHERE date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY extracted_at DESC) = 1
),

unpivoted AS (
    SELECT CAST(date AS DATE) AS date, 'natural_gas' AS source_name, COALESCE(natural_gas, 0) AS generation_mwh FROM deduped
    UNION ALL
    SELECT CAST(date AS DATE), 'wind', COALESCE(wind, 0) FROM deduped
    UNION ALL
    SELECT CAST(date AS DATE), 'solar', COALESCE(solar, 0) FROM deduped
    UNION ALL
    SELECT CAST(date AS DATE), 'lignite', COALESCE(lignite, 0) FROM deduped
    UNION ALL
    SELECT CAST(date AS DATE), 'hard_coal', COALESCE(hard_coal, 0) FROM deduped
    UNION ALL
    SELECT CAST(date AS DATE), 'fuel_oil', COALESCE(fuel_oil, 0) FROM deduped
    UNION ALL
    SELECT CAST(date AS DATE), 'geothermal', COALESCE(geothermal, 0) FROM deduped
    UNION ALL
    SELECT CAST(date AS DATE), 'dammed_hydro', COALESCE(dammed_hydro, 0) FROM deduped
    UNION ALL
    SELECT CAST(date AS DATE), 'river', COALESCE(river, 0) FROM deduped
    UNION ALL
    SELECT CAST(date AS DATE), 'biomass', COALESCE(biomass, 0) FROM deduped
    UNION ALL
    SELECT CAST(date AS DATE), 'naphta', COALESCE(naphta, 0) FROM deduped
    UNION ALL
    SELECT CAST(date AS DATE), 'import_export', COALESCE(import_export, 0) FROM deduped
),

daily AS (
    SELECT
        date,
        source_name,
        SUM(generation_mwh) AS generation_mwh
    FROM unpivoted
    GROUP BY date, source_name
),

daily_totals AS (
    SELECT date, SUM(generation_mwh) AS total_mwh
    FROM daily
    WHERE source_name != 'import_export'
    GROUP BY date
)

SELECT
    d.date,
    d.source_name,
    ROUND(d.generation_mwh, 2) AS generation_mwh,
    ROUND(d.generation_mwh / NULLIF(t.total_mwh, 0) * 100, 2) AS share_pct,

    EXTRACT(YEAR FROM d.date) AS year,
    EXTRACT(MONTH FROM d.date) AS month,
    CASE
        WHEN EXTRACT(MONTH FROM d.date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM d.date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM d.date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Autumn'
    END AS season,
    FORMAT_DATE('%A', d.date) AS day_of_week

FROM daily d
LEFT JOIN daily_totals t ON d.date = t.date
ORDER BY d.date, d.generation_mwh DESC
