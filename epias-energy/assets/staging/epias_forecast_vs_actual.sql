/* @bruin
name: epias_staging.epias_forecast_vs_actual
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Joins day-ahead production plan (DPP first version) with actual real-time
  generation to compute forecast accuracy metrics. Computes hourly and daily
  error (MWh), error percentage, and absolute error for each energy source
  and for the total.

depends:
  - epias_raw.epias_realtime_generation
  - epias_raw.epias_dpp_first_version

materialization:
  type: table
  strategy: create+replace

columns:
  - name: date
    type: DATE
    description: Calendar date
    primary_key: true
    nullable: false
  - name: source_name
    type: VARCHAR
    description: Energy source (natural_gas, wind, solar, total, etc.)
    primary_key: true
    nullable: false
  - name: forecast_mwh
    type: DOUBLE
    description: Total day-ahead forecast for this source (MWh)
  - name: actual_mwh
    type: DOUBLE
    description: Total actual generation for this source (MWh)
  - name: error_mwh
    type: DOUBLE
    description: Forecast minus actual (MWh, positive = over-forecast)
  - name: abs_error_mwh
    type: DOUBLE
    description: Absolute forecast error (MWh)
  - name: error_pct
    type: DOUBLE
    description: Error as percentage of actual generation
  - name: year
    type: INTEGER
    description: Year extracted from date
  - name: month
    type: INTEGER
    description: Month number (1-12)

@bruin */

WITH actual_deduped AS (
    SELECT *
    FROM epias_raw.epias_realtime_generation
    WHERE date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY extracted_at DESC) = 1
),

forecast_deduped AS (
    SELECT *
    FROM epias_raw.epias_dpp_first_version
    WHERE date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY extracted_at DESC) = 1
),

actual_unpivoted AS (
    SELECT CAST(date AS DATE) AS date, 'natural_gas' AS source_name, COALESCE(natural_gas, 0) AS mwh FROM actual_deduped
    UNION ALL SELECT CAST(date AS DATE), 'wind', COALESCE(wind, 0) FROM actual_deduped
    UNION ALL SELECT CAST(date AS DATE), 'solar', COALESCE(solar, 0) FROM actual_deduped
    UNION ALL SELECT CAST(date AS DATE), 'lignite', COALESCE(lignite, 0) FROM actual_deduped
    UNION ALL SELECT CAST(date AS DATE), 'hard_coal', COALESCE(hard_coal, 0) FROM actual_deduped
    UNION ALL SELECT CAST(date AS DATE), 'geothermal', COALESCE(geothermal, 0) FROM actual_deduped
    UNION ALL SELECT CAST(date AS DATE), 'dammed_hydro', COALESCE(dammed_hydro, 0) FROM actual_deduped
    UNION ALL SELECT CAST(date AS DATE), 'river', COALESCE(river, 0) FROM actual_deduped
    UNION ALL SELECT CAST(date AS DATE), 'biomass', COALESCE(biomass, 0) FROM actual_deduped
    UNION ALL SELECT CAST(date AS DATE), 'total', COALESCE(total, 0) FROM actual_deduped
),

forecast_unpivoted AS (
    SELECT CAST(date AS DATE) AS date, 'natural_gas' AS source_name, COALESCE(natural_gas, 0) AS mwh FROM forecast_deduped
    UNION ALL SELECT CAST(date AS DATE), 'wind', COALESCE(wind, 0) FROM forecast_deduped
    UNION ALL SELECT CAST(date AS DATE), 'solar', COALESCE(solar, 0) FROM forecast_deduped
    UNION ALL SELECT CAST(date AS DATE), 'lignite', COALESCE(lignite, 0) FROM forecast_deduped
    UNION ALL SELECT CAST(date AS DATE), 'hard_coal', COALESCE(hard_coal, 0) FROM forecast_deduped
    UNION ALL SELECT CAST(date AS DATE), 'geothermal', COALESCE(geothermal, 0) FROM forecast_deduped
    UNION ALL SELECT CAST(date AS DATE), 'dammed_hydro', COALESCE(dammed_hydro, 0) FROM forecast_deduped
    UNION ALL SELECT CAST(date AS DATE), 'river', COALESCE(river, 0) FROM forecast_deduped
    UNION ALL SELECT CAST(date AS DATE), 'biomass', COALESCE(biomass, 0) FROM forecast_deduped
    UNION ALL SELECT CAST(date AS DATE), 'total', COALESCE(total, 0) FROM forecast_deduped
),

actual_daily AS (
    SELECT date, source_name, SUM(mwh) AS actual_mwh
    FROM actual_unpivoted
    GROUP BY date, source_name
),

forecast_daily AS (
    SELECT date, source_name, SUM(mwh) AS forecast_mwh
    FROM forecast_unpivoted
    GROUP BY date, source_name
)

SELECT
    COALESCE(f.date, a.date) AS date,
    COALESCE(f.source_name, a.source_name) AS source_name,
    ROUND(COALESCE(f.forecast_mwh, 0), 2) AS forecast_mwh,
    ROUND(COALESCE(a.actual_mwh, 0), 2) AS actual_mwh,
    ROUND(COALESCE(f.forecast_mwh, 0) - COALESCE(a.actual_mwh, 0), 2) AS error_mwh,
    ROUND(ABS(COALESCE(f.forecast_mwh, 0) - COALESCE(a.actual_mwh, 0)), 2) AS abs_error_mwh,
    ROUND(
        (COALESCE(f.forecast_mwh, 0) - COALESCE(a.actual_mwh, 0))
        / NULLIF(a.actual_mwh, 0) * 100,
        2
    ) AS error_pct,

    EXTRACT(YEAR FROM COALESCE(f.date, a.date)) AS year,
    EXTRACT(MONTH FROM COALESCE(f.date, a.date)) AS month

FROM forecast_daily f
FULL OUTER JOIN actual_daily a
    ON f.date = a.date AND f.source_name = a.source_name
WHERE COALESCE(f.date, a.date) < DATE_TRUNC(CURRENT_DATE(), MONTH)
ORDER BY date, source_name
