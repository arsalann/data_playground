/* @bruin
name: epias_staging.h2_drought_hydro_mcp
type: bq.sql
connection: bruin-playground-arsalan
description: |
  H2: "Turkey's droughts are hidden electricity crises"
  Joins Turkey precipitation data with hydro generation and MCP prices at monthly
  granularity. Tests whether precipitation deficits predict hydro share decline
  and electricity price spikes.

depends:
  - epias_raw.openmeteo_turkey_weather
  - epias_staging.epias_generation_daily
  - epias_staging.epias_market_prices_daily

materialization:
  type: table
  strategy: create+replace

columns:
  - name: year
    type: INTEGER
    description: Year
    primary_key: true
    nullable: false
  - name: month
    type: INTEGER
    description: Month (1-12)
    primary_key: true
    nullable: false
  - name: precip_mm
    type: DOUBLE
    description: Average monthly precipitation across all stations (mm)
  - name: precip_long_term_avg_mm
    type: DOUBLE
    description: Long-term average precipitation for this calendar month (mm)
  - name: precip_anomaly_pct
    type: DOUBLE
    description: Precipitation anomaly vs long-term average (%)
  - name: precip_cumulative_3m_mm
    type: DOUBLE
    description: Cumulative precipitation over prior 3 months (mm)
  - name: precip_cumulative_6m_mm
    type: DOUBLE
    description: Cumulative precipitation over prior 6 months (mm)
  - name: temp_mean_c
    type: DOUBLE
    description: Average monthly temperature across all stations (C)
  - name: hydro_generation_mwh
    type: DOUBLE
    description: Total monthly hydro generation (dammed + river) in MWh
  - name: hydro_share_pct
    type: DOUBLE
    description: Hydro share of total generation (%)
  - name: total_generation_mwh
    type: DOUBLE
    description: Total monthly generation in MWh
  - name: mcp_avg_try
    type: DOUBLE
    description: Average monthly MCP in Turkish Lira
  - name: mcp_avg_eur
    type: DOUBLE
    description: Average monthly MCP in Euros

@bruin */

WITH weather_monthly AS (
    SELECT
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        ROUND(AVG(precipitation_mm) * 30, 1) AS precip_mm,
        ROUND(AVG(temp_mean_c), 2) AS temp_mean_c
    FROM epias_raw.openmeteo_turkey_weather
    WHERE date IS NOT NULL
    GROUP BY 1, 2
),

weather_with_normals AS (
    SELECT
        w.*,
        ROUND(AVG(w.precip_mm) OVER (PARTITION BY w.month), 1) AS precip_long_term_avg_mm,
        ROUND(
            (w.precip_mm - AVG(w.precip_mm) OVER (PARTITION BY w.month))
            / NULLIF(AVG(w.precip_mm) OVER (PARTITION BY w.month), 0) * 100,
            1
        ) AS precip_anomaly_pct,
        ROUND(
            SUM(w.precip_mm) OVER (ORDER BY w.year, w.month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
            1
        ) AS precip_cumulative_3m_mm,
        ROUND(
            SUM(w.precip_mm) OVER (ORDER BY w.year, w.month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
            1
        ) AS precip_cumulative_6m_mm
    FROM weather_monthly w
),

hydro_monthly AS (
    SELECT
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        SUM(CASE WHEN source_name IN ('dammed_hydro', 'river') THEN generation_mwh ELSE 0 END) AS hydro_generation_mwh,
        SUM(CASE WHEN source_name != 'import_export' THEN generation_mwh ELSE 0 END) AS total_generation_mwh
    FROM epias_staging.epias_generation_daily
    GROUP BY 1, 2
),

hydro_with_share AS (
    SELECT
        *,
        ROUND(hydro_generation_mwh / NULLIF(total_generation_mwh, 0) * 100, 2) AS hydro_share_pct
    FROM hydro_monthly
),

prices_monthly AS (
    SELECT
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        ROUND(AVG(mcp_avg), 2) AS mcp_avg_try,
        ROUND(AVG(mcp_avg_eur), 4) AS mcp_avg_eur
    FROM epias_staging.epias_market_prices_daily
    GROUP BY 1, 2
)

SELECT
    w.year,
    w.month,
    w.precip_mm,
    w.precip_long_term_avg_mm,
    w.precip_anomaly_pct,
    w.precip_cumulative_3m_mm,
    w.precip_cumulative_6m_mm,
    w.temp_mean_c,
    ROUND(h.hydro_generation_mwh, 2) AS hydro_generation_mwh,
    h.hydro_share_pct,
    ROUND(h.total_generation_mwh, 2) AS total_generation_mwh,
    p.mcp_avg_try,
    p.mcp_avg_eur

FROM weather_with_normals w
LEFT JOIN hydro_with_share h ON w.year = h.year AND w.month = h.month
LEFT JOIN prices_monthly p ON w.year = p.year AND w.month = p.month
WHERE DATE(w.year, w.month, 1) < DATE_TRUNC(CURRENT_DATE(), MONTH)
ORDER BY w.year, w.month
