/* @bruin
name: epias_staging.h1_lira_coal_shift
type: bq.sql
connection: bruin-playground-arsalan
description: |
  H1: "When the Lira crashes, Turkey burns more coal"
  Joins monthly TRY/USD exchange rate (FRED) with EPIAS generation mix and market
  prices at monthly granularity. Tests whether Lira depreciation correlates with
  shifts from gas to lignite/coal.

depends:
  - epias_raw.fred_tryusd
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
  - name: tryusd
    type: DOUBLE
    description: Monthly average Turkish Lira per US Dollar
  - name: tryusd_yoy_pct
    type: DOUBLE
    description: Year-over-year TRY/USD depreciation (%)
  - name: tryusd_3m_change_pct
    type: DOUBLE
    description: 3-month rolling change in TRY/USD (%)
  - name: gas_share_pct
    type: DOUBLE
    description: Natural gas share of monthly generation (%)
  - name: coal_share_pct
    type: DOUBLE
    description: Lignite + hard coal share of monthly generation (%)
  - name: hydro_share_pct
    type: DOUBLE
    description: Dammed hydro + river share of monthly generation (%)
  - name: renewable_share_pct
    type: DOUBLE
    description: Wind + solar + geothermal + biomass share (%)
  - name: mcp_avg_try
    type: DOUBLE
    description: Average monthly MCP in Turkish Lira
  - name: mcp_avg_eur
    type: DOUBLE
    description: Average monthly MCP in Euros

@bruin */

WITH fx AS (
    SELECT
        EXTRACT(YEAR FROM observation_date) AS year,
        EXTRACT(MONTH FROM observation_date) AS month,
        value AS tryusd
    FROM epias_raw.fred_tryusd
    WHERE value IS NOT NULL
),

fx_enriched AS (
    SELECT
        year,
        month,
        tryusd,
        ROUND(
            (tryusd - LAG(tryusd, 12) OVER (ORDER BY year, month))
            / NULLIF(LAG(tryusd, 12) OVER (ORDER BY year, month), 0) * 100,
            2
        ) AS tryusd_yoy_pct,
        ROUND(
            (tryusd - LAG(tryusd, 3) OVER (ORDER BY year, month))
            / NULLIF(LAG(tryusd, 3) OVER (ORDER BY year, month), 0) * 100,
            2
        ) AS tryusd_3m_change_pct
    FROM fx
),

gen_monthly AS (
    SELECT
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        SUM(generation_mwh) AS total_mwh,
        SUM(CASE WHEN source_name = 'natural_gas' THEN generation_mwh ELSE 0 END) AS gas_mwh,
        SUM(CASE WHEN source_name IN ('lignite', 'hard_coal') THEN generation_mwh ELSE 0 END) AS coal_mwh,
        SUM(CASE WHEN source_name IN ('dammed_hydro', 'river') THEN generation_mwh ELSE 0 END) AS hydro_mwh,
        SUM(CASE WHEN source_name IN ('wind', 'solar', 'geothermal', 'biomass') THEN generation_mwh ELSE 0 END) AS renew_mwh
    FROM epias_staging.epias_generation_daily
    WHERE source_name != 'import_export'
    GROUP BY 1, 2
),

gen_shares AS (
    SELECT
        year, month,
        ROUND(gas_mwh / NULLIF(total_mwh, 0) * 100, 2) AS gas_share_pct,
        ROUND(coal_mwh / NULLIF(total_mwh, 0) * 100, 2) AS coal_share_pct,
        ROUND(hydro_mwh / NULLIF(total_mwh, 0) * 100, 2) AS hydro_share_pct,
        ROUND(renew_mwh / NULLIF(total_mwh, 0) * 100, 2) AS renewable_share_pct
    FROM gen_monthly
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
    g.year,
    g.month,
    f.tryusd,
    f.tryusd_yoy_pct,
    f.tryusd_3m_change_pct,
    g.gas_share_pct,
    g.coal_share_pct,
    g.hydro_share_pct,
    g.renewable_share_pct,
    p.mcp_avg_try,
    p.mcp_avg_eur

FROM gen_shares g
LEFT JOIN fx_enriched f ON g.year = f.year AND g.month = f.month
LEFT JOIN prices_monthly p ON g.year = p.year AND g.month = p.month
WHERE DATE(g.year, g.month, 1) < DATE_TRUNC(CURRENT_DATE(), MONTH)
ORDER BY g.year, g.month
