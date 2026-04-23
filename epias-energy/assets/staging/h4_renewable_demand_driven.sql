/* @bruin
name: epias_staging.h4_renewable_demand_driven
type: bq.sql
connection: bruin-playground-arsalan
description: |
  H4: "Turkey's renewable growth is demand-driven, not policy-driven"
  Joins World Bank Turkey development indicators (GDP, urbanization) with EPIAS
  annual generation trends. Tests whether renewable capacity correlates more with
  economic growth than policy milestones.

  Cross-pipeline reference: baby-bust pipeline (staging.fertility_squeeze)

depends:
  - epias_staging.epias_generation_daily

materialization:
  type: table
  strategy: create+replace

columns:
  - name: year
    type: INTEGER
    description: Calendar year
    primary_key: true
    nullable: false
  - name: gdp_per_capita_ppp
    type: DOUBLE
    description: GDP per capita PPP (current international $)
  - name: gdp_yoy_pct
    type: DOUBLE
    description: Year-over-year GDP per capita growth (%)
  - name: urbanization_pct
    type: DOUBLE
    description: Urban population as % of total
  - name: total_generation_twh
    type: DOUBLE
    description: Total annual electricity generation (TWh)
  - name: generation_yoy_pct
    type: DOUBLE
    description: Year-over-year total generation growth (%)
  - name: renewable_generation_twh
    type: DOUBLE
    description: Total renewable generation (TWh)
  - name: renewable_share_pct
    type: DOUBLE
    description: Renewable share of total generation (%)
  - name: renewable_yoy_pct
    type: DOUBLE
    description: Year-over-year renewable generation growth (%)
  - name: solar_twh
    type: DOUBLE
    description: Solar generation (TWh)
  - name: wind_twh
    type: DOUBLE
    description: Wind generation (TWh)
  - name: hydro_twh
    type: DOUBLE
    description: Hydro generation (dammed + river) (TWh)
  - name: coal_twh
    type: DOUBLE
    description: Coal generation (lignite + hard coal) (TWh)
  - name: gas_twh
    type: DOUBLE
    description: Natural gas generation (TWh)
  - name: coal_share_pct
    type: DOUBLE
    description: Coal share of total generation (%)
  - name: gas_share_pct
    type: DOUBLE
    description: Natural gas share of total generation (%)

@bruin */

WITH turkey_dev AS (
    SELECT
        year,
        gdp_per_capita_ppp,
        urbanization_pct
    FROM `bruin-playground-arsalan`.staging.fertility_squeeze
    WHERE country_code = 'TUR'
      AND year >= 2015
),

annual_gen AS (
    SELECT
        EXTRACT(YEAR FROM date) AS year,
        ROUND(SUM(generation_mwh) / 1e6, 3) AS total_generation_twh,
        ROUND(SUM(CASE WHEN source_name IN ('wind', 'solar', 'geothermal', 'biomass', 'dammed_hydro', 'river')
                       THEN generation_mwh ELSE 0 END) / 1e6, 3) AS renewable_generation_twh,
        ROUND(SUM(CASE WHEN source_name = 'solar' THEN generation_mwh ELSE 0 END) / 1e6, 3) AS solar_twh,
        ROUND(SUM(CASE WHEN source_name = 'wind' THEN generation_mwh ELSE 0 END) / 1e6, 3) AS wind_twh,
        ROUND(SUM(CASE WHEN source_name IN ('dammed_hydro', 'river') THEN generation_mwh ELSE 0 END) / 1e6, 3) AS hydro_twh,
        ROUND(SUM(CASE WHEN source_name IN ('lignite', 'hard_coal') THEN generation_mwh ELSE 0 END) / 1e6, 3) AS coal_twh,
        ROUND(SUM(CASE WHEN source_name = 'natural_gas' THEN generation_mwh ELSE 0 END) / 1e6, 3) AS gas_twh
    FROM epias_staging.epias_generation_daily
    WHERE source_name != 'import_export'
    GROUP BY 1
),

annual_enriched AS (
    SELECT
        *,
        ROUND(renewable_generation_twh / NULLIF(total_generation_twh, 0) * 100, 2) AS renewable_share_pct,
        ROUND(coal_twh / NULLIF(total_generation_twh, 0) * 100, 2) AS coal_share_pct,
        ROUND(gas_twh / NULLIF(total_generation_twh, 0) * 100, 2) AS gas_share_pct,
        ROUND(
            (total_generation_twh - LAG(total_generation_twh) OVER (ORDER BY year))
            / NULLIF(LAG(total_generation_twh) OVER (ORDER BY year), 0) * 100,
            2
        ) AS generation_yoy_pct,
        ROUND(
            (renewable_generation_twh - LAG(renewable_generation_twh) OVER (ORDER BY year))
            / NULLIF(LAG(renewable_generation_twh) OVER (ORDER BY year), 0) * 100,
            2
        ) AS renewable_yoy_pct
    FROM annual_gen
),

dev_enriched AS (
    SELECT
        *,
        ROUND(
            (gdp_per_capita_ppp - LAG(gdp_per_capita_ppp) OVER (ORDER BY year))
            / NULLIF(LAG(gdp_per_capita_ppp) OVER (ORDER BY year), 0) * 100,
            2
        ) AS gdp_yoy_pct
    FROM turkey_dev
)

SELECT
    g.year,
    d.gdp_per_capita_ppp,
    d.gdp_yoy_pct,
    d.urbanization_pct,
    g.total_generation_twh,
    g.generation_yoy_pct,
    g.renewable_generation_twh,
    g.renewable_share_pct,
    g.renewable_yoy_pct,
    g.solar_twh,
    g.wind_twh,
    g.hydro_twh,
    g.coal_twh,
    g.gas_twh,
    g.coal_share_pct,
    g.gas_share_pct

FROM annual_enriched g
LEFT JOIN dev_enriched d ON g.year = d.year
WHERE g.year >= 2015
ORDER BY g.year
