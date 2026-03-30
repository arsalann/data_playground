/* @bruin
name: staging.hormuz_crisis_analysis
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Enriches wide-format data with crisis period flags, YoY inflation rates,
  rolling averages, and the Brent-WTI spread. Primary table for the dashboard.

  Crisis periods: 2011 Arab Spring, 2014 Oil Glut, 2020 COVID Crash,
  2022 Russia-Ukraine, 2026 Hormuz Crisis.

depends:
  - staging.hormuz_prices_wide

materialization:
  type: table
  strategy: create+replace

columns:
  - name: observation_date
    type: DATE
    description: Date of the observation
    primary_key: true
    nullable: false
  - name: year
    type: INTEGER
    description: Year
  - name: month
    type: INTEGER
    description: Month
  - name: brent_crude_usd
    type: DOUBLE
    description: Brent crude oil price (USD/barrel)
  - name: wti_crude_usd
    type: DOUBLE
    description: WTI crude oil price (USD/barrel)
  - name: gasoline_usd
    type: DOUBLE
    description: US regular gasoline (USD/gallon)
  - name: natural_gas_usd
    type: DOUBLE
    description: Henry Hub natural gas (USD/MMBtu)
  - name: wheat_usd
    type: DOUBLE
    description: Global wheat (USD/metric ton)
  - name: copper_usd
    type: DOUBLE
    description: Global copper (USD/metric ton)
  - name: cpi_all
    type: DOUBLE
    description: CPI All Items index
  - name: cpi_core
    type: DOUBLE
    description: CPI Core ex Food & Energy index
  - name: cpi_gasoline
    type: DOUBLE
    description: CPI Gasoline index
  - name: cpi_new_vehicles
    type: DOUBLE
    description: CPI New Vehicles index
  - name: cpi_all_yoy
    type: DOUBLE
    description: CPI All Items year-over-year percent change
  - name: cpi_core_yoy
    type: DOUBLE
    description: CPI Core year-over-year percent change
  - name: cpi_gasoline_yoy
    type: DOUBLE
    description: CPI Gasoline year-over-year percent change
  - name: cpi_vehicles_yoy
    type: DOUBLE
    description: CPI New Vehicles year-over-year percent change
  - name: consumer_sentiment
    type: DOUBLE
    description: U of Michigan Consumer Sentiment Index
  - name: inflation_expectations
    type: DOUBLE
    description: 1-year inflation expectations (percent)
  - name: unemployment_rate
    type: DOUBLE
    description: Unemployment rate (percent)
  - name: yield_curve
    type: DOUBLE
    description: 10Y-2Y Treasury spread (percent)
  - name: brent_wti_spread
    type: DOUBLE
    description: Brent minus WTI spread (USD/barrel)
  - name: crisis_period
    type: VARCHAR
    description: Name of crisis period or Normal
  - name: is_crisis
    type: BOOLEAN
    description: True during a defined crisis period
  - name: brent_30d_avg
    type: DOUBLE
    description: 30-day rolling average of Brent crude
  - name: brent_90d_avg
    type: DOUBLE
    description: 90-day rolling average of Brent crude

@bruin */

-- Compute YoY inflation on monthly-only rows first, then join back to daily
WITH base AS (
    SELECT *
    FROM staging.hormuz_prices_wide
),

-- Monthly CPI rows only (where cpi_all is not null)
monthly_cpi AS (
    SELECT
        observation_date,
        cpi_all,
        cpi_core,
        cpi_gasoline,
        cpi_new_vehicles,
        ROUND(
            (cpi_all - LAG(cpi_all, 12) OVER (ORDER BY observation_date))
            / NULLIF(LAG(cpi_all, 12) OVER (ORDER BY observation_date), 0) * 100, 2
        ) AS cpi_all_yoy,
        ROUND(
            (cpi_core - LAG(cpi_core, 12) OVER (ORDER BY observation_date))
            / NULLIF(LAG(cpi_core, 12) OVER (ORDER BY observation_date), 0) * 100, 2
        ) AS cpi_core_yoy,
        ROUND(
            (cpi_gasoline - LAG(cpi_gasoline, 12) OVER (ORDER BY observation_date))
            / NULLIF(LAG(cpi_gasoline, 12) OVER (ORDER BY observation_date), 0) * 100, 2
        ) AS cpi_gasoline_yoy,
        ROUND(
            (cpi_new_vehicles - LAG(cpi_new_vehicles, 12) OVER (ORDER BY observation_date))
            / NULLIF(LAG(cpi_new_vehicles, 12) OVER (ORDER BY observation_date), 0) * 100, 2
        ) AS cpi_vehicles_yoy
    FROM base
    WHERE cpi_all IS NOT NULL
),

with_crisis AS (
    SELECT
        b.*,
        CASE
            WHEN b.observation_date BETWEEN '2011-01-01' AND '2011-12-31'
                THEN '2011 Arab Spring'
            WHEN b.observation_date BETWEEN '2014-06-01' AND '2016-02-29'
                THEN '2014 Oil Glut'
            WHEN b.observation_date BETWEEN '2020-03-01' AND '2020-04-30'
                THEN '2020 COVID Crash'
            WHEN b.observation_date BETWEEN '2022-02-24' AND '2022-12-31'
                THEN '2022 Russia-Ukraine'
            WHEN b.observation_date >= '2026-03-01'
                THEN '2026 Hormuz Crisis'
            ELSE 'Normal'
        END AS crisis_period,
        m.cpi_all_yoy,
        m.cpi_core_yoy,
        m.cpi_gasoline_yoy,
        m.cpi_vehicles_yoy
    FROM base b
    LEFT JOIN monthly_cpi m ON b.observation_date = m.observation_date
)

SELECT
    observation_date,
    EXTRACT(YEAR FROM observation_date) AS year,
    EXTRACT(MONTH FROM observation_date) AS month,

    brent_crude_usd,
    wti_crude_usd,
    gasoline_usd,
    natural_gas_usd,
    wheat_usd,
    copper_usd,

    cpi_all,
    cpi_core,
    cpi_gasoline,
    cpi_new_vehicles,
    cpi_all_yoy,
    cpi_core_yoy,
    cpi_gasoline_yoy,
    cpi_vehicles_yoy,

    consumer_sentiment,
    inflation_expectations,
    unemployment_rate,
    yield_curve,
    brent_wti_spread,

    crisis_period,
    crisis_period != 'Normal' AS is_crisis,

    ROUND(AVG(brent_crude_usd) OVER (
        ORDER BY observation_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 2) AS brent_30d_avg,

    ROUND(AVG(brent_crude_usd) OVER (
        ORDER BY observation_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ), 2) AS brent_90d_avg

FROM with_crisis
ORDER BY observation_date
