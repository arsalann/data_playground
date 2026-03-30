/* @bruin
name: staging.hormuz_prices_wide
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deduplicates raw FRED observations and pivots into wide format with one row
  per date. Covers energy prices, commodities, CPI inflation, consumer
  sentiment, inflation expectations, unemployment, and the yield curve.

depends:
  - raw.hormuz_fred_prices

materialization:
  type: table
  strategy: create+replace

columns:
  - name: observation_date
    type: DATE
    description: Date of the observation
    primary_key: true
    nullable: false
  - name: brent_crude_usd
    type: DOUBLE
    description: Brent crude oil price (USD/barrel)
  - name: wti_crude_usd
    type: DOUBLE
    description: WTI crude oil price (USD/barrel)
  - name: gasoline_usd
    type: DOUBLE
    description: US regular gasoline price (USD/gallon)
  - name: natural_gas_usd
    type: DOUBLE
    description: Henry Hub natural gas (USD/MMBtu)
  - name: wheat_usd
    type: DOUBLE
    description: Global wheat price (USD/metric ton)
  - name: copper_usd
    type: DOUBLE
    description: Global copper price (USD/metric ton)
  - name: cpi_all
    type: DOUBLE
    description: CPI All Items index (1982-84 = 100)
  - name: cpi_core
    type: DOUBLE
    description: CPI Core ex Food & Energy index (1982-84 = 100)
  - name: cpi_gasoline
    type: DOUBLE
    description: CPI Gasoline index (1982-84 = 100)
  - name: cpi_new_vehicles
    type: DOUBLE
    description: CPI New Vehicles index (1982-84 = 100)
  - name: consumer_sentiment
    type: DOUBLE
    description: U of Michigan Consumer Sentiment Index
  - name: inflation_expectations
    type: DOUBLE
    description: U of Michigan 1-year inflation expectations (percent)
  - name: unemployment_rate
    type: DOUBLE
    description: US unemployment rate (percent)
  - name: yield_curve
    type: DOUBLE
    description: 10Y minus 2Y Treasury spread (percent, negative = inverted)
  - name: brent_wti_spread
    type: DOUBLE
    description: Brent minus WTI spread (USD/barrel)

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.hormuz_fred_prices
    WHERE observation_date IS NOT NULL
      AND value IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY observation_date, series_id
        ORDER BY extracted_at DESC
    ) = 1
),

pivoted AS (
    SELECT
        observation_date,
        MAX(CASE WHEN series_id = 'DCOILBRENTEU' THEN value END) AS brent_crude_usd,
        MAX(CASE WHEN series_id = 'DCOILWTICO' THEN value END) AS wti_crude_usd,
        MAX(CASE WHEN series_id = 'GASREGW' THEN value END) AS gasoline_usd,
        MAX(CASE WHEN series_id = 'DHHNGSP' THEN value END) AS natural_gas_usd,
        MAX(CASE WHEN series_id = 'PWHEAMTUSDM' THEN value END) AS wheat_usd,
        MAX(CASE WHEN series_id = 'PCOPPUSDM' THEN value END) AS copper_usd,
        MAX(CASE WHEN series_id = 'CPIAUCSL' THEN value END) AS cpi_all,
        MAX(CASE WHEN series_id = 'CPILFESL' THEN value END) AS cpi_core,
        MAX(CASE WHEN series_id = 'CUSR0000SETB01' THEN value END) AS cpi_gasoline,
        MAX(CASE WHEN series_id = 'CUSR0000SETA01' THEN value END) AS cpi_new_vehicles,
        MAX(CASE WHEN series_id = 'UMCSENT' THEN value END) AS consumer_sentiment,
        MAX(CASE WHEN series_id = 'MICH' THEN value END) AS inflation_expectations,
        MAX(CASE WHEN series_id = 'UNRATE' THEN value END) AS unemployment_rate,
        MAX(CASE WHEN series_id = 'T10Y2Y' THEN value END) AS yield_curve
    FROM deduped
    GROUP BY observation_date
)

SELECT
    observation_date,
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
    consumer_sentiment,
    inflation_expectations,
    unemployment_rate,
    yield_curve,
    ROUND(brent_crude_usd - wti_crude_usd, 2) AS brent_wti_spread
FROM pivoted
ORDER BY observation_date
