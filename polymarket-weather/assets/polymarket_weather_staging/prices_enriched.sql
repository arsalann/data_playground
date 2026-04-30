/* @bruin

name: polymarket_weather_staging.prices_enriched
type: bq.sql
description: |
  High-resolution price history for Polymarket weather prediction markets, central to forensic analysis of alleged Paris-CDG temperature sensor tampering during April 2026. Each row represents a sub-hour price tick (implied probability) for a specific prediction market outcome, enriched with weather-specific market classifications.

  This asset combines deduplicated CLOB price data with systematic market categorization from markets_enriched, creating an analysis-ready dataset for investigating trading behavior around suspected sensor manipulation events. The deduplication strategy retains the freshest price tick per (token_id, ts_utc) from the raw extraction.

  Key forensic features:
  - Sub-hour price precision captures intraday trading dynamics during alleged tampering events (Apr 6 & 15, 2026)
  - Market classification enables filtering by city/period/metric for targeted analysis
  - Temperature bucket metadata supports counterfactual resolution analysis
  - Paris timezone conversion facilitates correlation with local weather station data
  - 39.4% of records flagged as Paris daily April 2026 markets (255,769 of 648,997 ticks)

  Statistical profile: 648,997 price ticks across 1,074 distinct tokens and 537 markets. Price range spans full probability spectrum (0.0005-0.9995) with mean 0.499, indicating balanced prediction market activity. Temperature markets comprise 55.9% of records with bucket classifications, while non-temperature markets (precipitation, snow, hurricane) lack bucket values.
connection: bruin-playground-arsalan
tags:
  - forensics
  - prediction_markets
  - high_frequency_data
  - weather_trading
  - staging_layer
  - polymarket
  - paris_cdg_investigation
  - time_series
  - probability_data
  - temperature_analysis

materialization:
  type: table
  strategy: create+replace

depends:
  - polymarket_weather_raw.polymarket_prices
  - polymarket_weather_staging.markets_enriched

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: token_id
    type: VARCHAR
    description: CLOB token identifier (77-78 character blockchain address) uniquely identifying each tradeable outcome within Polymarket's conditional token framework
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: ts_utc
    type: TIMESTAMP
    description: Price tick timestamp in UTC, providing sub-hour precision for forensic analysis of trading behavior during suspected sensor tampering events
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: ts_local_paris
    type: DATETIME
    description: Price tick timestamp converted to Europe/Paris local time for correlation with weather station observations and market resolution times
    nullable: false
    checks:
      - name: not_null
  - name: market_id
    type: VARCHAR
    description: Polymarket inner-market identifier (6-7 characters) linking to specific prediction market questions within the broader weather betting universe
    nullable: false
    checks:
      - name: not_null
  - name: condition_id
    type: VARCHAR
    description: 66-character on-chain condition identifier for blockchain settlement, perfectly unique across all Polymarket conditional tokens
    nullable: false
    checks:
      - name: not_null
  - name: outcome_label
    type: VARCHAR
    description: Binary prediction outcome label ("Yes" or "No") representing the specific side of the market being priced
    nullable: false
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - "Yes"
          - "No"
  - name: question
    type: VARCHAR
    description: Full market question text (22-99 characters) describing the specific weather prediction being traded, typically binary temperature threshold questions
    nullable: false
    checks:
      - name: not_null
  - name: event_slug
    type: VARCHAR
    description: Parent event slug (21-68 characters) grouping related markets for the same weather event across multiple outcomes or time periods
    nullable: false
    checks:
      - name: not_null
  - name: series_slug
    type: VARCHAR
    description: Parent series slug (7-41 characters) identifying recurring market series like "paris-daily-weather", NULL for ~22.8% of records representing one-off events
  - name: city
    type: VARCHAR
    description: Geographic classification (3-12 characters) derived from market metadata, with Paris representing the primary focus of the sensor tampering investigation
    nullable: false
    checks:
      - name: not_null
  - name: period
    type: VARCHAR
    description: Temporal classification of the prediction window - daily markets dominate forensic analysis due to dependency on daily temperature maxima
    nullable: false
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - daily
          - monthly
          - seasonal
          - event
  - name: metric
    type: VARCHAR
    description: Weather phenomenon classification enabling focused analysis on temperature markets most susceptible to sensor manipulation
    nullable: false
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - temperature
          - precipitation
          - snow
          - hurricane
          - other
  - name: bucket_value_c
    type: INTEGER
    description: Temperature threshold in degrees Celsius for temperature bucket markets (NULL for 44.1% of records representing non-temperature phenomena), critical for resolution analysis
  - name: bucket_kind
    type: VARCHAR
    description: Temperature bucket boundary type for daily temperature markets - "point" (exact), "le" (X or below), "ge" (X or above), NULL for non-temperature markets
    checks:
      - name: accepted_values
        value:
          - point
          - le
          - ge
  - name: paris_daily_april_2026
    type: BOOLEAN
    description: Critical forensic flag isolating the 255,769 price ticks (39.4% of dataset) within Paris daily weather markets during the alleged CDG sensor tampering period
    nullable: false
    checks:
      - name: not_null
  - name: price
    type: DOUBLE
    description: Implied probability of outcome (0.0005-0.9995 range) representing market consensus on prediction likelihood, with extreme values indicating high confidence
    nullable: false
    checks:
      - name: not_null

@bruin */

WITH deduped AS (
    SELECT * EXCEPT(rn) FROM (
        SELECT
            token_id, ts_utc, condition_id, market_id, outcome_label,
            question, event_slug, series_slug, price,
            ROW_NUMBER() OVER (PARTITION BY token_id, ts_utc ORDER BY extracted_at DESC) AS rn
        FROM `bruin-playground-arsalan.polymarket_weather_raw.polymarket_prices`
        WHERE token_id IS NOT NULL AND ts_utc IS NOT NULL
    )
    WHERE rn = 1
)

SELECT
    p.token_id,
    p.ts_utc,
    DATETIME(p.ts_utc, 'Europe/Paris') AS ts_local_paris,
    p.market_id,
    p.condition_id,
    p.outcome_label,
    COALESCE(m.question, p.question) AS question,
    COALESCE(m.event_slug, p.event_slug) AS event_slug,
    COALESCE(m.series_slug, p.series_slug) AS series_slug,
    m.city,
    m.period,
    m.metric,
    m.bucket_value_c,
    m.bucket_kind,
    COALESCE(m.paris_daily_april_2026, FALSE) AS paris_daily_april_2026,
    p.price
FROM deduped p
LEFT JOIN `bruin-playground-arsalan.polymarket_weather_staging.markets_enriched` m
    ON p.market_id = m.market_id
