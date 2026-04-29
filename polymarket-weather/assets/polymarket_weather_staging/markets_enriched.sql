/* @bruin

name: polymarket_weather_staging.markets_enriched
type: bq.sql
description: |
  Deduplicated and classified weather prediction markets from Polymarket, central to the forensic investigation of suspected temperature sensor tampering at Paris-CDG airport during April 2026. This staging table transforms raw Polymarket market data into an analysis-ready format with weather-specific classifications.

  The deduplication strategy retains the freshest snapshot per market_id from raw.polymarket_markets (based on extracted_at DESC). The enrichment logic adds systematic classification features:

    - `city`                    derived from `series_slug` (most reliable signal for daily series),
                                falling back to question keyword matching for 54+ global cities
    - `period`                  temporal classification: daily / monthly / seasonal / event
    - `metric`                  weather phenomenon: temperature / precipitation / snow / hurricane / other
    - `bucket_value_c`          parsed integer temperature bucket (°C) for daily temperature markets,
                                e.g. 21 for "...be 21°C...", critical for resolution analysis
    - `bucket_kind`             bucket type: 'point' (exact), 'le' (X or below), 'ge' (X or higher)
    - `resolved_yes`            resolution outcome derived from outcome_prices for closed Yes/No markets
    - `paris_daily_april_2026`  boolean flag isolating the 319 markets within the suspected tampering window

  Statistical profile: 34,561 total markets with 96.5% resolution rate (33,342 closed). Only 10.4% of resolved markets settled as "Yes", reflecting the binary prediction market structure. The analysis focuses on Paris daily weather markets, particularly those resolving in April 2026 during the alleged sensor manipulation period.
connection: bruin-playground-arsalan
tags:
  - forensics
  - prediction_markets
  - weather_data
  - staging_layer
  - polymarket
  - temperature_analysis
  - paris_cdg_investigation

materialization:
  type: table
  strategy: create+replace

depends:
  - polymarket_weather_raw.polymarket_markets

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: market_id
    type: VARCHAR
    description: Polymarket inner-market identifier
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: unique
  - name: event_id
    type: VARCHAR
    description: Parent event identifier linking multiple related markets within the same event (e.g., different temperature buckets for the same day)
    checks:
      - name: not_null
  - name: event_slug
    type: VARCHAR
    description: Parent event slug
  - name: event_title
    type: VARCHAR
    description: Parent event title
  - name: series_slug
    type: VARCHAR
    description: Parent series slug (canonical city signal for daily series)
  - name: question
    type: VARCHAR
    description: Inner-market question text, typically phrased as a binary prediction (e.g., "Will the highest temperature in Paris on April 6, 2026 be 21°C or higher?")
    checks:
      - name: not_null
  - name: slug
    type: VARCHAR
    description: Inner-market slug used to parse temperature bucket values (e.g., "paris-daily-weather-april-6-2026-21c", "paris-daily-weather-april-6-2026-21corbelow")
    checks:
      - name: not_null
      - name: unique
  - name: resolution_source
    type: VARCHAR
    description: Free-text resolution source published by Polymarket
  - name: end_date
    type: TIMESTAMP
    description: Resolution / expiry timestamp
  - name: end_local_date
    type: DATE
    description: Resolution date in Europe/Paris local time
  - name: city
    type: VARCHAR
    description: City classification derived from series_slug or question
  - name: period
    type: VARCHAR
    description: Time-period classification derived from question text and event patterns
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
    description: Weather phenomenon classification based on question text analysis
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
    description: Parsed integer °C value for temperature bucket markets, NULL otherwise
  - name: bucket_kind
    type: VARCHAR
    description: Temperature bucket boundary type for daily temperature markets - NULL for non-temperature markets (~40.7% of records)
    checks:
      - name: accepted_values
        value:
          - point
          - le
          - ge
  - name: outcomes
    type: VARCHAR
    description: JSON outcome labels
  - name: outcome_prices
    type: VARCHAR
    description: JSON outcome prices (probabilities)
  - name: closed
    type: BOOLEAN
    description: Whether the market has resolved
  - name: resolved_yes
    type: BOOLEAN
    description: Whether the market resolved YES (price near 1.0 on closed Yes/No market)
  - name: condition_id
    type: VARCHAR
    description: 66-character on-chain condition identifier for linking to CLOB token trading data (perfectly unique across all markets)
    checks:
      - name: not_null
      - name: unique
  - name: clob_token_ids
    type: VARCHAR
    description: JSON array of CLOB token addresses
  - name: volume
    type: DOUBLE
    description: Lifetime market volume in USD (null for ~14.5% of markets, likely due to low activity)
  - name: event_volume
    type: DOUBLE
    description: Aggregated lifetime volume across all markets in the parent event (USD), higher than individual market volume
  - name: paris_daily_april_2026
    type: BOOLEAN
    description: Critical forensic analysis flag isolating the 319 Paris daily weather markets resolving in April 2026 during the alleged CDG sensor tampering period (Apr 6 & 15 specifically targeted). Null when end_date is missing.
  - name: extracted_at
    type: TIMESTAMP
    description: Snapshot timestamp from raw.polymarket_markets indicating data freshness (single extraction time for deduplication)
    checks:
      - name: not_null

@bruin */

WITH deduped AS (
    SELECT * EXCEPT(rn) FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY extracted_at DESC) AS rn
        FROM `bruin-playground-arsalan.polymarket_weather_raw.polymarket_markets`
        WHERE market_id IS NOT NULL
    )
    WHERE rn = 1
),

classified AS (
    SELECT
        *,
        -- City derivation
        CASE
            WHEN series_slug LIKE '%-daily-weather' THEN
                INITCAP(REPLACE(REGEXP_REPLACE(series_slug, '-daily-weather$', ''), '-', ' '))
            WHEN LOWER(question) LIKE '%paris%' THEN 'Paris'
            WHEN LOWER(question) LIKE '%new york%' OR LOWER(question) LIKE '%nyc%' THEN 'New York'
            WHEN LOWER(question) LIKE '%los angeles%' OR LOWER(question) LIKE '% la %' THEN 'Los Angeles'
            WHEN LOWER(question) LIKE '%london%' THEN 'London'
            WHEN LOWER(question) LIKE '%chicago%' THEN 'Chicago'
            WHEN LOWER(question) LIKE '%tokyo%' THEN 'Tokyo'
            WHEN LOWER(question) LIKE '%berlin%' THEN 'Berlin'
            WHEN LOWER(question) LIKE '%madrid%' THEN 'Madrid'
            WHEN LOWER(question) LIKE '%global%' OR LOWER(question) LIKE '%temperature increase%' THEN 'Global'
            ELSE 'Other'
        END AS city,

        -- Period classification
        CASE
            WHEN series_slug LIKE '%-daily-weather' THEN 'daily'
            WHEN REGEXP_CONTAINS(LOWER(question), r'\bon\s+\w+\s+\d+\b') THEN 'daily'
            WHEN REGEXP_CONTAINS(LOWER(event_title), r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4}') THEN 'monthly'
            WHEN LOWER(question) LIKE '%hurricane season%' OR LOWER(question) LIKE '%fire season%' THEN 'seasonal'
            WHEN LOWER(question) LIKE '%hottest month%' OR LOWER(question) LIKE '%coldest month%' THEN 'seasonal'
            ELSE 'event'
        END AS period,

        -- Metric classification
        CASE
            WHEN LOWER(question) LIKE '%temperature%' OR LOWER(question) LIKE '%°c%' OR LOWER(question) LIKE '%celsius%'
                 OR LOWER(question) LIKE '%hottest%' OR LOWER(question) LIKE '%coldest%' OR LOWER(question) LIKE '%heat wave%' THEN 'temperature'
            WHEN LOWER(question) LIKE '%snow%' THEN 'snow'
            WHEN LOWER(question) LIKE '%rain%' OR LOWER(question) LIKE '%precip%' OR LOWER(question) LIKE '%rainfall%' THEN 'precipitation'
            WHEN LOWER(question) LIKE '%hurricane%' OR LOWER(question) LIKE '%cyclone%' OR LOWER(question) LIKE '%typhoon%' THEN 'hurricane'
            ELSE 'other'
        END AS metric,

        -- Bucket parsing for °C markets
        CASE
            WHEN REGEXP_CONTAINS(slug, r'\d+corbelow$') THEN 'le'
            WHEN REGEXP_CONTAINS(slug, r'\d+corhigher$') THEN 'ge'
            WHEN REGEXP_CONTAINS(slug, r'\d+c$') THEN 'point'
            ELSE NULL
        END AS bucket_kind,
        SAFE_CAST(REGEXP_EXTRACT(slug, r'(\d+)c(?:orbelow|orhigher)?$') AS INT64) AS bucket_value_c,

        -- Outcome resolution: closed Yes/No markets where outcome_prices = ["1","0"] resolved YES
        CASE
            WHEN closed = TRUE
                 AND outcome_prices LIKE '%"1"%'
                 AND REGEXP_EXTRACT(outcome_prices, r'^\["([01](?:\.\d+)?)"') = '1'
            THEN TRUE
            WHEN closed = TRUE THEN FALSE
            ELSE NULL
        END AS resolved_yes,

        DATE(end_date, 'Europe/Paris') AS end_local_date
    FROM deduped
)

SELECT
    market_id,
    event_id,
    event_slug,
    event_title,
    series_slug,
    question,
    slug,
    resolution_source,
    end_date,
    end_local_date,
    city,
    period,
    metric,
    bucket_value_c,
    bucket_kind,
    outcomes,
    outcome_prices,
    closed,
    resolved_yes,
    condition_id,
    clob_token_ids,
    volume,
    event_volume,
    series_slug = 'paris-daily-weather'
        AND end_local_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-30' AS paris_daily_april_2026,
    extracted_at
FROM classified
