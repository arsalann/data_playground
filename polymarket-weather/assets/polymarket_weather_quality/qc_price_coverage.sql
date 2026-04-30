/* @bruin

name: polymarket_weather_quality.qc_price_coverage
type: bq.sql
description: |
  Per-city Polymarket price-tick coverage across the multi-city investigation
  window 2026-01-01..2026-04-30. For each city, reports the number of distinct
  daily-temperature events, the number of markets with at least one CLOB tick,
  and the median fraction of event-window hours covered by ticks. Anomaly hour
  vs Yes-price overlays in the dashboard are only meaningful when this fraction
  is comfortably above zero.
connection: bruin-playground-arsalan
tags:
  - quality
  - polymarket
  - price_coverage
  - multi_city

materialization:
  type: table
  strategy: create+replace

depends:
  - polymarket_weather_staging.markets_enriched
  - polymarket_weather_staging.prices_enriched

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: city
    type: VARCHAR
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: events
    type: INTEGER
  - name: markets_total
    type: INTEGER
  - name: markets_with_ticks
    type: INTEGER
  - name: pct_markets_with_ticks
    type: DOUBLE
  - name: median_hourly_coverage
    type: DOUBLE
    description: Median across markets of (distinct hours with >=1 tick / 24)
  - name: status
    type: VARCHAR
    checks:
      - name: accepted_values
        value:
          - pass
          - warn
          - fail

@bruin */

WITH temp_markets AS (
    SELECT
        market_id,
        event_id,
        city,
        end_local_date
    FROM `bruin-playground-arsalan.polymarket_weather_staging.markets_enriched`
    WHERE period = 'daily'
      AND metric = 'temperature'
      AND bucket_kind IS NOT NULL
      AND end_local_date BETWEEN DATE '2026-01-01' AND DATE '2026-04-30'
      AND city IN ('Paris', 'London', 'Seoul', 'Toronto')
),

market_tick_count AS (
    SELECT
        m.city,
        m.market_id,
        m.end_local_date,
        COUNT(DISTINCT TIMESTAMP_TRUNC(p.ts_utc, HOUR)) AS hours_with_tick
    FROM temp_markets m
    LEFT JOIN `bruin-playground-arsalan.polymarket_weather_staging.prices_enriched` p
        ON p.market_id = m.market_id
       AND DATE(p.ts_utc) = m.end_local_date
    GROUP BY 1, 2, 3
),

market_coverage AS (
    SELECT
        city,
        market_id,
        hours_with_tick,
        ROUND(hours_with_tick / 24.0, 3) AS coverage_frac
    FROM market_tick_count
)

SELECT
    mc.city,
    COUNT(DISTINCT m.event_id) AS events,
    COUNT(DISTINCT mc.market_id) AS markets_total,
    COUNTIF(mc.hours_with_tick > 0) AS markets_with_ticks,
    ROUND(SAFE_DIVIDE(COUNTIF(mc.hours_with_tick > 0), COUNT(DISTINCT mc.market_id)), 3) AS pct_markets_with_ticks,
    APPROX_QUANTILES(mc.coverage_frac, 100)[OFFSET(50)] AS median_hourly_coverage,
    CASE
        WHEN APPROX_QUANTILES(mc.coverage_frac, 100)[OFFSET(50)] >= 0.5 THEN 'pass'
        WHEN APPROX_QUANTILES(mc.coverage_frac, 100)[OFFSET(50)] >= 0.1 THEN 'warn'
        ELSE 'fail'
    END AS status
FROM market_coverage mc
JOIN temp_markets m USING (market_id)
GROUP BY 1
