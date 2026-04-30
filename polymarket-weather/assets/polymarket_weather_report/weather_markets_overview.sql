/* @bruin

name: polymarket_weather_report.weather_markets_overview
type: bq.sql
description: |
  City × month × period aggregate of weather-market activity for the dashboard's
  broader-context section. Counts events, distinct markets and total trading
  volume so Paris can be ranked against London / NYC / Tokyo / etc., and so the
  reader can see how concentrated the global weather-betting universe is.

  Key analytical insights: 54+ cities tracked, daily temperature markets dominate volume,
  high variance in per-event activity ($8k-$2.7M), enables forensic comparison of
  Paris April 2026 activity against seasonal baselines and peer cities.
connection: bruin-playground-arsalan
tags:
  - forensics
  - prediction_markets
  - weather_betting
  - dashboard_feed
  - comparative_analytics
  - paris_investigation

materialization:
  type: table
  strategy: create+replace

depends:
  - polymarket_weather_staging.markets_enriched

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: city
    type: VARCHAR
    description: Standardized city name from series_slug or question text. Covers 54+ global cities including Paris (investigation target), London, NYC, Tokyo, Berlin, 'Other' for unmatched.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: period
    type: VARCHAR
    description: Temporal classification - daily/monthly/seasonal/event. Daily markets are focus of tampering investigation.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - daily
          - monthly
          - seasonal
          - event
  - name: end_month
    type: DATE
    description: Truncated month (1st day) from resolution dates. Spans 2025-01 through 2026-12. Critical for April 2026 tampering window context.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: events
    type: INTEGER
    description: Count of distinct weather events. Each spawns multiple binary markets for different thresholds. Range 1-44 per bucket.
    checks:
      - name: not_null
  - name: markets
    type: INTEGER
    description: Total individual prediction markets. Always >= events since each event has multiple binary markets.
    checks:
      - name: not_null
  - name: total_volume
    type: DOUBLE
    description: Sum of lifetime trading volume (USD) across all markets in bucket. Range $8k-$14.7M. One NULL likely zero-volume market.
  - name: avg_event_volume
    type: DOUBLE
    description: Mean trading volume (USD) per weather event. Indicates typical monetary scale, useful for identifying manipulation.
  - name: total_event_volume
    type: DOUBLE
    description: Sum of distinct event-level volumes. Differs from total_volume due to aggregation methodology. Critical for anomaly detection.

@bruin */

SELECT
    city,
    period,
    DATE_TRUNC(end_local_date, MONTH) AS end_month,
    COUNT(DISTINCT event_id) AS events,
    COUNT(*) AS markets,
    SUM(volume) AS total_volume,
    AVG(event_volume) AS avg_event_volume,
    SUM(DISTINCT event_volume) AS total_event_volume
FROM `bruin-playground-arsalan.polymarket_weather_staging.markets_enriched`
WHERE end_local_date IS NOT NULL
  AND end_local_date BETWEEN DATE '2024-01-01' AND DATE '2027-01-01'
GROUP BY city, period, end_month
ORDER BY end_month, total_volume DESC NULLS LAST
