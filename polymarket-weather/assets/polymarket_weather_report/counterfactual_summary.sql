/* @bruin

name: polymarket_weather_report.counterfactual_summary
type: bq.sql
description: |
  Forensic counterfactual analysis table showing how Paris temperature markets would have resolved
  under alternative weather data sources during the April 2026 sensor tampering investigation.

  This table is the long-format transformation of market_resolutions, creating one row per
  (event_local_date, alt_source) combination. Critical for identifying resolution source sensitivity
  and potential market manipulation. Each row shows:
  - What temperature bucket the event would have resolved to using each weather station/grid
  - Whether that counterfactual resolution agrees with the actual Polymarket outcome
  - Market volume context to assess financial impact of source selection

  Used primarily by the dashboard's Section 3 counterfactual analysis to visualize disagreement
  patterns across the 6 Paris weather stations plus independent gridded reanalysis during the
  period when CDG sensor anomalies allegedly generated ~$34k in trading profits.

  Temperature data sources span different microclimates: airport stations (CDG, Orly, Le Bourget),
  urban reference (Montsouris), military (Villacoublay), semi-rural (Trappes), and grid-based
  reanalysis (Open-Meteo). Disagreements between sources highlight resolution fragility.
connection: bruin-playground-arsalan
tags:
  - domain:finance
  - data_type:fact_table
  - analysis_type:forensic_investigation
  - sensitivity:public
  - pipeline_role:mart
  - market_type:prediction_market
  - weather_data:temperature
  - temporal_scope:april_2026
  - investigation:sensor_tampering
  - format:long_panel

materialization:
  type: table
  strategy: create+replace

depends:
  - polymarket_weather_staging.market_resolutions

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: event_local_date
    type: DATE
    description: Event resolution date in Europe/Paris local time (market calendar date, not settlement timestamp)
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: alt_source
    type: VARCHAR
    description: Alternative weather data source name - 6 Paris-area stations plus independent gridded reanalysis
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - CDG
          - Orly
          - Le Bourget
          - Montsouris
          - Villacoublay
          - Trappes
          - Open-Meteo grid
  - name: temp_max_c
    type: DOUBLE
    description: Daily max temperature at this source in degrees Celsius (null when station/grid data unavailable for the day)
  - name: bucket
    type: INTEGER
    description: Counterfactual winning bucket (14-24 integer range) if event were resolved against this source (null when temp_max_c is null)
  - name: winning_bucket_observed
    type: INTEGER
    description: The integer-°C bucket that actually resolved YES on Polymarket (14-24 range, where 14=≤14°C, 24=≥24°C)
    nullable: false
    checks:
      - name: not_null
  - name: agrees_with_observed
    type: BOOLEAN
    description: Whether counterfactual bucket matches actual Polymarket resolution (null when bucket is null, critical tampering indicator when false)
  - name: total_event_volume
    type: DOUBLE
    description: Total event-level lifetime trading volume in USD (market size context for assessing financial impact of source disagreements)
    nullable: false
    checks:
      - name: not_null
  - name: event_resolution_source_url
    type: VARCHAR
    description: Official Polymarket resolution source URL (Weather Underground station page - CDG vs. Le Bourget URLs indicate tampering exposure)
    nullable: false
    checks:
      - name: not_null

@bruin */

WITH base AS (
    SELECT
        event_local_date,
        winning_bucket_observed,
        total_event_volume,
        event_resolution_source_url,
        temp_max_cdg,         bucket_cdg,
        temp_max_orly,        bucket_orly,
        temp_max_le_bourget,  bucket_le_bourget,
        temp_max_montsouris,  bucket_montsouris,
        temp_max_villacoublay,bucket_villacoublay,
        temp_max_trappes,     bucket_trappes,
        temp_max_grid,        bucket_grid
    FROM `bruin-playground-arsalan.polymarket_weather_staging.market_resolutions`
),

unioned AS (
    SELECT event_local_date, 'CDG'             AS alt_source, temp_max_cdg          AS temp_max_c, bucket_cdg          AS bucket, winning_bucket_observed, total_event_volume, event_resolution_source_url FROM base
    UNION ALL
    SELECT event_local_date, 'Orly',           temp_max_orly,         bucket_orly,         winning_bucket_observed, total_event_volume, event_resolution_source_url FROM base
    UNION ALL
    SELECT event_local_date, 'Le Bourget',     temp_max_le_bourget,   bucket_le_bourget,   winning_bucket_observed, total_event_volume, event_resolution_source_url FROM base
    UNION ALL
    SELECT event_local_date, 'Montsouris',     temp_max_montsouris,   bucket_montsouris,   winning_bucket_observed, total_event_volume, event_resolution_source_url FROM base
    UNION ALL
    SELECT event_local_date, 'Villacoublay',   temp_max_villacoublay, bucket_villacoublay, winning_bucket_observed, total_event_volume, event_resolution_source_url FROM base
    UNION ALL
    SELECT event_local_date, 'Trappes',        temp_max_trappes,      bucket_trappes,      winning_bucket_observed, total_event_volume, event_resolution_source_url FROM base
    UNION ALL
    SELECT event_local_date, 'Open-Meteo grid',temp_max_grid,         bucket_grid,         winning_bucket_observed, total_event_volume, event_resolution_source_url FROM base
)

SELECT
    event_local_date,
    alt_source,
    temp_max_c,
    bucket,
    winning_bucket_observed,
    bucket = winning_bucket_observed AS agrees_with_observed,
    total_event_volume,
    event_resolution_source_url
FROM unioned
ORDER BY event_local_date, alt_source
