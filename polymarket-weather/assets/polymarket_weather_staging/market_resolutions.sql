/* @bruin

name: polymarket_weather_staging.market_resolutions
type: bq.sql
description: |
  Forensic counterfactual resolution analysis for Paris daily temperature markets during the alleged
  sensor tampering period (April 2026). This table is central to investigating the Paris-CDG
  temperature sensor allegations by comparing actual Polymarket resolutions with what would have
  happened under alternative weather data sources.

  For each event, we compute:
    - `winning_bucket_observed`           the bucket that actually resolved YES on Polymarket
                                          (identified by closing price ≈ 1.0)
    - `bucket_<station>`                  the counterfactual winning bucket if resolved against
                                          each alternative weather station or gridded data
    - `disagreement_count_vs_cdg`         number of alternative sources disagreeing with CDG
    - `disagreement_count_vs_observed`    number of sources disagreeing with actual resolution

  The investigation focuses on CDG sensor anomalies on 2026-04-06 and 2026-04-15 that allegedly
  generated ~$34k in trading profits. Verified from the warehouse during Phase 5: across 29 April
  events, 0 days had every alternative source agreeing with Polymarket; 13 had a majority (≥5 of 7)
  of alternative sources disagreeing; 19 had CDG itself disagreeing with ≥4 alternatives. Apr 6
  resolved 21°C with CDG max=21.0 vs peers 16.6-18.0; Apr 15 resolved 22°C with CDG max=18.0 vs
  peers 17.5-18.1 (no station supports the 22°C resolution). The resolution-source URL switched from
  Wunderground LFPG (Charles de Gaulle) to LFPB (Bonneuil-en-France / Le Bourget) starting 2026-04-19.

  Temperature buckets follow Polymarket's scheme: ≤14°C, 15-23°C (individual integer buckets), ≥24°C.
  Station daily maximums are rounded to nearest integer and mapped to the closest available bucket.
connection: bruin-playground-arsalan
tags:
  - domain:finance
  - data_type:fact_table
  - sensitivity:public
  - pipeline_role:staging
  - analysis_type:forensic_investigation
  - market_type:prediction_market
  - weather_data:temperature
  - temporal_scope:april_2026
  - investigation:sensor_tampering

materialization:
  type: table
  strategy: create+replace

depends:
  - polymarket_weather_staging.temperature_daily
  - polymarket_weather_staging.markets_enriched
  - polymarket_weather_staging.prices_enriched

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: event_local_date
    type: DATE
    description: Event resolution date in Europe/Paris local time (daily temperature markets resolve based on this calendar date)
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: event_id
    type: VARCHAR
    description: Polymarket event identifier (6-character alphanumeric string, unique per day during investigation period)
    nullable: false
    checks:
      - name: not_null
  - name: event_slug
    type: VARCHAR
    description: Polymarket event slug (URL-friendly identifier, typically ~45 characters for Paris daily temperature events)
    nullable: false
    checks:
      - name: not_null
  - name: event_title
    type: VARCHAR
    description: Polymarket event title (human-readable market name, typically ~40 characters for temperature events)
    nullable: false
    checks:
      - name: not_null
  - name: event_resolution_source_url
    type: VARCHAR
    description: Official resolution source URL (Weather Underground station page - critical for investigation as CDG vs. Le Bourget determines tampering exposure)
    nullable: false
    checks:
      - name: not_null
  - name: total_event_volume
    type: DOUBLE
    description: Total event-level lifetime trading volume in USD (ranges ~$20k-$780k, higher on suspected tampering days)
    nullable: false
    checks:
      - name: not_null
  - name: winning_bucket_observed
    type: INTEGER
    description: The integer-°C bucket that resolved YES on Polymarket (14-24 range, where 14=≤14°C, 24=≥24°C, 15-23 are point buckets)
    nullable: false
    checks:
      - name: not_null
  - name: winning_bucket_kind_observed
    type: VARCHAR
    description: Bucket type for observed winner - 'point' (exact integer bucket), 'le' (≤14°C boundary), 'ge' (≥24°C boundary)
    nullable: false
    checks:
      - name: not_null
  - name: temp_max_cdg
    type: DOUBLE
    description: Daily max temperature at Paris-CDG (Meteostat 07157, the suspected sensor in tampering investigation)
  - name: temp_max_orly
    type: DOUBLE
    description: Daily max temperature at Paris-Orly (Meteostat 07149)
  - name: temp_max_le_bourget
    type: DOUBLE
    description: Daily max temperature at Paris-Le Bourget (Meteostat 07150)
  - name: temp_max_montsouris
    type: DOUBLE
    description: Daily max temperature at Paris-Montsouris (Meteostat 07156)
  - name: temp_max_villacoublay
    type: DOUBLE
    description: Daily max temperature at Villacoublay (Meteostat 07147)
  - name: temp_max_trappes
    type: DOUBLE
    description: Daily max temperature at Trappes (Meteostat 07145)
  - name: temp_max_grid
    type: DOUBLE
    description: Daily max temperature from Open-Meteo grid at Paris centre (independent gridded reanalysis, not station-based). Null when grid data is missing for the day.
  - name: bucket_cdg
    type: INTEGER
    description: Counterfactual winning bucket if the event were settled on CDG (rounded daily max)
  - name: bucket_orly
    type: INTEGER
    description: Counterfactual winning bucket if the event were settled on Orly
  - name: bucket_le_bourget
    type: INTEGER
    description: Counterfactual winning bucket if the event were settled on Le Bourget
  - name: bucket_montsouris
    type: INTEGER
    description: Counterfactual winning bucket if the event were settled on Montsouris
  - name: bucket_villacoublay
    type: INTEGER
    description: Counterfactual winning bucket if the event were settled on Villacoublay
  - name: bucket_trappes
    type: INTEGER
    description: Counterfactual winning bucket if the event were settled on Trappes
  - name: bucket_grid
    type: INTEGER
    description: Counterfactual winning bucket if event were settled on Open-Meteo grid (independent alternative to station data). Null when grid data is missing for the day.
  - name: disagreement_count_vs_cdg
    type: INTEGER
    description: Count of alternative sources (5 stations + grid) whose winning bucket differs from CDG (key tampering indicator)
    nullable: false
    checks:
      - name: not_null
  - name: disagreement_count_vs_observed
    type: INTEGER
    description: Count of all sources (6 stations + grid) whose winning bucket differs from actual Polymarket resolution
    nullable: false
    checks:
      - name: not_null

@bruin */

WITH events AS (
    SELECT
        event_id,
        ANY_VALUE(event_slug) AS event_slug,
        ANY_VALUE(event_title) AS event_title,
        ANY_VALUE(resolution_source) AS event_resolution_source_url,
        end_local_date AS event_local_date,
        SUM(volume) AS total_event_volume
    FROM `bruin-playground-arsalan.polymarket_weather_staging.markets_enriched`
    WHERE paris_daily_april_2026 = TRUE
      AND bucket_value_c IS NOT NULL
    GROUP BY event_id, end_local_date
),

resolved_buckets AS (
    -- The "Yes" outcome whose final price is ~1.0 marks the winning bucket
    SELECT
        m.event_id,
        ARRAY_AGG(STRUCT(m.bucket_value_c AS bucket_value_c, m.bucket_kind AS bucket_kind, m.market_id AS market_id) ORDER BY m.bucket_value_c) AS bucket_list,
        ARRAY_AGG(STRUCT(m.bucket_value_c AS bucket_value_c, m.bucket_kind AS bucket_kind) ORDER BY m.volume DESC LIMIT 1) AS most_traded
    FROM `bruin-playground-arsalan.polymarket_weather_staging.markets_enriched` m
    WHERE m.paris_daily_april_2026 = TRUE
    GROUP BY m.event_id
),

last_yes_prices AS (
    -- The latest "Yes" tick per market for Paris April events
    SELECT
        market_id,
        ANY_VALUE(price HAVING MAX ts_utc) AS last_yes_price
    FROM `bruin-playground-arsalan.polymarket_weather_staging.prices_enriched`
    WHERE paris_daily_april_2026 = TRUE
      AND outcome_label = 'Yes'
    GROUP BY market_id
),

observed_winners AS (
    SELECT
        m.event_id,
        ARRAY_AGG(STRUCT(m.bucket_value_c AS v, m.bucket_kind AS k) ORDER BY p.last_yes_price DESC LIMIT 1)[OFFSET(0)] AS w
    FROM `bruin-playground-arsalan.polymarket_weather_staging.markets_enriched` m
    JOIN last_yes_prices p USING (market_id)
    WHERE m.paris_daily_april_2026 = TRUE
    GROUP BY m.event_id
),

station_max AS (
    SELECT
        local_date,
        MAX(IF(source_id = '07157', temp_max_c, NULL))   AS temp_max_cdg,
        MAX(IF(source_id = '07149', temp_max_c, NULL))   AS temp_max_orly,
        MAX(IF(source_id = '07150', temp_max_c, NULL))   AS temp_max_le_bourget,
        MAX(IF(source_id = '07156', temp_max_c, NULL))   AS temp_max_montsouris,
        MAX(IF(source_id = '07147', temp_max_c, NULL))   AS temp_max_villacoublay,
        MAX(IF(source_id = '07145', temp_max_c, NULL))   AS temp_max_trappes,
        MAX(IF(source = 'openmeteo_grid', temp_max_c, NULL)) AS temp_max_grid
    FROM `bruin-playground-arsalan.polymarket_weather_staging.temperature_daily`
    WHERE local_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-30'
    GROUP BY local_date
),

bucket_map AS (
    -- For every event, list of available buckets so we can pick the matching one
    SELECT event_id, bucket_list FROM resolved_buckets
)

SELECT
    e.event_local_date,
    e.event_id,
    e.event_slug,
    e.event_title,
    e.event_resolution_source_url,
    e.total_event_volume,

    ow.w.v        AS winning_bucket_observed,
    ow.w.k        AS winning_bucket_kind_observed,

    s.temp_max_cdg,
    s.temp_max_orly,
    s.temp_max_le_bourget,
    s.temp_max_montsouris,
    s.temp_max_villacoublay,
    s.temp_max_trappes,
    s.temp_max_grid,

    -- Map each station's rounded daily max to the closest available bucket value.
    -- Buckets are integer °C 14..24 with 14 = "≤14" and 24 = "≥24" caps.
    LEAST(GREATEST(CAST(ROUND(s.temp_max_cdg)         AS INT64), 14), 24) AS bucket_cdg,
    LEAST(GREATEST(CAST(ROUND(s.temp_max_orly)        AS INT64), 14), 24) AS bucket_orly,
    LEAST(GREATEST(CAST(ROUND(s.temp_max_le_bourget)  AS INT64), 14), 24) AS bucket_le_bourget,
    LEAST(GREATEST(CAST(ROUND(s.temp_max_montsouris)  AS INT64), 14), 24) AS bucket_montsouris,
    LEAST(GREATEST(CAST(ROUND(s.temp_max_villacoublay)AS INT64), 14), 24) AS bucket_villacoublay,
    LEAST(GREATEST(CAST(ROUND(s.temp_max_trappes)     AS INT64), 14), 24) AS bucket_trappes,
    LEAST(GREATEST(CAST(ROUND(s.temp_max_grid)        AS INT64), 14), 24) AS bucket_grid,

    -- Disagreements vs CDG (count alt sources where the rounded bucket differs)
    (CASE WHEN ROUND(s.temp_max_orly)         != ROUND(s.temp_max_cdg) THEN 1 ELSE 0 END
   + CASE WHEN ROUND(s.temp_max_le_bourget)   != ROUND(s.temp_max_cdg) THEN 1 ELSE 0 END
   + CASE WHEN ROUND(s.temp_max_montsouris)   != ROUND(s.temp_max_cdg) THEN 1 ELSE 0 END
   + CASE WHEN ROUND(s.temp_max_villacoublay) != ROUND(s.temp_max_cdg) THEN 1 ELSE 0 END
   + CASE WHEN ROUND(s.temp_max_trappes)      != ROUND(s.temp_max_cdg) THEN 1 ELSE 0 END
   + CASE WHEN ROUND(s.temp_max_grid)         != ROUND(s.temp_max_cdg) THEN 1 ELSE 0 END
    ) AS disagreement_count_vs_cdg,

    -- Disagreements vs observed Polymarket resolution
    (CASE WHEN ROUND(s.temp_max_cdg)          != ow.w.v THEN 1 ELSE 0 END
   + CASE WHEN ROUND(s.temp_max_orly)         != ow.w.v THEN 1 ELSE 0 END
   + CASE WHEN ROUND(s.temp_max_le_bourget)   != ow.w.v THEN 1 ELSE 0 END
   + CASE WHEN ROUND(s.temp_max_montsouris)   != ow.w.v THEN 1 ELSE 0 END
   + CASE WHEN ROUND(s.temp_max_villacoublay) != ow.w.v THEN 1 ELSE 0 END
   + CASE WHEN ROUND(s.temp_max_trappes)      != ow.w.v THEN 1 ELSE 0 END
   + CASE WHEN ROUND(s.temp_max_grid)         != ow.w.v THEN 1 ELSE 0 END
    ) AS disagreement_count_vs_observed
FROM events e
LEFT JOIN station_max s ON s.local_date = e.event_local_date
LEFT JOIN observed_winners ow USING (event_id)
ORDER BY e.event_local_date
