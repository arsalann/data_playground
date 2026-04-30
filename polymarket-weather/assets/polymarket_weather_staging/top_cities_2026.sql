/* @bruin

name: polymarket_weather_staging.top_cities_2026
type: bq.sql
description: |
  Ranks cities by Polymarket daily-temperature event volume in the
  Jan-Apr 2026 window and surfaces, per city, the most-frequent
  resolution-source URL plus the airport ICAO codes referenced by
  Polymarket for resolution. Drives the multi-city forensic
  investigation (Phase 0 of the multi-city plan): the top three
  rows determine which cities the raw + staging layers ingest stations
  for, and the ICAO column tells us which Meteostat station each city's
  Polymarket markets resolve on.

  One row per city. event_volume is summed after deduping markets to one
  row per event_id (since each event contains multiple bucket markets but
  exposes a single event_volume).
connection: bruin-playground-arsalan
tags:
  - prediction_markets
  - weather_data
  - staging_layer
  - polymarket
  - discovery

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
    description: City name as classified in markets_enriched
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: unique
  - name: rank
    type: INT64
    description: 1 = highest event_volume in window
    checks:
      - name: not_null
  - name: total_event_volume_usd
    type: FLOAT64
    description: Sum of event_volume across distinct events in 2026-01-01..2026-04-30
  - name: event_count
    type: INT64
    description: Distinct events resolving in the window
  - name: market_count
    type: INT64
    description: Distinct temperature-bucket markets resolving in the window
  - name: top_resolution_source
    type: VARCHAR
    description: Most-frequent resolution source URL across the city's markets in the window
  - name: top_resolution_market_count
    type: INT64
    description: Number of markets that cite top_resolution_source
  - name: primary_icao
    type: VARCHAR
    description: 4-letter airport ICAO parsed from top_resolution_source URL (last path segment)
  - name: distinct_icaos
    type: VARCHAR
    description: Comma-separated list of distinct ICAOs seen across the city's resolution sources

@bruin */

WITH temp_markets AS (
    SELECT
        market_id,
        event_id,
        city,
        bucket_kind,
        resolution_source,
        event_volume,
        end_local_date
    FROM `bruin-playground-arsalan.polymarket_weather_staging.markets_enriched`
    WHERE period = 'daily'
      AND metric = 'temperature'
      AND bucket_kind IS NOT NULL
      AND end_local_date BETWEEN DATE '2026-01-01' AND DATE '2026-04-30'
      AND city NOT IN ('Other', 'Global')
),
event_dedup AS (
    SELECT
        city,
        event_id,
        ANY_VALUE(event_volume) AS event_volume
    FROM temp_markets
    GROUP BY city, event_id
),
city_volume AS (
    SELECT
        city,
        SUM(event_volume) AS total_event_volume_usd,
        COUNT(DISTINCT event_id) AS event_count
    FROM event_dedup
    GROUP BY city
),
city_market_count AS (
    SELECT city, COUNT(DISTINCT market_id) AS market_count
    FROM temp_markets
    GROUP BY city
),
resolution_counts AS (
    SELECT
        city,
        resolution_source,
        COUNT(*) AS market_count,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY COUNT(*) DESC) AS rn
    FROM temp_markets
    WHERE resolution_source IS NOT NULL
    GROUP BY city, resolution_source
),
top_resolution AS (
    SELECT
        city,
        resolution_source AS top_resolution_source,
        market_count AS top_resolution_market_count,
        UPPER(REGEXP_EXTRACT(resolution_source, r'/([A-Za-z]{4})/?$')) AS primary_icao
    FROM resolution_counts
    WHERE rn = 1
),
city_icaos AS (
    SELECT
        city,
        STRING_AGG(DISTINCT icao, ',' ORDER BY icao) AS distinct_icaos
    FROM (
        SELECT
            city,
            UPPER(REGEXP_EXTRACT(resolution_source, r'/([A-Za-z]{4})/?$')) AS icao
        FROM temp_markets
        WHERE resolution_source IS NOT NULL
    )
    WHERE icao IS NOT NULL
    GROUP BY city
)
SELECT
    cv.city,
    ROW_NUMBER() OVER (ORDER BY cv.total_event_volume_usd DESC) AS rank,
    cv.total_event_volume_usd,
    cv.event_count,
    cmc.market_count,
    tr.top_resolution_source,
    tr.top_resolution_market_count,
    tr.primary_icao,
    ci.distinct_icaos
FROM city_volume cv
LEFT JOIN city_market_count cmc USING (city)
LEFT JOIN top_resolution tr USING (city)
LEFT JOIN city_icaos ci USING (city)
ORDER BY cv.total_event_volume_usd DESC
