/* @bruin

name: polymarket_weather_quality.qc_resolution_mapping
type: bq.sql
description: |
  Confirms that each investigation city has a primary station present in the
  ingested temperature_hourly panel and that its ICAO matches the most-frequent
  Polymarket resolution-source URL for the city. Surfaces any mismatch as a
  fail row to be reviewed before trusting the primary-station overlay.
connection: bruin-playground-arsalan
tags:
  - quality
  - polymarket
  - resolution_mapping

materialization:
  type: table
  strategy: create+replace

depends:
  - polymarket_weather_staging.top_cities_2026
  - polymarket_weather_raw.station_hourly

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
  - name: rank
    type: INT64
  - name: polymarket_primary_icao
    type: VARCHAR
    description: ICAO parsed from Polymarket's most-frequent resolution_source URL
  - name: ingested_primary_station_id
    type: VARCHAR
  - name: ingested_primary_icao
    type: VARCHAR
  - name: ingested_primary_name
    type: VARCHAR
  - name: status
    type: VARCHAR
    checks:
      - name: accepted_values
        value:
          - pass
          - warn
          - fail

@bruin */

WITH polymarket_top AS (
    SELECT
        city,
        rank,
        primary_icao
    FROM `bruin-playground-arsalan.polymarket_weather_staging.top_cities_2026`
    WHERE city IN ('Paris', 'London', 'Seoul', 'Toronto')
),

ingested_primary AS (
    SELECT
        city,
        ANY_VALUE(station_id) AS station_id,
        ANY_VALUE(icao) AS icao,
        ANY_VALUE(station_name) AS station_name
    FROM `bruin-playground-arsalan.polymarket_weather_raw.station_hourly`
    WHERE role = 'primary'
    GROUP BY city
)

SELECT
    pt.city,
    pt.rank,
    pt.primary_icao AS polymarket_primary_icao,
    ip.station_id   AS ingested_primary_station_id,
    ip.icao         AS ingested_primary_icao,
    ip.station_name AS ingested_primary_name,
    CASE
        WHEN ip.station_id IS NULL THEN 'fail'
        WHEN UPPER(ip.icao) = UPPER(pt.primary_icao) THEN 'pass'
        ELSE 'warn'
    END AS status
FROM polymarket_top pt
LEFT JOIN ingested_primary ip USING (city)
ORDER BY pt.rank
