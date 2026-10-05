/* @bruin
name: staging.flood_events
type: bq.sql
connection: bruin-playground-arsalan
description: |
  One row per GDACS flood event with a parsed affected-area polygon and
  summarised Sendai-framework impact reports.
  - Deduplicates appended raw rows on event_id (latest extracted_at wins).
  - Normalises alert-level casing (GDACS returns e.g. "green" and "ORANGE" for some events).
  - Parses the GDACS "Affected area" GeoJSON into a GEOGRAPHY (make_valid).
  - Impact figures: GDACS publishes overlapping, cumulative reports per region.
    For each (country, region, impact type) the maximum reported value is kept,
    then regions are summed. This avoids double counting repeated updates but can
    undercount if a region reports distinct incidents separately.

depends:
  - raw.gdacs_flood_events

materialization:
  type: table
  strategy: create+replace

columns:
  - name: event_id
    type: INTEGER
    description: GDACS event identifier.
    primary_key: true
    nullable: false
    checks:
      - name: unique
      - name: not_null
  - name: event_name
    type: VARCHAR
    description: GDACS display name.
  - name: country
    type: VARCHAR
    description: Primary country.
  - name: iso3
    type: VARCHAR
    description: Primary country ISO3 code.
  - name: alert_level
    type: VARCHAR
    description: GDACS alert level (Green, Orange, Red).
    checks:
      - name: accepted_values
        value: [Green, Orange, Red]
  - name: alert_rank
    type: INTEGER
    description: 1 = Green, 2 = Orange, 3 = Red.
  - name: start_date
    type: DATE
    description: Event start date (UTC).
  - name: end_date
    type: DATE
    description: Event end or latest-update date (UTC).
  - name: duration_days
    type: INTEGER
    description: Inclusive event length (days).
  - name: centroid_lon
    type: DOUBLE
    description: Event centroid longitude (WGS84 degrees).
  - name: centroid_lat
    type: DOUBLE
    description: Event centroid latitude (WGS84 degrees).
  - name: affected_area
    type: GEOGRAPHY
    description: GDACS affected-area polygon; NULL if GDACS published none or it failed to parse.
  - name: affected_area_km2
    type: DOUBLE
    description: Area of the affected-area polygon (km2).
  - name: deaths
    type: INTEGER
    description: Reported fatalities (people), summed across regions after taking the max per region.
  - name: displaced
    type: INTEGER
    description: Reported displaced / evacuated people, same aggregation as deaths.
  - name: source
    type: VARCHAR
    description: Upstream detection source (e.g. GLOFAS).
  - name: report_url
    type: VARCHAR
    description: GDACS event report URL.
@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.gdacs_flood_events
    WHERE event_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY extracted_at DESC) = 1
),

impact_rows AS (
    SELECT
        d.event_id,
        JSON_VALUE(item, '$.sendainame') AS impact_type,
        JSON_VALUE(item, '$.country') AS impact_country,
        JSON_VALUE(item, '$.region') AS impact_region,
        SAFE_CAST(JSON_VALUE(item, '$.sendaivalue') AS INT64) AS impact_value
    FROM deduped AS d,
        UNNEST(JSON_QUERY_ARRAY(d.sendai_json)) AS item
    WHERE d.sendai_json IS NOT NULL
),

impact_by_region AS (
    SELECT event_id, impact_type, impact_country, impact_region, MAX(impact_value) AS impact_value
    FROM impact_rows
    WHERE impact_type IN ('death', 'displaced')
    GROUP BY 1, 2, 3, 4
),

impacts AS (
    SELECT
        event_id,
        SUM(IF(impact_type = 'death', impact_value, 0)) AS deaths,
        SUM(IF(impact_type = 'displaced', impact_value, 0)) AS displaced
    FROM impact_by_region
    GROUP BY 1
),

parsed AS (
    SELECT
        d.* REPLACE (INITCAP(LOWER(d.alert_level)) AS alert_level),
        SAFE.ST_GEOGFROMGEOJSON(d.affected_geojson, make_valid => TRUE) AS affected_area
    FROM deduped AS d
)

SELECT
    p.event_id,
    p.event_name,
    p.country,
    p.iso3,
    p.alert_level,
    CASE p.alert_level WHEN 'Red' THEN 3 WHEN 'Orange' THEN 2 ELSE 1 END AS alert_rank,
    DATE(p.from_date) AS start_date,
    DATE(p.to_date) AS end_date,
    DATE_DIFF(DATE(p.to_date), DATE(p.from_date), DAY) + 1 AS duration_days,
    p.centroid_lon,
    p.centroid_lat,
    p.affected_area,
    ROUND(ST_AREA(p.affected_area) / 1e6, 1) AS affected_area_km2,
    COALESCE(i.deaths, 0) AS deaths,
    COALESCE(i.displaced, 0) AS displaced,
    p.source,
    p.report_url
FROM parsed AS p
LEFT JOIN impacts AS i USING (event_id)
