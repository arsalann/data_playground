/* @bruin
name: fifa_staging.live_stadiums
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Latest stadium reference rows from the worldcup26.ir live tracker source.
  Deduplicates by stadium identifier and keeps the newest extracted_at snapshot.

depends:
  - fifa_raw.live_stadiums

materialization:
  type: table
  strategy: create+replace

columns:
  - name: stadium_id
    type: VARCHAR
    description: worldcup26.ir stadium identifier.
    primary_key: true
    nullable: false
  - name: stadium_name
    type: VARCHAR
    description: Stadium English display name.
  - name: fifa_stadium_name
    type: VARCHAR
    description: FIFA event-time stadium display name.
  - name: venue_city
    type: VARCHAR
    description: Host city English display name.
  - name: venue_country
    type: VARCHAR
    description: Host country English display name.
  - name: capacity
    type: INTEGER
    description: Stadium listed seating capacity in seats.
  - name: region
    type: VARCHAR
    description: Source geographic region label.
  - name: source_extracted_at
    type: TIMESTAMP
    description: UTC extraction timestamp of the retained source row.

@bruin */

WITH deduped AS (
  SELECT *
  FROM `bruin-playground-arsalan.fifa_raw.live_stadiums`
  WHERE stadium_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY stadium_id ORDER BY extracted_at DESC) = 1
)
SELECT
  stadium_id,
  stadium_name_en AS stadium_name,
  fifa_stadium_name,
  city_en AS venue_city,
  country_en AS venue_country,
  capacity,
  region,
  extracted_at AS source_extracted_at
FROM deduped
ORDER BY venue_country, venue_city
