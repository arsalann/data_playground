/* @bruin
name: staging.crime_events
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Cleans and deduplicates Toronto Community Safety Indicator rows into one
  typed offence/victim-level table with temporal dimensions, standardized
  category labels, neighbourhood keys, and approximate geography points.

depends:
  - raw.toronto_csi_events

materialization:
  type: table
  strategy: create+replace

columns:
  - name: objectid
    type: INTEGER
    description: ArcGIS object identifier for the source row.
    primary_key: true
    nullable: false
    checks:
      - name: unique
      - name: not_null
  - name: event_unique_id
    type: VARCHAR
    description: Toronto Police event identifier; multiple rows can share an event.
  - name: occurrence_date
    type: DATE
    description: Date when the event occurred.
  - name: occurrence_timestamp
    type: TIMESTAMP
    description: Timestamp when the event occurred.
  - name: report_date
    type: DATE
    description: Date when the event was reported.
  - name: report_timestamp
    type: TIMESTAMP
    description: Timestamp when the event was reported.
  - name: occurrence_year
    type: INTEGER
    description: Occurrence year.
    checks:
      - name: min
        value: 2014
  - name: occurrence_month_num
    type: INTEGER
    description: Occurrence month number, 1-12.
  - name: occurrence_month_name
    type: VARCHAR
    description: Occurrence month name.
  - name: occurrence_day
    type: INTEGER
    description: Occurrence day of month.
  - name: occurrence_day_of_week
    type: VARCHAR
    description: Occurrence day of week.
  - name: occurrence_day_of_week_num
    type: INTEGER
    description: Occurrence day-of-week number using BigQuery convention, Sunday=1.
  - name: occurrence_hour
    type: INTEGER
    description: Occurrence hour of day, 0-23.
    checks:
      - name: min
        value: 0
      - name: max
        value: 23
  - name: is_weekend
    type: BOOLEAN
    description: Whether occurrence day is Saturday or Sunday.
  - name: season
    type: VARCHAR
    description: Meteorological season of occurrence.
  - name: division
    type: VARCHAR
    description: Toronto Police division code.
  - name: location_type
    type: VARCHAR
    description: Published location type.
  - name: premises_type
    type: VARCHAR
    description: Published premises type.
  - name: ucr_code
    type: VARCHAR
    description: Uniform Crime Reporting offence code.
  - name: ucr_ext
    type: VARCHAR
    description: Uniform Crime Reporting extension code.
  - name: offence
    type: VARCHAR
    description: Published offence description.
  - name: csi_category
    type: VARCHAR
    description: Standardized Community Safety Indicator category.
    checks:
      - name: accepted_values
        value:
          - Assault
          - Auto Theft
          - Break and Enter
          - Robbery
          - Theft Over $5k
  - name: neighbourhood_model
    type: INTEGER
    description: Default neighbourhood model used for analysis, 158.
  - name: neighbourhood_id
    type: VARCHAR
    description: Zero-padded 158-model neighbourhood identifier, null for unknown/NSA.
  - name: neighbourhood_name
    type: VARCHAR
    description: 158-model neighbourhood name, null for unknown/NSA.
  - name: hood_140
    type: VARCHAR
    description: Historical 140-model neighbourhood identifier.
  - name: neighbourhood_140
    type: VARCHAR
    description: Historical 140-model neighbourhood label.
  - name: longitude
    type: DOUBLE
    description: Privacy-offset longitude in WGS84 decimal degrees.
  - name: latitude
    type: DOUBLE
    description: Privacy-offset latitude in WGS84 decimal degrees.
  - name: valid_toronto_coordinate
    type: BOOLEAN
    description: Whether the coordinate is non-zero and within a plausible Toronto bounding box.
  - name: crime_point
    type: VARCHAR
    description: BigQuery GEOGRAPHY point for valid approximate coordinates.
  - name: is_neighbourhood_known
    type: BOOLEAN
    description: Whether the 158-model neighbourhood is known and not NSA.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC extraction timestamp.

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.toronto_csi_events
    WHERE objectid IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY objectid
        ORDER BY extracted_at DESC
    ) = 1
),

typed AS (
    SELECT
        objectid,
        NULLIF(TRIM(event_unique_id), '') AS event_unique_id,
        DATE(occurrence_date) AS occurrence_date,
        occurrence_date AS occurrence_timestamp,
        DATE(report_date) AS report_date,
        report_date AS report_timestamp,
        COALESCE(occurrence_year, EXTRACT(YEAR FROM occurrence_date)) AS occurrence_year,
        EXTRACT(MONTH FROM occurrence_date) AS occurrence_month_num,
        COALESCE(NULLIF(TRIM(occurrence_month), ''), FORMAT_DATE('%B', DATE(occurrence_date))) AS occurrence_month_name,
        COALESCE(occurrence_day, EXTRACT(DAY FROM occurrence_date)) AS occurrence_day,
        COALESCE(NULLIF(TRIM(occurrence_day_of_week), ''), FORMAT_DATE('%A', DATE(occurrence_date))) AS occurrence_day_of_week,
        EXTRACT(DAYOFWEEK FROM DATE(occurrence_date)) AS occurrence_day_of_week_num,
        occurrence_hour,
        division,
        location_type,
        premises_type,
        ucr_code,
        ucr_ext,
        offence,
        CASE
            WHEN TRIM(csi_category) = 'Theft Over' THEN 'Theft Over $5k'
            ELSE TRIM(csi_category)
        END AS csi_category,
        hood_158,
        neighbourhood_158,
        hood_140,
        neighbourhood_140,
        longitude,
        latitude,
        extracted_at
    FROM deduped
)

SELECT
    objectid,
    event_unique_id,
    occurrence_date,
    occurrence_timestamp,
    report_date,
    report_timestamp,
    occurrence_year,
    occurrence_month_num,
    occurrence_month_name,
    occurrence_day,
    occurrence_day_of_week,
    occurrence_day_of_week_num,
    occurrence_hour,
    occurrence_day_of_week_num IN (1, 7) AS is_weekend,
    CASE
        WHEN occurrence_month_num IN (12, 1, 2) THEN 'Winter'
        WHEN occurrence_month_num IN (3, 4, 5) THEN 'Spring'
        WHEN occurrence_month_num IN (6, 7, 8) THEN 'Summer'
        WHEN occurrence_month_num IN (9, 10, 11) THEN 'Autumn'
    END AS season,
    NULLIF(TRIM(division), '') AS division,
    NULLIF(TRIM(location_type), '') AS location_type,
    NULLIF(TRIM(premises_type), '') AS premises_type,
    NULLIF(TRIM(ucr_code), '') AS ucr_code,
    NULLIF(TRIM(ucr_ext), '') AS ucr_ext,
    NULLIF(TRIM(offence), '') AS offence,
    csi_category,
    158 AS neighbourhood_model,
    CASE
        WHEN UPPER(TRIM(hood_158)) = 'NSA' THEN NULL
        ELSE LPAD(TRIM(hood_158), 3, '0')
    END AS neighbourhood_id,
    CASE
        WHEN UPPER(TRIM(neighbourhood_158)) = 'NSA' THEN NULL
        ELSE REGEXP_REPLACE(TRIM(neighbourhood_158), r'\s+\(\d+\)$', '')
    END AS neighbourhood_name,
    hood_140,
    neighbourhood_140,
    longitude,
    latitude,
    longitude BETWEEN -79.7 AND -79.1
        AND latitude BETWEEN 43.5 AND 43.9
        AND NOT (longitude = 0 AND latitude = 0) AS valid_toronto_coordinate,
    CASE
        WHEN longitude BETWEEN -79.7 AND -79.1
         AND latitude BETWEEN 43.5 AND 43.9
         AND NOT (longitude = 0 AND latitude = 0)
            THEN ST_GEOGPOINT(longitude, latitude)
    END AS crime_point,
    NOT (
        UPPER(TRIM(COALESCE(hood_158, ''))) = 'NSA'
        OR UPPER(TRIM(COALESCE(neighbourhood_158, ''))) = 'NSA'
        OR NULLIF(TRIM(COALESCE(hood_158, '')), '') IS NULL
    ) AS is_neighbourhood_known,
    extracted_at
FROM typed
WHERE occurrence_date IS NOT NULL
  AND occurrence_year >= 2014
  AND occurrence_hour BETWEEN 0 AND 23
ORDER BY occurrence_timestamp, objectid
