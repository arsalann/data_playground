/* @bruin

name: eu_mortality_staging.em_nuts3_dim
type: bq.sql
description: |
  Canonical NUTS3 dimension for the EU environmental-mortality investigation.

  One row per NUTS3 region in the EU-27 (1,165 rows, classification 2024). Used as
  the anchor table that every other staging table joins on. Re-used by the
  sibling eu-pfas pipeline.

  Adds derived attributes:
    - degurba_label: human-readable urbanisation class.
    - mount_label: mountain-area class.
    - coast_label: coastal class.
    - country_name_en: English country name.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - eu_mortality_raw.nuts3_reference

columns:
  - name: nuts_id
    type: VARCHAR
    description: NUTS3 code. Primary key.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: nuts1_id
    type: VARCHAR
    description: Parent NUTS1 code.
  - name: nuts2_id
    type: VARCHAR
    description: Parent NUTS2 code.
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2 country code.
    checks:
      - name: not_null
  - name: country_name_en
    type: VARCHAR
    description: English country name.
  - name: name_latn
    type: VARCHAR
    description: Latin-script regional name.
  - name: name_native
    type: VARCHAR
    description: Native-script regional name.
  - name: degurba
    type: INTEGER
    description: DEGURBA class (1 cities, 2 towns and suburbs, 3 rural).
  - name: degurba_label
    type: VARCHAR
    description: Human-readable DEGURBA label.
  - name: mount_type
    type: INTEGER
    description: GISCO mountain-area class.
  - name: mount_label
    type: VARCHAR
    description: Human-readable mountain class.
  - name: coast_type
    type: INTEGER
    description: GISCO coastal class.
  - name: coast_label
    type: VARCHAR
    description: Human-readable coast class.
  - name: centroid_lat
    type: DOUBLE
    description: Representative inside-polygon latitude.
  - name: centroid_lon
    type: DOUBLE
    description: Representative inside-polygon longitude.

@bruin */

WITH src AS (
    SELECT *
    FROM `bruin-playground-arsalan.eu_mortality_raw.nuts3_reference`
    WHERE level_code = 3
),

country_names AS (
    SELECT * FROM UNNEST([
        STRUCT('AT' AS country_code, 'Austria' AS country_name_en),
        STRUCT('BE', 'Belgium'),
        STRUCT('BG', 'Bulgaria'),
        STRUCT('HR', 'Croatia'),
        STRUCT('CY', 'Cyprus'),
        STRUCT('CZ', 'Czechia'),
        STRUCT('DK', 'Denmark'),
        STRUCT('EE', 'Estonia'),
        STRUCT('FI', 'Finland'),
        STRUCT('FR', 'France'),
        STRUCT('DE', 'Germany'),
        STRUCT('EL', 'Greece'),
        STRUCT('HU', 'Hungary'),
        STRUCT('IE', 'Ireland'),
        STRUCT('IT', 'Italy'),
        STRUCT('LV', 'Latvia'),
        STRUCT('LT', 'Lithuania'),
        STRUCT('LU', 'Luxembourg'),
        STRUCT('MT', 'Malta'),
        STRUCT('NL', 'Netherlands'),
        STRUCT('PL', 'Poland'),
        STRUCT('PT', 'Portugal'),
        STRUCT('RO', 'Romania'),
        STRUCT('SK', 'Slovakia'),
        STRUCT('SI', 'Slovenia'),
        STRUCT('ES', 'Spain'),
        STRUCT('SE', 'Sweden')
    ])
)

SELECT
    s.nuts_id,
    s.nuts1_id,
    s.nuts2_id,
    s.country_code,
    c.country_name_en,
    s.name_latn,
    s.name_native,
    s.degurba,
    CASE s.degurba
        WHEN 1 THEN 'Cities (densely populated)'
        WHEN 2 THEN 'Towns and suburbs (intermediate)'
        WHEN 3 THEN 'Rural areas (thinly populated)'
        ELSE 'Unknown'
    END AS degurba_label,
    s.mount_type,
    CASE s.mount_type
        WHEN 1 THEN '>50% population in mountain areas'
        WHEN 2 THEN '>50% area in mountains'
        WHEN 3 THEN 'Mountain population and area'
        WHEN 4 THEN 'Non-mountain'
        ELSE 'Unspecified'
    END AS mount_label,
    s.coast_type,
    CASE s.coast_type
        WHEN 1 THEN 'Coastline'
        WHEN 2 THEN '>50% population within 50 km of coast'
        WHEN 3 THEN 'Inland'
        ELSE 'Unspecified'
    END AS coast_label,
    s.centroid_lat,
    s.centroid_lon
FROM src s
LEFT JOIN country_names c USING (country_code)
ORDER BY s.country_code, s.nuts_id
