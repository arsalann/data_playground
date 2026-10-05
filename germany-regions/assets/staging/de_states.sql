/* @bruin
name: staging.de_states
type: bq.sql
connection: bruin-playground-arsalan
description: |
  One row per German state (Bundesland): official state code (AGS 01-16), NUTS 1 code,
  names, two-letter abbreviation, East/West grouping, area, and a map label anchor.

  The AGS <-> NUTS 1 mapping follows the Eurostat NUTS 2024 classification
  (https://ec.europa.eu/eurostat/web/nuts). East/West follows the Destatis convention:
  "Neue Laender" = Brandenburg, Mecklenburg-Vorpommern, Sachsen, Sachsen-Anhalt, Thueringen;
  Berlin is kept as its own group because its statistics combine former East and West Berlin.
  Label anchors are the GISCO polygon centroids, except Brandenburg (centroid falls inside
  Berlin), Bremen (centroid falls between Bremen and Bremerhaven) and Niedersachsen (label
  would collide with Bremen), which are placed by hand.

depends:
  - raw.de_state_boundaries

materialization:
  type: table
  strategy: create+replace

columns:
  - name: ags_code
    type: VARCHAR
    description: Official state code (Amtlicher Gemeindeschluessel, first two digits), 01-16.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: unique
  - name: nuts1_id
    type: VARCHAR
    description: NUTS 1 code, DE1-DEG.
    nullable: false
    checks:
      - name: not_null
      - name: unique
  - name: state_name
    type: VARCHAR
    description: Official German state name.
  - name: state_name_en
    type: VARCHAR
    description: English state name.
  - name: state_abbr
    type: VARCHAR
    description: Two-letter state abbreviation used on the maps (BW, BY, BE, ...).
  - name: region_group
    type: VARCHAR
    description: West, East, or Berlin.
    checks:
      - name: accepted_values
        value: [West, East, Berlin]
  - name: area_km2
    type: DOUBLE
    description: Polygon area from the GISCO 1:10M boundary (km2); display only, density uses the official Destatis area.
  - name: label_lon
    type: DOUBLE
    description: Longitude (WGS84) of the map label anchor.
  - name: label_lat
    type: DOUBLE
    description: Latitude (WGS84) of the map label anchor.

@bruin */

WITH states AS (
    SELECT *
    FROM UNNEST([
        STRUCT('01' AS ags_code, 'DEF' AS nuts1_id, 'Schleswig-Holstein' AS state_name, 'Schleswig-Holstein' AS state_name_en, 'SH' AS state_abbr, 'West' AS region_group, CAST(NULL AS FLOAT64) AS label_lon, CAST(NULL AS FLOAT64) AS label_lat),
        STRUCT('02', 'DE6', 'Hamburg', 'Hamburg', 'HH', 'West', NULL, NULL),
        STRUCT('03', 'DE9', 'Niedersachsen', 'Lower Saxony', 'NI', 'West', 9.70, 52.40),
        STRUCT('04', 'DE5', 'Bremen', 'Bremen', 'HB', 'West', 8.80, 53.08),
        STRUCT('05', 'DEA', 'Nordrhein-Westfalen', 'North Rhine-Westphalia', 'NW', 'West', NULL, NULL),
        STRUCT('06', 'DE7', 'Hessen', 'Hesse', 'HE', 'West', NULL, NULL),
        STRUCT('07', 'DEB', 'Rheinland-Pfalz', 'Rhineland-Palatinate', 'RP', 'West', NULL, NULL),
        STRUCT('08', 'DE1', 'Baden-Württemberg', 'Baden-Württemberg', 'BW', 'West', NULL, NULL),
        STRUCT('09', 'DE2', 'Bayern', 'Bavaria', 'BY', 'West', NULL, NULL),
        STRUCT('10', 'DEC', 'Saarland', 'Saarland', 'SL', 'West', NULL, NULL),
        STRUCT('11', 'DE3', 'Berlin', 'Berlin', 'BE', 'Berlin', NULL, NULL),
        STRUCT('12', 'DE4', 'Brandenburg', 'Brandenburg', 'BB', 'East', 13.90, 52.05),
        STRUCT('13', 'DE8', 'Mecklenburg-Vorpommern', 'Mecklenburg-Western Pomerania', 'MV', 'East', NULL, NULL),
        STRUCT('14', 'DED', 'Sachsen', 'Saxony', 'SN', 'East', NULL, NULL),
        STRUCT('15', 'DEE', 'Sachsen-Anhalt', 'Saxony-Anhalt', 'ST', 'East', NULL, NULL),
        STRUCT('16', 'DEG', 'Thüringen', 'Thuringia', 'TH', 'East', NULL, NULL)
    ])
),

geoms AS (
    SELECT
        nuts1_id,
        ST_GEOGFROMGEOJSON(geometry_geojson, make_valid => TRUE) AS geom
    FROM raw.de_state_boundaries
)

SELECT
    s.ags_code,
    s.nuts1_id,
    s.state_name,
    s.state_name_en,
    s.state_abbr,
    s.region_group,
    ROUND(ST_AREA(g.geom) / 1e6, 1) AS area_km2,
    COALESCE(s.label_lon, ROUND(ST_X(ST_CENTROID(g.geom)), 3)) AS label_lon,
    COALESCE(s.label_lat, ROUND(ST_Y(ST_CENTROID(g.geom)), 3)) AS label_lat
FROM states AS s
INNER JOIN geoms AS g USING (nuts1_id)
ORDER BY s.ags_code
