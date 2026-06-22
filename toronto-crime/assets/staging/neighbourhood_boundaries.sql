/* @bruin
name: staging.neighbourhood_boundaries
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Converts raw Toronto neighbourhood boundary GeoJSON into BigQuery geography
  objects with boundary-derived area and centroid fields for spatial joins and
  rate denominators.

depends:
  - raw.toronto_neighbourhood_boundaries

materialization:
  type: table
  strategy: create+replace

columns:
  - name: neighbourhood_model
    type: INTEGER
    description: Toronto neighbourhood model number, either 158 or 140.
    primary_key: true
    nullable: false
  - name: neighbourhood_id
    type: VARCHAR
    description: Zero-padded neighbourhood identifier within the model.
    primary_key: true
    nullable: false
  - name: neighbourhood_name
    type: VARCHAR
    description: Neighbourhood name.
  - name: geometry
    type: VARCHAR
    description: BigQuery GEOGRAPHY boundary geometry derived from source GeoJSON.
  - name: boundary_area_km2
    type: DOUBLE
    description: Boundary polygon area in square kilometres, derived with ST_AREA.
    checks:
      - name: positive
  - name: centroid
    type: VARCHAR
    description: BigQuery GEOGRAPHY centroid derived from the boundary polygon.
  - name: source_resource_name
    type: VARCHAR
    description: CKAN source resource name.
  - name: source_resource_url
    type: VARCHAR
    description: CKAN source resource URL.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC extraction timestamp.

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.toronto_neighbourhood_boundaries
    WHERE neighbourhood_model IS NOT NULL
      AND neighbourhood_id IS NOT NULL
      AND geojson_geometry IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY neighbourhood_model, neighbourhood_id
        ORDER BY extracted_at DESC
    ) = 1
),

geographies AS (
    SELECT
        neighbourhood_model,
        LPAD(CAST(neighbourhood_id AS STRING), 3, '0') AS neighbourhood_id,
        TRIM(neighbourhood_name) AS neighbourhood_name,
        SAFE.ST_GEOGFROMGEOJSON(geojson_geometry) AS geometry,
        source_resource_name,
        source_resource_url,
        extracted_at
    FROM deduped
)

SELECT
    neighbourhood_model,
    neighbourhood_id,
    neighbourhood_name,
    geometry,
    ROUND(ST_AREA(geometry) / 1000000.0, 4) AS boundary_area_km2,
    ST_CENTROID(geometry) AS centroid,
    source_resource_name,
    source_resource_url,
    extracted_at
FROM geographies
WHERE geometry IS NOT NULL
ORDER BY neighbourhood_model, neighbourhood_id
