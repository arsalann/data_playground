/* @bruin
name: staging.neighbourhood_profiles
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Cleans Toronto neighbourhood profile rows and fills missing 158-model land
  area from the official boundary geometry. Provides population and land-area
  denominators for neighbourhood crime rates.

depends:
  - raw.toronto_neighbourhood_profiles
  - staging.neighbourhood_boundaries

materialization:
  type: table
  strategy: create+replace

columns:
  - name: neighbourhood_model
    type: INTEGER
    description: Toronto neighbourhood model number, either 158 or 140.
    primary_key: true
    nullable: false
  - name: profile_year
    type: INTEGER
    description: Census/profile year.
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
  - name: population
    type: INTEGER
    description: Resident population count.
    checks:
      - name: positive
  - name: land_area_km2
    type: DOUBLE
    description: Land area in square kilometres; profile value when present, otherwise boundary-derived area.
    checks:
      - name: positive
  - name: land_area_source
    type: VARCHAR
    description: Whether land area came from the profile source or boundary geometry.
  - name: population_density_per_km2
    type: DOUBLE
    description: Residents per square kilometre.
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
    FROM raw.toronto_neighbourhood_profiles
    WHERE neighbourhood_model IS NOT NULL
      AND profile_year IS NOT NULL
      AND neighbourhood_id IS NOT NULL
      AND population > 0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY neighbourhood_model, profile_year, neighbourhood_id
        ORDER BY extracted_at DESC
    ) = 1
)

SELECT
    p.neighbourhood_model,
    p.profile_year,
    LPAD(CAST(p.neighbourhood_id AS STRING), 3, '0') AS neighbourhood_id,
    TRIM(p.neighbourhood_name) AS neighbourhood_name,
    CAST(p.population AS INT64) AS population,
    ROUND(COALESCE(NULLIF(p.land_area_km2, 0), b.boundary_area_km2), 4) AS land_area_km2,
    CASE
        WHEN p.land_area_km2 IS NOT NULL AND p.land_area_km2 > 0 THEN 'profile'
        ELSE 'boundary_geometry'
    END AS land_area_source,
    ROUND(SAFE_DIVIDE(p.population, COALESCE(NULLIF(p.land_area_km2, 0), b.boundary_area_km2)), 2)
        AS population_density_per_km2,
    p.source_resource_name,
    p.source_resource_url,
    p.extracted_at
FROM deduped AS p
LEFT JOIN staging.neighbourhood_boundaries AS b
    ON p.neighbourhood_model = b.neighbourhood_model
   AND LPAD(CAST(p.neighbourhood_id AS STRING), 3, '0') = b.neighbourhood_id
WHERE COALESCE(NULLIF(p.land_area_km2, 0), b.boundary_area_km2) > 0
ORDER BY neighbourhood_model, neighbourhood_id
