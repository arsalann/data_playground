/* @bruin
name: staging.crime_neighbourhood_yearly
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Aggregates offence/victim-level CSI rows by year, 158-model neighbourhood,
  category, and premises type, adding population- and area-normalized rates.

depends:
  - staging.crime_events
  - staging.neighbourhood_profiles
  - staging.neighbourhood_boundaries

materialization:
  type: table
  strategy: create+replace

columns:
  - name: occurrence_year
    type: INTEGER
    description: Occurrence year.
    primary_key: true
  - name: neighbourhood_model
    type: INTEGER
    description: Toronto neighbourhood model number.
    primary_key: true
  - name: neighbourhood_id
    type: VARCHAR
    description: Zero-padded neighbourhood identifier.
    primary_key: true
  - name: neighbourhood_name
    type: VARCHAR
    description: Neighbourhood name.
  - name: csi_category
    type: VARCHAR
    description: Community Safety Indicator category.
    primary_key: true
  - name: premises_type
    type: VARCHAR
    description: Published premises type.
    primary_key: true
  - name: crime_count
    type: INTEGER
    description: Count of CSI offence/victim-level rows.
  - name: distinct_event_count
    type: INTEGER
    description: Count of distinct event identifiers.
  - name: population
    type: INTEGER
    description: Profile population denominator.
  - name: land_area_km2
    type: DOUBLE
    description: Profile or boundary-filled land area denominator in square kilometres.
  - name: boundary_area_km2
    type: DOUBLE
    description: Boundary-derived area in square kilometres.
  - name: crime_count_per_1000_people
    type: DOUBLE
    description: CSI rows per 1,000 residents.
  - name: crime_count_per_km2_profile
    type: DOUBLE
    description: CSI rows per square kilometre using profile land area denominator.
  - name: crime_count_per_km2_boundary
    type: DOUBLE
    description: CSI rows per square kilometre using boundary-derived area denominator.

@bruin */

SELECT
    e.occurrence_year,
    e.neighbourhood_model,
    e.neighbourhood_id,
    COALESCE(p.neighbourhood_name, e.neighbourhood_name) AS neighbourhood_name,
    e.csi_category,
    COALESCE(e.premises_type, 'Unknown') AS premises_type,
    COUNT(*) AS crime_count,
    COUNT(DISTINCT e.event_unique_id) AS distinct_event_count,
    p.population,
    p.land_area_km2,
    b.boundary_area_km2,
    ROUND(SAFE_DIVIDE(COUNT(*), p.population) * 1000, 2) AS crime_count_per_1000_people,
    ROUND(SAFE_DIVIDE(COUNT(*), p.land_area_km2), 2) AS crime_count_per_km2_profile,
    ROUND(SAFE_DIVIDE(COUNT(*), b.boundary_area_km2), 2) AS crime_count_per_km2_boundary
FROM staging.crime_events AS e
LEFT JOIN staging.neighbourhood_profiles AS p
    ON e.neighbourhood_model = p.neighbourhood_model
   AND e.neighbourhood_id = p.neighbourhood_id
LEFT JOIN staging.neighbourhood_boundaries AS b
    ON e.neighbourhood_model = b.neighbourhood_model
   AND e.neighbourhood_id = b.neighbourhood_id
WHERE e.is_neighbourhood_known
GROUP BY
    e.occurrence_year,
    e.neighbourhood_model,
    e.neighbourhood_id,
    neighbourhood_name,
    e.csi_category,
    premises_type,
    p.population,
    p.land_area_km2,
    b.boundary_area_km2
ORDER BY occurrence_year, neighbourhood_id, csi_category, premises_type
