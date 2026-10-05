/* @bruin
name: raw.atlas_cities
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Reference list of the 471 urban centres with at least 1 million residents (2015)
  used as the unit of analysis for the Disaster Summer Atlas. Selected from
  raw.ghsl_urban_centers, which is ingested by the city-pulse pipeline from the
  GHSL Urban Centre Database R2024A (European Commission JRC,
  https://ghsl.jrc.ec.europa.eu/ghs_stat_ucdb2015mt_r2019a.php, CC BY 4.0).
  The upstream table belongs to another pipeline, so it is not declared in depends.

  HDI cleaning: when a city's GHSL HDI is more than 0.20 below the median HDI of
  all GHSL urban centres in the same country, it is treated as a data error and
  replaced by that country median (hdi_imputed = TRUE). This affects Dammam, SAU
  (0.318 vs 0.866) and Xiamen, CHN (0.527 vs 0.754).

materialization:
  type: table
  strategy: create+replace

columns:
  - name: ghsl_id
    type: INTEGER
    description: GHSL urban centre identifier (ID_UC_G0).
    primary_key: true
    nullable: false
    checks:
      - name: unique
      - name: not_null
  - name: city_name
    type: VARCHAR
    description: Urban centre name as published by GHSL.
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-3 country code.
  - name: country_name
    type: VARCHAR
    description: Country name.
  - name: latitude
    type: DOUBLE
    description: Urban centre centroid latitude (WGS84 degrees).
  - name: longitude
    type: DOUBLE
    description: Urban centre centroid longitude (WGS84 degrees).
  - name: population_2015
    type: DOUBLE
    description: GHS-POP modelled resident population in 2015 (people).
    checks:
      - name: min
        value: 1000000
  - name: hdi_raw
    type: DOUBLE
    description: Subnational Human Development Index (2020, 0-1) as published in GHSL; NULL for 9 centres.
  - name: hdi
    type: DOUBLE
    description: Cleaned HDI (0-1) - hdi_raw, or the country median when hdi_raw is > 0.20 below it.
  - name: hdi_imputed
    type: BOOLEAN
    description: TRUE when hdi was replaced by the country median.
  - name: area_km2
    type: INTEGER
    description: Urban centre footprint area (km2).
@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.ghsl_urban_centers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ghsl_id ORDER BY extracted_at DESC) = 1
),

country_hdi AS (
    SELECT country_code, APPROX_QUANTILES(hdi, 2)[OFFSET(1)] AS country_median_hdi
    FROM deduped
    WHERE hdi IS NOT NULL
    GROUP BY 1
)

SELECT
    d.ghsl_id,
    d.city_name,
    d.country_code,
    d.country_name,
    d.latitude,
    d.longitude,
    d.population_2015,
    d.hdi AS hdi_raw,
    IF(d.hdi < ch.country_median_hdi - 0.20, ch.country_median_hdi, d.hdi) AS hdi,
    COALESCE(d.hdi < ch.country_median_hdi - 0.20, FALSE) AS hdi_imputed,
    d.area_km2
FROM deduped AS d
LEFT JOIN country_hdi AS ch USING (country_code)
WHERE d.population_2015 >= 1000000
