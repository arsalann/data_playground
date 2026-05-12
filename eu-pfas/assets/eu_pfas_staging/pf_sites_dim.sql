/* @bruin

name: eu_pfas_staging.pf_sites_dim
type: bq.sql
description: |
  Canonical PFAS-site dimension for the EU-27.

  Built from the Forever Pollution Project / PFAS Data Hub raw extract. Filters
  to (a) EU-27 countries, (b) sites with valid coordinates, and assigns each site
  to the NUTS3 region whose Eurostat GISCO label point sits closest. Spatial
  approximation: nearest-centroid join via Haversine, computed in BigQuery using
  ST_DISTANCE on GEOGRAPHY points.

  Drinking-water relevance flag: water-matrix samples (drinking water, ground
  water, surface water) are flagged is_water_sample for compliance use.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - eu_pfas_raw.forever_pollution_sites

columns:
  - name: site_uid
    type: VARCHAR
    description: Synthetic site identifier from raw layer.
    primary_key: true
    checks:
      - name: not_null
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2 (derived from country-name to ISO mapping).
  - name: country_name_en
    type: VARCHAR
    description: English country name.
  - name: city
    type: VARCHAR
    description: City / locality.
  - name: site_name
    type: VARCHAR
    description: Site name as reported.
  - name: site_type
    type: VARCHAR
    description: Industrial site / Military / Airport / Wastewater / etc.
  - name: category
    type: VARCHAR
    description: PFAS Data Hub site category.
  - name: matrix
    type: VARCHAR
    description: Sampling matrix.
  - name: is_water_sample
    type: BOOLEAN
    description: TRUE if matrix is drinking water, groundwater, surface water, or tap water.
  - name: pfas_sum
    type: DOUBLE
    description: Aggregated PFAS concentration.
  - name: unit
    type: VARCHAR
    description: Unit of pfas_sum.
  - name: pfas_sum_ng_l
    type: DOUBLE
    description: Concentration normalised to ng/L (NULL for non-water matrices or unparseable units).
  - name: nuts_id
    type: VARCHAR
    description: NUTS3 region assigned by nearest-centroid join. May be NULL if the nearest centroid is > 200 km (offshore / extra-EU coords).
  - name: distance_to_centroid_km
    type: DOUBLE
    description: Haversine distance from site to assigned NUTS3 centroid (km).
  - name: lat
    type: DOUBLE
    description: Latitude.
  - name: lon
    type: DOUBLE
    description: Longitude.
  - name: source_url
    type: VARCHAR
    description: Original authority URL.
  - name: measure_year
    type: INTEGER
    description: Year of measurement where reported.

@bruin */

WITH country_map AS (
    SELECT * FROM UNNEST([
        STRUCT('Austria' AS country, 'AT' AS country_code),
        STRUCT('Belgium', 'BE'),
        STRUCT('Bulgaria', 'BG'),
        STRUCT('Croatia', 'HR'),
        STRUCT('Cyprus', 'CY'),
        STRUCT('Czechia', 'CZ'),
        STRUCT('Czech Republic', 'CZ'),
        STRUCT('Denmark', 'DK'),
        STRUCT('Estonia', 'EE'),
        STRUCT('Finland', 'FI'),
        STRUCT('France', 'FR'),
        STRUCT('Germany', 'DE'),
        STRUCT('Greece', 'EL'),
        STRUCT('Hungary', 'HU'),
        STRUCT('Ireland', 'IE'),
        STRUCT('Italy', 'IT'),
        STRUCT('Latvia', 'LV'),
        STRUCT('Lithuania', 'LT'),
        STRUCT('Luxembourg', 'LU'),
        STRUCT('Malta', 'MT'),
        STRUCT('Netherlands', 'NL'),
        STRUCT('Poland', 'PL'),
        STRUCT('Portugal', 'PT'),
        STRUCT('Romania', 'RO'),
        STRUCT('Slovakia', 'SK'),
        STRUCT('Slovenia', 'SI'),
        STRUCT('Spain', 'ES'),
        STRUCT('Sweden', 'SE')
    ])
),

sites AS (
    SELECT
        s.site_uid,
        s.country,
        c.country_code,
        s.city,
        s.site_name,
        s.site_type,
        s.category,
        s.matrix,
        LOWER(IFNULL(s.matrix, '')) IN ('water', 'drinking water', 'groundwater',
            'surface water', 'tap water', 'eau potable', 'water - drinking water',
            'water - groundwater', 'water - surface water') AS is_water_sample,
        s.pfas_sum,
        s.unit,
        CASE
            WHEN LOWER(IFNULL(s.unit, '')) IN ('ng/l', 'ng/l', 'ng l-1', 'ng_l') THEN s.pfas_sum
            WHEN LOWER(IFNULL(s.unit, '')) IN ('ug/l', 'ug/l', 'mug/l', 'mg/l divided 1000') THEN s.pfas_sum * 1000
            WHEN LOWER(IFNULL(s.unit, '')) IN ('mg/l') THEN s.pfas_sum * 1000000
            ELSE NULL
        END AS pfas_sum_ng_l,
        s.source_url,
        SAFE_CAST(s.measure_year AS INT64) AS measure_year,
        s.lat,
        s.lon,
        ST_GEOGPOINT(s.lon, s.lat) AS site_geo
    FROM `bruin-playground-arsalan.eu_pfas_raw.forever_pollution_sites` s
    LEFT JOIN country_map c ON c.country = s.country
    WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
      AND c.country_code IS NOT NULL
),

centroids AS (
    SELECT
        nuts_id,
        country_code,
        country_name_en,
        ST_GEOGPOINT(centroid_lon, centroid_lat) AS nuts_geo
    FROM `bruin-playground-arsalan.eu_mortality_staging.em_nuts3_dim`
),

scored AS (
    SELECT
        s.site_uid,
        s.country_code,
        s.city,
        s.site_name,
        s.site_type,
        s.category,
        s.matrix,
        s.is_water_sample,
        s.pfas_sum,
        s.unit,
        s.pfas_sum_ng_l,
        s.source_url,
        s.measure_year,
        s.lat,
        s.lon,
        c.nuts_id,
        c.country_name_en AS nuts_country_name_en,
        ST_DISTANCE(s.site_geo, c.nuts_geo) / 1000 AS distance_to_centroid_km,
        ROW_NUMBER() OVER (
            PARTITION BY s.site_uid
            ORDER BY ST_DISTANCE(s.site_geo, c.nuts_geo)
        ) AS rn
    FROM sites s
    JOIN centroids c ON c.country_code = s.country_code
)

SELECT
    site_uid,
    country_code,
    nuts_country_name_en AS country_name_en,
    city,
    site_name,
    site_type,
    category,
    matrix,
    is_water_sample,
    pfas_sum,
    unit,
    pfas_sum_ng_l,
    nuts_id,
    distance_to_centroid_km,
    lat,
    lon,
    source_url,
    measure_year
FROM scored
WHERE rn = 1
ORDER BY country_code, site_uid
