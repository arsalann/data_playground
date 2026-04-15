/* @bruin

name: staging.city_profiles
type: bq.sql
description: |
  Comprehensive city-level dataset joining GHSL Urban Centre Database (~10K cities)
  with OSMnx street network analysis metrics (~20 cities). Provides the primary
  data source for the city-pulse dashboard visualizations.

  Combines authoritative global urban data from the European Commission's GHSL
  (population epochs, GDP, building heights, climate) with detailed street network
  "fingerprints" from OpenStreetMap analysis.
connection: bruin-playground-arsalan
tags:
  - urban_planning
  - geospatial
  - staging

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.ghsl_urban_centers
  - raw.street_networks

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: ghsl_id
    type: INTEGER
    description: GHSL unique identifier for the urban center (1-11422).
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: city_name
    type: VARCHAR
    description: City name from GHSL Urban Centre Database (GADM source).
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-3 country code (3 chars, 190 distinct countries).
    checks:
      - name: not_null
  - name: country_name
    type: VARCHAR
    description: Country name from GHSL (GADM source, 191 distinct countries).
    checks:
      - name: not_null
  - name: latitude
    type: DOUBLE
    description: City centroid latitude in decimal degrees (WGS84).
    checks:
      - name: not_null
  - name: longitude
    type: DOUBLE
    description: City centroid longitude in decimal degrees (WGS84).
    checks:
      - name: not_null
  - name: population_2015
    type: DOUBLE
    description: GHSL population estimate for 2015 epoch.
    checks:
      - name: not_null
  - name: population_2000
    type: DOUBLE
    description: GHSL population estimate for 2000 epoch.
    checks:
      - name: not_null
  - name: population_1975
    type: DOUBLE
    description: GHSL population estimate for 1975 epoch.
    checks:
      - name: not_null
  - name: area_km2
    type: DOUBLE
    description: Built-up surface area in square kilometers from GHSL.
    checks:
      - name: not_null
  - name: gdp_ppp
    type: DOUBLE
    description: GDP Purchasing Power Parity estimate in USD.
    checks:
      - name: not_null
  - name: avg_building_height_m
    type: DOUBLE
    description: Average building height in meters from GHS-BUILT-H 2020.
    checks:
      - name: not_null
  - name: hdi
    type: DOUBLE
    description: Human Development Index for 2020, scale 0-1.
  - name: avg_temp_c
    type: DOUBLE
    description: Mean annual temperature in Celsius.
    checks:
      - name: not_null
  - name: precipitation_mm
    type: DOUBLE
    description: Mean annual precipitation in millimeters.
    checks:
      - name: not_null
  - name: elevation_m
    type: DOUBLE
    description: Mean elevation in meters above sea level.
    checks:
      - name: not_null
  - name: population_tier
    type: VARCHAR
    description: Population tier from 2015 data (Megacity/Large/Medium/Small).
    checks:
      - name: not_null
  - name: climate_zone
    type: VARCHAR
    description: Simplified climate zone from temperature and precipitation thresholds.
  - name: continent
    type: VARCHAR
    description: Continent derived from country_code lookup.
    checks:
      - name: not_null
  - name: pop_growth_pct_2000_2015
    type: DOUBLE
    description: Population growth rate 2000-2015 as percentage.
    checks:
      - name: not_null
  - name: pop_density_per_km2
    type: DOUBLE
    description: Population density (2015 pop / area_km2) in persons per sq km.
    checks:
      - name: not_null
  - name: has_network_analysis
    type: BOOLEAN
    description: Whether OSMnx street network analysis is available (19 of 11K cities).
    checks:
      - name: not_null
  - name: orientation_entropy
    type: DOUBLE
    description: Street orientation entropy (lower = more grid-like). Only 19 cities.
  - name: orientation_order
    type: DOUBLE
    description: Grid-ness score 0-1 (higher = more grid-like). Only 19 cities.
  - name: avg_street_length_m
    type: DOUBLE
    description: Mean street segment length in meters. Only 19 cities.
  - name: intersection_count
    type: INTEGER
    description: Number of true intersections (degree >= 3). Only for analyzed cities.
  - name: dead_end_proportion
    type: DOUBLE
    description: Fraction of dead-end nodes in street network.
  - name: avg_circuity
    type: DOUBLE
    description: Average route circuity (network distance / straight-line distance).
  - name: bearing_counts
    type: VARCHAR
    description: JSON array of 36 bearing bins for street orientation polar plots.

@bruin */

WITH deduped_ghsl AS (
    SELECT *
    FROM raw.ghsl_urban_centers
    WHERE ghsl_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ghsl_id ORDER BY extracted_at DESC) = 1
),

deduped_networks AS (
    SELECT *
    FROM raw.street_networks
    WHERE city_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city_id ORDER BY extracted_at DESC) = 1
),

-- Match street network cities to GHSL centers by proximity (within ~50km)
-- Since OSMnx uses neighborhood-level queries, we match on country_code + nearest city
network_matched AS (
    SELECT
        g.ghsl_id,
        n.city_id,
        n.orientation_entropy,
        n.orientation_order,
        n.avg_street_length_m,
        n.intersection_count,
        n.dead_end_proportion,
        n.avg_circuity,
        n.bearing_counts,
        -- Distance in degrees (approximate) for matching
        SQRT(POW(g.latitude - n.latitude, 2) + POW(g.longitude - n.longitude, 2)) AS dist_deg
    FROM deduped_ghsl g
    CROSS JOIN deduped_networks n
    WHERE g.country_code = n.country_code
      AND SQRT(POW(g.latitude - n.latitude, 2) + POW(g.longitude - n.longitude, 2)) < 0.5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY n.city_id ORDER BY dist_deg ASC) = 1
),

enriched AS (
    SELECT
        g.ghsl_id,
        g.city_name,
        g.country_code,
        g.country_name,
        g.latitude,
        g.longitude,
        g.population_2015,
        g.population_2000,
        g.population_1975,
        g.area_km2,
        g.gdp_ppp,
        g.avg_building_height_m,
        g.hdi,
        g.avg_temp_c,
        g.precipitation_mm,
        g.elevation_m,

        -- Population tier
        CASE
            WHEN g.population_2015 >= 10000000 THEN 'Megacity (10M+)'
            WHEN g.population_2015 >= 1000000 THEN 'Large (1-10M)'
            WHEN g.population_2015 >= 100000 THEN 'Medium (100K-1M)'
            ELSE 'Small (<100K)'
        END AS population_tier,

        -- Simplified climate zone
        CASE
            WHEN g.avg_temp_c IS NULL THEN NULL
            WHEN g.avg_temp_c >= 18 AND COALESCE(g.precipitation_mm, 0) >= 1500 THEN 'Tropical'
            WHEN COALESCE(g.precipitation_mm, 0) < 250 THEN 'Arid'
            WHEN g.avg_temp_c >= 18 THEN 'Subtropical'
            WHEN g.avg_temp_c >= 10 THEN 'Temperate'
            WHEN g.avg_temp_c >= 0 THEN 'Continental'
            ELSE 'Polar'
        END AS climate_zone,

        -- Continent from country code
        CASE
            WHEN g.country_code IN ('CHN', 'JPN', 'KOR', 'PRK', 'MNG', 'TWN', 'HKG', 'MAC',
                                    'IDN', 'THA', 'VNM', 'MYS', 'PHL', 'SGP', 'MMR', 'KHM',
                                    'LAO', 'BRN', 'TLS', 'IND', 'PAK', 'BGD', 'LKA', 'NPL',
                                    'BTN', 'MDV', 'AFG', 'IRN', 'IRQ', 'ISR', 'JOR', 'LBN',
                                    'SYR', 'YEM', 'PSE', 'SAU', 'ARE', 'QAT', 'KWT', 'BHR',
                                    'OMN', 'TUR', 'GEO', 'ARM', 'AZE', 'KAZ', 'UZB', 'TKM',
                                    'TJK', 'KGZ')
                THEN 'Asia'
            WHEN g.country_code IN ('GBR', 'FRA', 'DEU', 'ITA', 'ESP', 'PRT', 'NLD', 'BEL',
                                    'AUT', 'CHE', 'SWE', 'NOR', 'DNK', 'FIN', 'ISL', 'IRL',
                                    'LUX', 'GRC', 'POL', 'CZE', 'SVK', 'HUN', 'ROU', 'BGR',
                                    'HRV', 'SVN', 'SRB', 'BIH', 'MNE', 'MKD', 'ALB', 'KOS',
                                    'EST', 'LVA', 'LTU', 'UKR', 'BLR', 'MDA', 'RUS', 'CYP',
                                    'MLT', 'AND', 'MCO', 'SMR', 'LIE', 'XKX')
                THEN 'Europe'
            WHEN g.country_code IN ('USA', 'CAN', 'MEX', 'BRA', 'ARG', 'COL', 'PER', 'VEN',
                                    'CHL', 'ECU', 'BOL', 'PRY', 'URY', 'GUY', 'SUR', 'CUB',
                                    'HTI', 'DOM', 'JAM', 'TTO', 'PRI', 'CRI', 'PAN', 'GTM',
                                    'HND', 'SLV', 'NIC', 'BLZ', 'BHS', 'BRB', 'GRD', 'LCA',
                                    'VCT', 'ATG', 'DMA', 'KNA')
                THEN 'Americas'
            WHEN g.country_code IN ('NGA', 'ETH', 'COD', 'TZA', 'KEN', 'UGA', 'GHA', 'MOZ',
                                    'MDG', 'CMR', 'CIV', 'NER', 'BFA', 'MLI', 'MWI', 'ZMB',
                                    'SEN', 'TCD', 'SOM', 'ZWE', 'GIN', 'RWA', 'BDI', 'BEN',
                                    'TGO', 'SLE', 'LBR', 'CAF', 'ERI', 'MRT', 'NAM', 'BWA',
                                    'LSO', 'GMB', 'GAB', 'SWZ', 'COM', 'MUS', 'STP', 'CPV',
                                    'SYC', 'DJI', 'GNQ', 'GNB', 'COG', 'AGO', 'SSD', 'ZAF',
                                    'EGY', 'LBY', 'TUN', 'DZA', 'MAR')
                THEN 'Africa'
            WHEN g.country_code IN ('AUS', 'NZL', 'PNG', 'FJI', 'SLB', 'VUT', 'WSM', 'TON',
                                    'FSM', 'KIR', 'MHL', 'PLW', 'TUV', 'NRU')
                THEN 'Oceania'
            ELSE 'Other'
        END AS continent,

        -- Population growth 2000-2015
        ROUND(
            SAFE_DIVIDE(g.population_2015 - g.population_2000, g.population_2000) * 100,
            2
        ) AS pop_growth_pct_2000_2015,

        -- Population density within the urban center
        ROUND(
            SAFE_DIVIDE(g.population_2015, g.area_km2),
            0
        ) AS pop_density_per_km2,

        -- Street network metrics (NULL for most cities)
        nm.city_id IS NOT NULL AS has_network_analysis,
        nm.orientation_entropy,
        nm.orientation_order,
        nm.avg_street_length_m,
        nm.intersection_count,
        nm.dead_end_proportion,
        nm.avg_circuity,
        nm.bearing_counts

    FROM deduped_ghsl g
    LEFT JOIN network_matched nm ON g.ghsl_id = nm.ghsl_id
)

SELECT *
FROM enriched
WHERE population_2015 IS NOT NULL AND population_2015 > 0
ORDER BY population_2015 DESC
