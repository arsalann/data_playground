/* @bruin
name: report.neighbourhood_rankings
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Latest-complete-year neighbourhood rankings by population-adjusted CSI rate
  and raw CSI row count.

depends:
  - staging.crime_events
  - staging.neighbourhood_profiles
  - staging.neighbourhood_boundaries

materialization:
  type: table
  strategy: create+replace

columns:
  - name: rank_by_rate
    type: INTEGER
    description: Rank by CSI rows per 1,000 residents, descending.
    primary_key: true
  - name: rank_by_count
    type: INTEGER
    description: Rank by raw CSI row count, descending.
  - name: neighbourhood_id
    type: VARCHAR
    description: Zero-padded 158-model neighbourhood identifier.
  - name: neighbourhood_name
    type: VARCHAR
    description: Neighbourhood name.
  - name: crime_count
    type: INTEGER
    description: CSI offence/victim-level rows in the latest complete year.
  - name: distinct_event_count
    type: INTEGER
    description: Distinct event identifiers in the latest complete year.
  - name: crime_count_per_1000_people
    type: DOUBLE
    description: CSI rows per 1,000 residents.
  - name: crime_count_per_km2
    type: DOUBLE
    description: CSI rows per square kilometre using profile or boundary-filled land area.
  - name: citywide_rate_per_1000_people
    type: DOUBLE
    description: Citywide CSI rows per 1,000 residents for the latest complete year.
  - name: population
    type: INTEGER
    description: Resident population denominator.
  - name: land_area_km2
    type: DOUBLE
    description: Land area denominator in square kilometres.
  - name: latest_complete_year
    type: INTEGER
    description: Latest year treated as complete for yearly comparisons.

@bruin */

WITH latest_year AS (
    SELECT COALESCE(
        MAX(IF(occurrence_year < EXTRACT(YEAR FROM CURRENT_DATE()), occurrence_year, NULL)),
        MAX(occurrence_year)
    ) AS latest_complete_year
    FROM staging.crime_events
),

neighbourhood_counts AS (
    SELECT
        e.neighbourhood_id,
        COALESCE(p.neighbourhood_name, e.neighbourhood_name) AS neighbourhood_name,
        COUNT(*) AS crime_count,
        COUNT(DISTINCT e.event_unique_id) AS distinct_event_count,
        p.population,
        p.land_area_km2,
        ly.latest_complete_year
    FROM staging.crime_events AS e
    CROSS JOIN latest_year AS ly
    LEFT JOIN staging.neighbourhood_profiles AS p
        ON e.neighbourhood_model = p.neighbourhood_model
       AND e.neighbourhood_id = p.neighbourhood_id
    WHERE e.occurrence_year = ly.latest_complete_year
      AND e.is_neighbourhood_known
      AND e.neighbourhood_model = 158
    GROUP BY 1, 2, 5, 6, 7
),

citywide AS (
    SELECT
        COUNT(*) AS city_crime_count,
        MAX(p.population) AS city_population
    FROM staging.crime_events AS e
    CROSS JOIN latest_year AS ly
    CROSS JOIN (
        SELECT SUM(population) AS population
        FROM staging.neighbourhood_profiles
        WHERE neighbourhood_model = 158
          AND profile_year = 2021
    ) AS p
    WHERE e.occurrence_year = ly.latest_complete_year
)

SELECT
    RANK() OVER (ORDER BY SAFE_DIVIDE(crime_count, population) DESC) AS rank_by_rate,
    RANK() OVER (ORDER BY crime_count DESC) AS rank_by_count,
    neighbourhood_id,
    neighbourhood_name,
    crime_count,
    distinct_event_count,
    ROUND(SAFE_DIVIDE(crime_count, population) * 1000, 2) AS crime_count_per_1000_people,
    ROUND(SAFE_DIVIDE(crime_count, land_area_km2), 2) AS crime_count_per_km2,
    ROUND(SAFE_DIVIDE(city_crime_count, city_population) * 1000, 2) AS citywide_rate_per_1000_people,
    population,
    land_area_km2,
    latest_complete_year
FROM neighbourhood_counts
CROSS JOIN citywide
WHERE population > 0
  AND land_area_km2 > 0
ORDER BY rank_by_rate, neighbourhood_name
