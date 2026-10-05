/* @bruin

name: staging.metro_projects_clean
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Cleans the Transit Costs Project snapshot and derives comparable urban-rail
  project metrics. Rows outside the metro/urban-rail comparison are preserved
  with comparability_class and include_in_analysis flags.

depends:
  - raw.metro_costs_project

materialization:
  type: table
  strategy: create+replace

columns:
  - name: project_id
    type: VARCHAR
    description: Stable Transit Costs Project identifier.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: country_code
    type: VARCHAR
    description: Two-letter country code.
  - name: city
    type: VARCHAR
    description: Project city or metropolitan area.
  - name: line_name
    type: VARCHAR
    description: Transit line or project family.
  - name: phase_name
    type: VARCHAR
    description: Named project phase or segment.
  - name: start_year
    type: INTEGER
    description: Construction start year.
  - name: end_year
    type: INTEGER
    description: Completion or expected completion year.
  - name: construction_midpoint_year
    type: INTEGER
    description: Midpoint year of the construction period, or start year when completion is unavailable.
  - name: construction_duration_years
    type: DOUBLE
    description: Elapsed construction duration in years when both years are valid.
    checks:
      - name: non_negative
  - name: construction_decade
    type: VARCHAR
    description: Decade containing the construction start year.
  - name: length_km
    type: DOUBLE
    description: Revenue corridor length in kilometers.
  - name: tunnel_pct
    type: DOUBLE
    description: Tunnel share of corridor length, percent.
    checks:
      - name: min
        value: 0
      - name: max
        value: 100
  - name: tunnel_km
    type: DOUBLE
    description: Tunnel corridor length in kilometers.
  - name: elevated_km
    type: DOUBLE
    description: Elevated corridor length in kilometers.
  - name: at_grade_km
    type: DOUBLE
    description: At-grade corridor length in kilometers.
  - name: alignment_known_pct
    type: DOUBLE
    description: Share of corridor length accounted for by tunnel, elevated, and at-grade lengths, percent.
  - name: stations
    type: INTEGER
    description: Number of stations attributed to the project or phase.
  - name: stations_per_km
    type: DOUBLE
    description: Stations per corridor kilometer.
  - name: platform_length_m
    type: DOUBLE
    description: Platform length in meters when available.
  - name: cost_per_km_2025_usd_m
    type: DOUBLE
    description: Cost per corridor kilometer in 2025 US dollars and millions.
  - name: total_cost_2025_usd_m
    type: DOUBLE
    description: Total project cost in 2025 US dollars and millions.
  - name: cost_per_station_2025_usd_m
    type: DOUBLE
    description: Total project cost per station in 2025 US dollars and millions.
  - name: cost_source_type
    type: VARCHAR
    description: Source quality category for the cost value.
  - name: source_quality_score
    type: INTEGER
    description: Ordered source quality score; official plans are higher than media or wiki sources.
  - name: source_length_type
    type: VARCHAR
    description: Source category for the length estimate.
  - name: reference_url
    type: VARCHAR
    description: Supporting project reference URL.
  - name: comparability_class
    type: VARCHAR
    description: Included or excluded project classification.
  - name: include_in_analysis
    type: BOOLEAN
    description: True when the project is an included metro/urban-rail project with positive length and cost per kilometer.
  - name: analysis_region
    type: VARCHAR
    description: North America, Europe, or Other.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp from the raw source snapshot.

custom_checks:
  - name: valid_year_order
    description: Ensures completion is not earlier than construction start when both years are present.
    query: |
      SELECT COUNT(*)
      FROM staging.metro_projects_clean
      WHERE start_year IS NOT NULL
        AND end_year IS NOT NULL
        AND end_year < start_year
    value: 0

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.metro_costs_project
    WHERE project_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY project_id
        ORDER BY extracted_at DESC
    ) = 1
),
derived AS (
    SELECT
        project_id,
        country_code,
        city,
        line_name,
        phase_name,
        start_year,
        end_year,
        CASE
            WHEN start_year IS NULL THEN NULL
            WHEN end_year IS NULL OR end_year < start_year THEN start_year
            ELSE CAST(FLOOR((start_year + end_year) / 2) AS INT64)
        END AS construction_midpoint_year,
        CASE
            WHEN start_year IS NOT NULL AND end_year IS NOT NULL AND end_year >= start_year
                THEN CAST(end_year - start_year AS FLOAT64)
            ELSE NULL
        END AS construction_duration_years,
        CASE
            WHEN start_year IS NULL THEN 'Unknown'
            ELSE CONCAT(CAST(CAST(FLOOR(start_year / 10) * 10 AS INT64) AS STRING), 's')
        END AS construction_decade,
        length_km,
        COALESCE(tunnel_pct, SAFE_DIVIDE(tunnel_km, length_km) * 100) AS tunnel_pct,
        tunnel_km,
        elevated_km,
        at_grade_km,
        SAFE_DIVIDE(COALESCE(tunnel_km, 0) + COALESCE(elevated_km, 0) + COALESCE(at_grade_km, 0), length_km) * 100 AS alignment_known_pct,
        stations,
        SAFE_DIVIDE(stations, length_km) AS stations_per_km,
        platform_length_m,
        cost_per_km_2025_usd_m,
        cost_per_km_2025_usd_m * length_km AS total_cost_2025_usd_m,
        SAFE_DIVIDE(cost_per_km_2025_usd_m * length_km, stations) AS cost_per_station_2025_usd_m,
        cost_source_type,
        CASE LOWER(TRIM(cost_source_type))
            WHEN 'plan' THEN 4
            WHEN 'trade' THEN 3
            WHEN 'media' THEN 2
            WHEN 'measured' THEN 1
            WHEN 'wiki' THEN 0
            ELSE NULL
        END AS source_quality_score,
        source_length_type,
        reference_url,
        comparability_class,
        comparability_class = 'included_metro_urban_rail'
            AND COALESCE(length_km, 0) > 0
            AND COALESCE(cost_per_km_2025_usd_m, 0) > 0 AS include_in_analysis,
        analysis_region,
        extracted_at
    FROM deduped
)
SELECT *
FROM derived
ORDER BY analysis_region, country_code, city, line_name, phase_name
