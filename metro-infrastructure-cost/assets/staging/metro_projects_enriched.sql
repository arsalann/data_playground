/* @bruin

name: staging.metro_projects_enriched
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Enriches cleaned metro construction projects with the nearest available World
  Bank country-year context to the construction midpoint. The chosen context
  year and year gap remain visible so the dashboard does not hide temporal mismatch.

depends:
  - staging.metro_projects_clean
  - raw.world_bank_context

materialization:
  type: table
  strategy: create+replace

columns:
  - name: project_id
    type: VARCHAR
    description: Stable project identifier.
    primary_key: true
  - name: country_code
    type: VARCHAR
    description: Two-letter country code.
  - name: country_name
    type: VARCHAR
    description: World Bank country name.
  - name: city
    type: VARCHAR
    description: Project city or metropolitan area.
  - name: line_name
    type: VARCHAR
    description: Transit line or project family.
  - name: phase_name
    type: VARCHAR
    description: Named project phase or segment.
  - name: analysis_region
    type: VARCHAR
    description: North America, Europe, or Other.
  - name: construction_midpoint_year
    type: INTEGER
    description: Construction midpoint year used for context matching.
  - name: context_year
    type: INTEGER
    description: World Bank year selected as nearest to the construction midpoint.
  - name: context_year_gap
    type: INTEGER
    description: Absolute difference between construction midpoint and selected context year.
  - name: gdp_per_capita_ppp_2021
    type: DOUBLE
    description: GDP per capita in constant 2021 international dollars at PPP.
  - name: population
    type: DOUBLE
    description: Total country population in persons.
  - name: urban_population_pct
    type: DOUBLE
    description: Urban population as a percentage of total population.
  - name: length_km
    type: DOUBLE
    description: Revenue corridor length in kilometers.
  - name: tunnel_pct
    type: DOUBLE
    description: Tunnel share of corridor length, percent.
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
    description: Share of corridor length accounted for by known alignment types, percent.
  - name: stations
    type: INTEGER
    description: Number of stations.
  - name: stations_per_km
    type: DOUBLE
    description: Stations per corridor kilometer.
  - name: construction_duration_years
    type: DOUBLE
    description: Elapsed construction duration in years.
  - name: construction_decade
    type: VARCHAR
    description: Decade containing construction start.
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
    description: Ordered cost-source quality score.
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
    description: True for included projects with positive length and cost/km.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp from the latest upstream snapshot.

@bruin */

WITH projects AS (
    SELECT *
    FROM staging.metro_projects_clean
    WHERE project_id IS NOT NULL
),
context AS (
    SELECT *
    FROM raw.world_bank_context
    WHERE country_code IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY country_code, year
        ORDER BY extracted_at DESC
    ) = 1
),
candidates AS (
    SELECT
        p.*,
        c.country_name,
        c.year AS context_year_candidate,
        c.gdp_per_capita_ppp_2021,
        c.population,
        c.urban_population_pct,
        COALESCE(p.construction_midpoint_year, p.start_year) AS target_context_year,
        ROW_NUMBER() OVER (
            PARTITION BY p.project_id
            ORDER BY
                CASE WHEN c.year IS NULL THEN 1 ELSE 0 END,
                ABS(c.year - COALESCE(p.construction_midpoint_year, p.start_year)),
                CASE
                    WHEN c.year <= COALESCE(p.construction_midpoint_year, p.start_year) THEN 0
                    ELSE 1
                END,
                c.year DESC
        ) AS context_rank
    FROM projects AS p
    LEFT JOIN context AS c
        ON c.country_code = p.country_code
)
SELECT
    project_id,
    country_code,
    country_name,
    city,
    line_name,
    phase_name,
    analysis_region,
    construction_midpoint_year,
    context_year_candidate AS context_year,
    ABS(context_year_candidate - target_context_year) AS context_year_gap,
    gdp_per_capita_ppp_2021,
    population,
    urban_population_pct,
    length_km,
    tunnel_pct,
    tunnel_km,
    elevated_km,
    at_grade_km,
    alignment_known_pct,
    stations,
    stations_per_km,
    construction_duration_years,
    construction_decade,
    cost_per_km_2025_usd_m,
    total_cost_2025_usd_m,
    cost_per_station_2025_usd_m,
    cost_source_type,
    source_quality_score,
    source_length_type,
    reference_url,
    comparability_class,
    include_in_analysis,
    extracted_at
FROM candidates
WHERE context_rank = 1
ORDER BY analysis_region, country_code, city, line_name, phase_name
