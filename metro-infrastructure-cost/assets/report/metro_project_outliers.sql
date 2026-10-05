/* @bruin

name: report.metro_project_outliers
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Project-level outlier table for dashboard drill-down. Includes cost drivers,
  context fields, and source-quality metadata so unusually expensive or cheap
  projects remain inspectable rather than being hidden in aggregates.

depends:
  - staging.metro_projects_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: project_id
    type: VARCHAR
    description: Stable project identifier.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: project_label
    type: VARCHAR
    description: Human-readable city, line, and phase label.
  - name: city
    type: VARCHAR
    description: Project city or metropolitan area.
  - name: country_code
    type: VARCHAR
    description: Two-letter country code.
  - name: country_name
    type: VARCHAR
    description: Country name.
  - name: analysis_region
    type: VARCHAR
    description: North America, Europe, or Other.
  - name: length_km
    type: DOUBLE
    description: Corridor length in kilometers.
    checks:
      - name: positive
  - name: tunnel_pct
    type: DOUBLE
    description: Tunnel share, percent.
    checks:
      - name: min
        value: 0
      - name: max
        value: 100
  - name: stations
    type: INTEGER
    description: Number of stations.
    checks:
      - name: non_negative
  - name: construction_duration_years
    type: DOUBLE
    description: Construction duration in years.
    checks:
      - name: non_negative
  - name: cost_per_km_2025_usd_m
    type: DOUBLE
    description: Cost per kilometer in 2025 USD millions.
    checks:
      - name: positive
  - name: cost_per_station_2025_usd_m
    type: DOUBLE
    description: Total cost per station in 2025 USD millions.
  - name: cost_source_type
    type: VARCHAR
    description: Cost source quality category.
  - name: source_quality_score
    type: INTEGER
    description: Ordered cost-source quality score.
  - name: source_length_type
    type: VARCHAR
    description: Length source category.
  - name: context_year_gap
    type: INTEGER
    description: Gap between construction midpoint and context year.
  - name: reference_url
    type: VARCHAR
    description: Supporting project reference URL.

@bruin */

SELECT
    project_id,
    CONCAT(city, ' — ', line_name, IF(phase_name IS NULL OR phase_name = '', '', CONCAT(' — ', phase_name))) AS project_label,
    city,
    country_code,
    country_name,
    analysis_region,
    length_km,
    tunnel_pct,
    stations,
    construction_duration_years,
    cost_per_km_2025_usd_m,
    cost_per_station_2025_usd_m,
    cost_source_type,
    source_quality_score,
    source_length_type,
    context_year_gap,
    reference_url
FROM staging.metro_projects_enriched
WHERE include_in_analysis = TRUE
  AND cost_per_km_2025_usd_m > 0
ORDER BY cost_per_km_2025_usd_m DESC
LIMIT 50
