SELECT
    project_label,
    city,
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
FROM `bruin-playground-arsalan.report.metro_project_outliers`
WHERE TRUE
{% if filters.region and filters.region | length > 0 %}
  AND analysis_region IN ('{{ filters.region | join("','") }}')
{% endif %}
ORDER BY cost_per_km_2025_usd_m DESC
LIMIT 25
