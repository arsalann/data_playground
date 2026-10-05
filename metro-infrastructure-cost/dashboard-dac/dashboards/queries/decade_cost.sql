SELECT
    construction_decade,
    COUNT(*) AS project_count,
    SUM(length_km) AS total_route_km,
    SAFE_DIVIDE(
        SUM(cost_per_km_2025_usd_m * length_km),
        SUM(length_km)
    ) AS weighted_avg_cost_per_km_2025_usd_m,
    SAFE_DIVIDE(SUM(tunnel_pct * length_km), SUM(length_km)) AS weighted_avg_tunnel_pct
FROM `bruin-playground-arsalan.staging.metro_projects_enriched`
WHERE include_in_analysis = TRUE
  AND construction_decade != 'Unknown'
{% if filters.region and filters.region | length > 0 %}
  AND analysis_region IN ('{{ filters.region | join("','") }}')
{% endif %}
GROUP BY construction_decade
ORDER BY MIN(construction_midpoint_year)
