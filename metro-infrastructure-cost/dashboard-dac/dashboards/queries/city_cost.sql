SELECT
    city_name AS city_label,
    city_name,
    country_name,
    project_count,
    total_route_km,
    weighted_avg_cost_per_km_2025_usd_m,
    median_cost_per_km_2025_usd_m,
    weighted_avg_tunnel_pct
FROM `bruin-playground-arsalan.report.metro_city_cost_summary`
WHERE project_count > 0
{% if filters.region and filters.region | length > 0 %}
  AND analysis_region IN ('{{ filters.region | join("','") }}')
{% endif %}
ORDER BY weighted_avg_cost_per_km_2025_usd_m DESC, city_label
LIMIT 20
