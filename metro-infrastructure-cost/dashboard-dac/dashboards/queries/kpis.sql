SELECT
    COUNT(*) AS included_projects,
    COUNT(DISTINCT CONCAT(country_code, '::', city)) AS included_cities,
    SUM(length_km) AS route_km,
    SAFE_DIVIDE(
        SUM(cost_per_km_2025_usd_m * length_km),
        SUM(length_km)
    ) AS weighted_avg_cost_per_km
FROM `bruin-playground-arsalan.staging.metro_projects_enriched`
WHERE include_in_analysis = TRUE
{% if filters.region and filters.region | length > 0 %}
  AND analysis_region IN ('{{ filters.region | join("','") }}')
{% endif %}
