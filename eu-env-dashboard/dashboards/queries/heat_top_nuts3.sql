/* Top NUTS3 regions by cumulative excess deaths 2015-2025. */
SELECT
    CONCAT(name_latn, ' (', country_code, ')') AS region,
    name_latn,
    country_name_en,
    ROUND(excess_deaths_total) AS excess_deaths,
    ROUND(avg_pop_total) AS avg_population,
    degurba_label,
    coast_label
FROM `bruin-playground-arsalan.eu_mortality_report.em_top_nuts3_heat_deaths`
WHERE excess_deaths_total > 0
ORDER BY excess_deaths_total DESC
LIMIT 20
