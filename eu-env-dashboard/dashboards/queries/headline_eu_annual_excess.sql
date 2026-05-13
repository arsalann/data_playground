/* EU-27 annual all-cause excess deaths above the 2015-2019 climatological
   baseline. Aggregated across all NUTS3 reporting weekly mortality. */
SELECT
    iso_year AS year,
    ROUND(SUM(IFNULL(excess_deaths, 0))) AS excess_deaths
FROM `bruin-playground-arsalan.eu_mortality_staging.em_heat_attribution`
GROUP BY iso_year
ORDER BY iso_year
