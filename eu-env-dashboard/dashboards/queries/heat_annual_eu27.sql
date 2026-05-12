/* Annual excess deaths across the EU-27, all NUTS3 summed. */
SELECT
    iso_year AS year,
    ROUND(SUM(IFNULL(excess_deaths, 0))) AS excess_deaths,
    ROUND(SUM(IFNULL(heat_attributable_excess, 0))) AS heat_attributable_excess,
    SUM(IFNULL(deaths_total, 0)) AS deaths_observed,
    SUM(IFNULL(expected_deaths, 0)) AS deaths_expected
FROM `bruin-playground-arsalan.eu_mortality_staging.em_heat_attribution`
GROUP BY iso_year
ORDER BY iso_year
