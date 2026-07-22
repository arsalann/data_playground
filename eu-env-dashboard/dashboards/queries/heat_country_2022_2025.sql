/* Country-year excess deaths 2022-2025 (heatwave-heavy window).
   Pivoted wide so the chart can show country bars stacked by year. */
SELECT
    country_name_en AS country,
    ROUND(SUM(IF(year = 2022, total_excess_deaths, 0))) AS y_2022,
    ROUND(SUM(IF(year = 2023, total_excess_deaths, 0))) AS y_2023,
    ROUND(SUM(IF(year = 2024, total_excess_deaths, 0))) AS y_2024,
    ROUND(SUM(IF(year = 2025, total_excess_deaths, 0))) AS y_2025,
    ROUND(SUM(total_excess_deaths)) AS total_excess_2022_2025
FROM `bruin-playground-arsalan.eu_mortality_report.em_country_annual_summary`
WHERE year BETWEEN 2022 AND 2025
GROUP BY country_name_en
ORDER BY total_excess_2022_2025 DESC
