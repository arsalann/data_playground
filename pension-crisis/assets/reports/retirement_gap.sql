SELECT
    iso3_code,
    country_name,
    region,
    retirement_age_latest,
    life_expectancy_at_65_today,
    years_in_retirement_today
FROM `bruin-playground-arsalan.staging.pc_country_pension_profile`
WHERE retirement_age_latest IS NOT NULL
  AND life_expectancy_at_65_today IS NOT NULL
ORDER BY country_name
