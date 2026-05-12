SELECT
    iso3_code,
    country_name,
    year,
    old_age_dep_ratio,
    is_projection
FROM `bruin-playground-arsalan.staging.pc_demographics_annual`
WHERE year BETWEEN 1990 AND 2050
ORDER BY iso3_code, year
