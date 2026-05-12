SELECT
    iso3_code,
    country_name,
    region,
    mercer_sustainability,
    mercer_adequacy,
    mercer_overall,
    old_age_poverty_rate_latest,
    old_age_poverty_rate_year,
    net_replacement_rate_latest
FROM `bruin-playground-arsalan.staging.pc_country_pension_profile`
WHERE mercer_sustainability IS NOT NULL
  AND old_age_poverty_rate_latest IS NOT NULL
ORDER BY country_name
