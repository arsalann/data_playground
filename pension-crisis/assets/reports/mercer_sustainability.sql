SELECT
    iso3_code,
    country_name,
    region,
    mercer_overall,
    mercer_adequacy,
    mercer_sustainability,
    mercer_integrity,
    old_age_dep_ratio_2050,
    pension_assets_pct_gdp_latest
FROM `bruin-playground-arsalan.staging.pc_country_pension_profile`
WHERE mercer_sustainability IS NOT NULL
ORDER BY country_name
