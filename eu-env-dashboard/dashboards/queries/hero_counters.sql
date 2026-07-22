/* Hero counters: top-line numbers for the dashboard header. */
SELECT
    (SELECT COUNT(*) FROM `bruin-playground-arsalan.eu_pfas_staging.pf_sites_dim`)
        AS pfas_sites_total,
    (SELECT COUNT(*) FROM `bruin-playground-arsalan.eu_pfas_staging.pf_sites_dim`
        WHERE pfas_sum_ng_l >= 500 AND is_water_sample)
        AS pfas_exceedances_total,
    (SELECT SUM(IFNULL(excess_deaths, 0))
        FROM `bruin-playground-arsalan.eu_mortality_staging.em_heat_attribution`
        WHERE iso_year = 2022)
        AS excess_deaths_2022,
    (SELECT COUNT(DISTINCT nuts_id)
        FROM `bruin-playground-arsalan.eu_env_dashboard_report.env_burden_nuts3`
        WHERE n_exceed_eu_dwd > 0 OR excess_deaths_total > 0)
        AS nuts3_with_burden,
    (SELECT MAX(max_pfas_ng_l_water)
        FROM `bruin-playground-arsalan.eu_pfas_staging.pf_nuts3_exposure`)
        AS max_pfas_ng_l
