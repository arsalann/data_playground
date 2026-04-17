/* @bruin

name: staging.transit_metro_comparison
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Metro-area level transit comparison combining NTD agency data with Census ACS
  commuting mode share and work-from-home rates.

  Aggregates NTD agencies to Urbanized Area (UZA) level, then joins with Census
  ACS Metropolitan Statistical Area (MSA) data using fuzzy name matching on the
  primary city name. One row per metro area per year.

  Key metrics per metro:
  - Total ridership and operating expenses
  - Aggregate cost per trip and fare recovery
  - Census transit mode share and WFH rate
  - Ridership recovery vs 2019 baseline

  Note: UZA-to-MSA matching is approximate. Some metros may not match due to
  naming differences between NTD UZA names and Census MSA names.

  Sources: NTD Annual Metrics + NTD Monthly Module + Census ACS B08301

depends:
  - raw.transit_ntd_annual
  - raw.transit_ntd_monthly
  - raw.transit_census_acs

materialization:
  type: table
  strategy: create+replace

columns:
  - name: uza_name
    type: VARCHAR
    description: NTD Urbanized Area name (join key for metro-level aggregation)
    primary_key: true
  - name: uace_code
    type: VARCHAR
    description: Census Urbanized Area Code
  - name: report_year
    type: INTEGER
    description: Reporting year
    primary_key: true
  - name: uza_population
    type: INTEGER
    description: Population of the Urbanized Area from NTD
  - name: num_agencies
    type: INTEGER
    description: Number of transit agencies in this metro area
  - name: total_upt
    type: INTEGER
    description: Total annual ridership (UPT) across all agencies in metro
  - name: total_operating_expenses
    type: INTEGER
    description: Total annual operating expenses across all agencies in metro
  - name: total_fare_revenue
    type: INTEGER
    description: Total annual fare revenues across all agencies in metro
  - name: total_vrm
    type: INTEGER
    description: Total annual Vehicle Revenue Miles across all agencies in metro
  - name: total_vrh
    type: INTEGER
    description: Total annual Vehicle Revenue Hours across all agencies in metro
  - name: metro_cost_per_trip
    type: DOUBLE
    description: Metro-wide operating expenses per UPT (dollars)
  - name: metro_fare_recovery
    type: DOUBLE
    description: Metro-wide fare recovery ratio (fare revenue / operating expenses)
  - name: trips_per_capita
    type: DOUBLE
    description: Annual UPT per person in the UZA
  - name: expense_per_capita
    type: DOUBLE
    description: Annual operating expenses per person in the UZA (dollars)
  - name: msa_name
    type: VARCHAR
    description: Matched Census MSA name (NULL if no match found)
  - name: total_workers
    type: INTEGER
    description: Total workers in matched MSA from Census ACS
  - name: transit_commuters
    type: INTEGER
    description: Public transit commuters in matched MSA from Census ACS
  - name: transit_mode_share_pct
    type: DOUBLE
    description: Percentage of workers commuting by public transit (Census ACS)
  - name: wfh_rate_pct
    type: DOUBLE
    description: Percentage of workers working from home (Census ACS)
  - name: ridership_2019
    type: INTEGER
    description: Total annual ridership in 2019 for this metro area (from monthly data)
  - name: recovery_pct
    type: DOUBLE
    description: Ridership recovery percentage vs 2019 (total_upt / ridership_2019 * 100)

@bruin */

WITH -- Aggregate annual NTD data to metro (UZA) level
annual_deduped AS (
    SELECT *
    FROM raw.transit_ntd_annual
    WHERE ntd_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ntd_id, report_year, mode, type_of_service
        ORDER BY extracted_at DESC
    ) = 1
),

metro_annual AS (
    SELECT
        uza_name,
        MAX(uace_code) AS uace_code,
        report_year,
        MAX(primary_uza_population) AS uza_population,
        COUNT(DISTINCT ntd_id) AS num_agencies,
        SUM(COALESCE(unlinked_passenger_trips, 0)) AS total_upt,
        SUM(COALESCE(total_operating_expenses, 0)) AS total_operating_expenses,
        SUM(COALESCE(fare_revenues_earned, 0)) AS total_fare_revenue,
        SUM(COALESCE(vehicle_revenue_miles, 0)) AS total_vrm,
        SUM(COALESCE(vehicle_revenue_hours, 0)) AS total_vrh
    FROM annual_deduped
    WHERE uza_name IS NOT NULL
      AND uza_name != ''
    GROUP BY uza_name, report_year
    HAVING SUM(COALESCE(unlinked_passenger_trips, 0)) > 0
),

-- Get 2019 annual ridership from monthly data for recovery calculation
monthly_deduped AS (
    SELECT *
    FROM raw.transit_ntd_monthly
    WHERE ntd_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ntd_id, mode, tos, report_month
        ORDER BY extracted_at DESC
    ) = 1
),

ridership_2019 AS (
    SELECT
        uza_name,
        SUM(COALESCE(upt, 0)) AS ridership_2019
    FROM monthly_deduped
    WHERE EXTRACT(YEAR FROM report_month) = 2019
      AND uza_name IS NOT NULL
      AND uza_name != ''
    GROUP BY uza_name
),

-- Prepare Census ACS data - extract primary city name for fuzzy matching
census_deduped AS (
    SELECT *
    FROM raw.transit_census_acs
    WHERE msa_code IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY msa_code, year
        ORDER BY extracted_at DESC
    ) = 1
),

-- Extract the primary city from MSA name for matching
-- e.g. "New York-Newark-Jersey City, NY-NJ-PA Metro Area" -> "New York"
census_with_city AS (
    SELECT
        *,
        TRIM(SPLIT(SPLIT(msa_name, ',')[SAFE_OFFSET(0)], '-')[SAFE_OFFSET(0)]) AS primary_city,
        TRIM(SPLIT(msa_name, ',')[SAFE_OFFSET(1)]) AS state_abbrevs
    FROM census_deduped
),

-- Extract primary city from UZA name for matching
-- e.g. "New York--Newark, NY--NJ--CT" -> "New York"
uza_cities AS (
    SELECT DISTINCT
        uza_name,
        TRIM(SPLIT(SPLIT(uza_name, ',')[SAFE_OFFSET(0)], '--')[SAFE_OFFSET(0)]) AS uza_primary_city
    FROM metro_annual
),

-- Match UZA to MSA using primary city name, deduplicate to best match per UZA
-- Use largest MSA (by total_workers) when multiple MSAs match the same city name
uza_msa_match AS (
    SELECT
        u.uza_name,
        c.msa_code,
        c.msa_name,
        c.year AS census_year,
        c.total_workers,
        c.transit_commuters,
        c.transit_mode_share_pct,
        c.wfh_rate_pct
    FROM uza_cities u
    INNER JOIN census_with_city c
        ON LOWER(u.uza_primary_city) = LOWER(c.primary_city)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY u.uza_name, c.year
        ORDER BY c.total_workers DESC
    ) = 1
),

-- For years without Census data (e.g. 2024), carry forward the latest available year
census_latest AS (
    SELECT
        uza_name,
        msa_code,
        msa_name,
        census_year,
        total_workers,
        transit_commuters,
        transit_mode_share_pct,
        wfh_rate_pct
    FROM uza_msa_match
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY uza_name
        ORDER BY census_year DESC
    ) = 1
)

SELECT
    m.uza_name,
    m.uace_code,
    m.report_year,
    m.uza_population,
    m.num_agencies,
    m.total_upt,
    m.total_operating_expenses,
    m.total_fare_revenue,
    m.total_vrm,
    m.total_vrh,
    ROUND(SAFE_DIVIDE(CAST(m.total_operating_expenses AS FLOAT64), CAST(m.total_upt AS FLOAT64)), 2) AS metro_cost_per_trip,
    ROUND(SAFE_DIVIDE(CAST(m.total_fare_revenue AS FLOAT64), CAST(m.total_operating_expenses AS FLOAT64)), 4) AS metro_fare_recovery,
    ROUND(SAFE_DIVIDE(CAST(m.total_upt AS FLOAT64), CAST(m.uza_population AS FLOAT64)), 2) AS trips_per_capita,
    ROUND(SAFE_DIVIDE(CAST(m.total_operating_expenses AS FLOAT64), CAST(m.uza_population AS FLOAT64)), 2) AS expense_per_capita,
    COALESCE(c.msa_name, cl.msa_name) AS msa_name,
    COALESCE(c.total_workers, cl.total_workers) AS total_workers,
    COALESCE(c.transit_commuters, cl.transit_commuters) AS transit_commuters,
    COALESCE(c.transit_mode_share_pct, cl.transit_mode_share_pct) AS transit_mode_share_pct,
    COALESCE(c.wfh_rate_pct, cl.wfh_rate_pct) AS wfh_rate_pct,
    r.ridership_2019,
    ROUND(SAFE_DIVIDE(CAST(m.total_upt AS FLOAT64), CAST(r.ridership_2019 AS FLOAT64)) * 100, 1) AS recovery_pct
FROM metro_annual m
LEFT JOIN uza_msa_match c
    ON m.uza_name = c.uza_name
    AND c.census_year = m.report_year
LEFT JOIN census_latest cl
    ON m.uza_name = cl.uza_name
    AND c.msa_name IS NULL
LEFT JOIN ridership_2019 r
    ON m.uza_name = r.uza_name
ORDER BY m.total_upt DESC
