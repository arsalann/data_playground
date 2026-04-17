/* @bruin

name: staging.transit_agency_efficiency
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Annual agency-level efficiency metrics from NTD Annual Database (2022-2024).

  Aggregates per-mode data to agency level, computing key efficiency metrics:
  - Cost per trip (operating expenses / UPT)
  - Fare recovery ratio (fare revenue / operating expenses)
  - Trips per capita (UPT / UZA population)
  - Cost per vehicle revenue hour
  - Average trip length (passenger miles / UPT)

  Filters out agencies with zero ridership or zero expenses.
  Includes agency metadata for geographic analysis.

  Source: NTD 2022-2024 Annual Data - Metrics (Socrata ekg5-frzt)

depends:
  - raw.transit_ntd_annual

materialization:
  type: table
  strategy: create+replace

columns:
  - name: ntd_id
    type: VARCHAR
    description: 5-digit NTD agency identifier
    primary_key: true
  - name: report_year
    type: INTEGER
    description: Reporting year (2022-2024)
    primary_key: true
  - name: agency
    type: VARCHAR
    description: Transit agency name
  - name: city
    type: VARCHAR
    description: Agency headquarters city
  - name: state
    type: VARCHAR
    description: Two-letter US state code
  - name: uace_code
    type: VARCHAR
    description: Census Urbanized Area Code
  - name: uza_name
    type: VARCHAR
    description: Urbanized Area name
  - name: primary_uza_population
    type: INTEGER
    description: Population of the primary Urbanized Area
  - name: agency_voms
    type: INTEGER
    description: Total vehicles operated in maximum service
  - name: total_upt
    type: INTEGER
    description: Total annual Unlinked Passenger Trips across all modes
  - name: total_vrm
    type: INTEGER
    description: Total annual Vehicle Revenue Miles across all modes
  - name: total_vrh
    type: INTEGER
    description: Total annual Vehicle Revenue Hours across all modes
  - name: total_passenger_miles
    type: INTEGER
    description: Total annual passenger miles across all modes
  - name: total_fare_revenue
    type: INTEGER
    description: Total annual fare revenues earned in dollars
  - name: total_operating_expenses
    type: INTEGER
    description: Total annual operating expenses in dollars
  - name: cost_per_trip
    type: DOUBLE
    description: Operating expenses per unlinked passenger trip (dollars)
  - name: fare_recovery_ratio
    type: DOUBLE
    description: Fare revenue / operating expenses (proportion, not percentage)
  - name: trips_per_capita
    type: DOUBLE
    description: Annual UPT per person in the UZA population
  - name: cost_per_vrh
    type: DOUBLE
    description: Operating expenses per vehicle revenue hour (dollars)
  - name: avg_trip_length_miles
    type: DOUBLE
    description: Average trip length in miles (passenger miles / UPT)
  - name: expense_per_capita
    type: DOUBLE
    description: Operating expenses per person in the UZA population (dollars)
  - name: num_modes
    type: INTEGER
    description: Number of distinct transit modes operated by this agency
  - name: primary_mode
    type: VARCHAR
    description: Mode with the most ridership for this agency in this year

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.transit_ntd_annual
    WHERE ntd_id IS NOT NULL
      AND report_year IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ntd_id, report_year, mode, type_of_service
        ORDER BY extracted_at DESC
    ) = 1
),

-- Aggregate to agency-year level
agency_totals AS (
    SELECT
        ntd_id,
        report_year,
        MAX(agency) AS agency,
        MAX(city) AS city,
        MAX(state) AS state,
        MAX(uace_code) AS uace_code,
        MAX(uza_name) AS uza_name,
        MAX(primary_uza_population) AS primary_uza_population,
        MAX(agency_voms) AS agency_voms,
        SUM(COALESCE(unlinked_passenger_trips, 0)) AS total_upt,
        SUM(COALESCE(vehicle_revenue_miles, 0)) AS total_vrm,
        SUM(COALESCE(vehicle_revenue_hours, 0)) AS total_vrh,
        SUM(COALESCE(passenger_miles, 0)) AS total_passenger_miles,
        SUM(COALESCE(fare_revenues_earned, 0)) AS total_fare_revenue,
        SUM(COALESCE(total_operating_expenses, 0)) AS total_operating_expenses,
        COUNT(DISTINCT mode) AS num_modes
    FROM deduped
    GROUP BY ntd_id, report_year
),

-- Find primary mode (highest ridership) per agency-year
primary_modes AS (
    SELECT
        ntd_id,
        report_year,
        mode AS primary_mode
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ntd_id, report_year
        ORDER BY COALESCE(unlinked_passenger_trips, 0) DESC
    ) = 1
)

SELECT
    a.ntd_id,
    a.report_year,
    a.agency,
    a.city,
    a.state,
    a.uace_code,
    a.uza_name,
    a.primary_uza_population,
    a.agency_voms,
    a.total_upt,
    a.total_vrm,
    a.total_vrh,
    a.total_passenger_miles,
    a.total_fare_revenue,
    a.total_operating_expenses,

    -- Efficiency metrics
    ROUND(SAFE_DIVIDE(CAST(a.total_operating_expenses AS FLOAT64), CAST(a.total_upt AS FLOAT64)), 2) AS cost_per_trip,
    ROUND(SAFE_DIVIDE(CAST(a.total_fare_revenue AS FLOAT64), CAST(a.total_operating_expenses AS FLOAT64)), 4) AS fare_recovery_ratio,
    ROUND(SAFE_DIVIDE(CAST(a.total_upt AS FLOAT64), CAST(a.primary_uza_population AS FLOAT64)), 2) AS trips_per_capita,
    ROUND(SAFE_DIVIDE(CAST(a.total_operating_expenses AS FLOAT64), CAST(a.total_vrh AS FLOAT64)), 2) AS cost_per_vrh,
    ROUND(SAFE_DIVIDE(CAST(a.total_passenger_miles AS FLOAT64), CAST(a.total_upt AS FLOAT64)), 2) AS avg_trip_length_miles,
    ROUND(SAFE_DIVIDE(CAST(a.total_operating_expenses AS FLOAT64), CAST(a.primary_uza_population AS FLOAT64)), 2) AS expense_per_capita,
    a.num_modes,
    p.primary_mode

FROM agency_totals a
LEFT JOIN primary_modes p
    ON a.ntd_id = p.ntd_id
    AND a.report_year = p.report_year
WHERE a.total_upt > 0
  AND a.total_operating_expenses > 0
ORDER BY a.total_upt DESC
