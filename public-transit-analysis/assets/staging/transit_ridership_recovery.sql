/* @bruin

name: staging.transit_ridership_recovery
type: bq.sql
description: |
  Monthly US public transit ridership recovery analysis comparing current performance
  to 2019 pre-COVID baselines by agency, mode, and geographic area.

  This asset processes National Transit Database (NTD) monthly data to compute recovery
  percentages for both ridership (UPT) and service delivery (VRM). Key transformations:

  - Deduplicates raw monthly reports by latest extraction timestamp
  - Filters to major transit modes (excludes rare/specialty modes like cable cars)
  - Aggregates directly operated (DO) and purchased transportation (PT) services
  - Computes same-month-of-year baselines from 2019 data
  - Calculates recovery percentages with SAFE_DIVIDE to handle edge cases

  Recovery metrics help analyze COVID-19 impact patterns, mode-specific resilience,
  and geographic variation in transit system restoration. Values >100% indicate
  ridership growth beyond pre-pandemic levels.

  Known limitations: ~15% of recovery calculations are null due to agencies that
  started reporting after 2019 or had zero ridership in baseline months. Extreme
  outliers (>1000% recovery) often reflect service launches or data quality issues.

  Source: NTD Monthly Module via Socrata API (January 2002-present, ~365K raw rows)
  Pipeline: Processes 832 transit agencies across 13 major mode categories
connection: bruin-playground-arsalan
tags:
  - domain:public_transit
  - domain:transportation
  - domain:government
  - data_type:fact_table
  - data_type:time_series
  - pipeline_role:staging
  - sensitivity:public
  - update_pattern:batch
  - analysis_type:recovery_tracking

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.transit_ntd_monthly

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: ntd_id
    type: VARCHAR
    description: |
      5-digit National Transit Database agency identifier (semantic type: identifier).
      Uniquely identifies transit agencies in federal reporting system.
      Format: always exactly 5 digits (e.g., "00001" = King County Metro).
      Cardinality: ~832 active agencies nationwide.
    primary_key: true
    checks:
      - name: not_null
  - name: agency
    type: VARCHAR
    description: |
      Transit agency name as officially reported to NTD (semantic type: dimension).
      May include legal suffixes, abbreviations, or regional designators.
      Examples: "King County Metro", "Metropolitan Transportation Authority".
    checks:
      - name: not_null
  - name: mode
    type: VARCHAR
    description: |
      NTD standardized transit mode code (semantic type: category).
      13 major modes included: HR=Heavy Rail, LR=Light Rail, CR=Commuter Rail,
      MB=Bus, DR=Demand Response, CB=Commuter Bus, RB=Bus Rapid Transit,
      TB=Trolleybus, FB=Ferryboat, SR=Streetcar, VP=Vanpool, YR=Hybrid Rail, MG=Monorail.
    primary_key: true
    checks:
      - name: not_null
  - name: mode_name
    type: VARCHAR
    description: |
      Human-readable transit mode description (semantic type: dimension).
      Provides user-friendly labels for mode codes.
      Examples: "Heavy Rail", "Bus", "Light Rail", "Ferryboat".
    checks:
      - name: not_null
  - name: report_month
    type: DATE
    description: |
      First day of the service month being reported (semantic type: temporal_dimension).
      Format: YYYY-MM-01. Time series spans January 2002 to present with ~290 unique months.
      Used as temporal grain for recovery percentage calculations.
    primary_key: true
    checks:
      - name: not_null
  - name: report_year
    type: INTEGER
    description: |
      Calendar year extracted from report_month (semantic type: temporal_dimension).
      Derived field for temporal aggregation and filtering.
      Range: 2002-2026 based on data span.
    checks:
      - name: not_null
  - name: report_month_num
    type: INTEGER
    description: |
      Month number (1-12) extracted from report_month (semantic type: temporal_dimension).
      Used for same-month-of-year baseline comparisons (Jan-to-Jan, Feb-to-Feb, etc.).
      Essential for seasonality-adjusted recovery calculations.
    checks:
      - name: not_null
  - name: uza_name
    type: VARCHAR
    description: |
      Census Urbanized Area name (semantic type: geographic_dimension).
      Represents metropolitan regions as defined by US Census for population density.
      Examples: "New York--Newark, NY--NJ--CT", "Los Angeles--Long Beach--Anaheim, CA".
      Cardinality: ~395 urbanized areas with transit service.
    checks:
      - name: not_null
  - name: uace_cd
    type: VARCHAR
    description: |
      Census Urbanized Area Code (semantic type: identifier).
      3-5 digit numeric identifier corresponding to uza_name.
      Links to broader Census geographic hierarchy and demographic data.
    checks:
      - name: not_null
  - name: state
    type: VARCHAR
    description: |
      Two-letter US state abbreviation (semantic type: geographic_dimension).
      Standard ANSI state codes including DC for District of Columbia.
      Cardinality: 53 unique values (50 states + DC + territories).
    checks:
      - name: not_null
  - name: upt
    type: INTEGER
    description: |
      Unlinked Passenger Trips for the reporting month (semantic type: metric).
      Each individual boarding counts as one trip regardless of transfers.
      Units: count of boardings. Aggregated from DO+PT service types.
      Can be zero for agencies with suspended service but never negative.
    checks:
      - name: not_null
      - name: non_negative
  - name: vrm
    type: INTEGER
    description: |
      Vehicle Revenue Miles for the reporting month (semantic type: metric).
      Total miles traveled while in active passenger service.
      Units: miles. Excludes deadhead/positioning moves. Service delivery indicator.
    checks:
      - name: not_null
      - name: non_negative
  - name: voms
    type: INTEGER
    description: |
      Vehicles Operated in Maximum Service (semantic type: metric).
      Peak fleet size during heaviest service period of the month.
      Units: count of vehicles. Capacity/resource utilization indicator.
    checks:
      - name: not_null
      - name: non_negative
  - name: baseline_2019_upt
    type: INTEGER
    description: |
      UPT baseline from same calendar month in 2019 (semantic type: reference_metric).
      Denominator for recovery percentage calculation. Pre-COVID ridership baseline.
      Null for agencies that began reporting after 2019 (~15% of records).
      Units: count of boardings.
    checks:
      - name: non_negative
  - name: recovery_pct
    type: DOUBLE
    description: |
      Ridership recovery percentage vs 2019 baseline (semantic type: derived_metric).
      Formula: (current_upt / baseline_2019_upt) * 100.
      100% = full recovery, >100% = growth beyond pre-pandemic levels.
      Null when baseline unavailable. Extreme values (>1000%) indicate data quality issues.
      Units: percentage (0-35000+ observed range, median ~101%).
    checks:
      - name: non_negative
  - name: baseline_2019_vrm
    type: INTEGER
    description: |
      VRM baseline from same calendar month in 2019 (semantic type: reference_metric).
      Service delivery baseline for recovery analysis. Pre-COVID service levels.
      Null pattern matches baseline_2019_upt (~15% of records).
      Units: miles.
    checks:
      - name: non_negative
  - name: vrm_recovery_pct
    type: DOUBLE
    description: |
      Vehicle revenue miles recovery percentage vs 2019 baseline (semantic type: derived_metric).
      Formula: (current_vrm / baseline_2019_vrm) * 100.
      Measures service delivery restoration independent of ridership demand.
      Often recovers faster than ridership as agencies restore routes before passengers return.
      Units: percentage.
    checks:
      - name: non_negative

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.transit_ntd_monthly
    WHERE ntd_id IS NOT NULL
      AND mode IS NOT NULL
      AND report_month IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ntd_id, mode, tos, report_month
        ORDER BY extracted_at DESC
    ) = 1
),

-- Filter to major transit modes and standard service types
filtered AS (
    SELECT
        ntd_id,
        agency,
        mode,
        mode_name,
        report_month,
        EXTRACT(YEAR FROM report_month) AS report_year,
        EXTRACT(MONTH FROM report_month) AS report_month_num,
        uza_name,
        uace_cd,
        state,
        -- Aggregate DO + PT for same agency/mode/month
        SUM(COALESCE(upt, 0)) AS upt,
        SUM(COALESCE(vrm, 0)) AS vrm,
        MAX(COALESCE(voms, 0)) AS voms
    FROM deduped
    WHERE mode IN ('HR', 'LR', 'CR', 'MB', 'CB', 'RB', 'SR', 'FB', 'TB', 'DR', 'VP', 'YR', 'MG')
      AND tos IN ('DO', 'PT', 'TX', 'TN')
    GROUP BY ntd_id, agency, mode, mode_name, report_month, uza_name, uace_cd, state
),

-- Get 2019 baseline for each agency/mode/month-of-year
baseline_2019 AS (
    SELECT
        ntd_id,
        mode,
        report_month_num,
        upt AS baseline_2019_upt,
        vrm AS baseline_2019_vrm
    FROM filtered
    WHERE report_year = 2019
)

SELECT
    f.ntd_id,
    f.agency,
    f.mode,
    f.mode_name,
    f.report_month,
    f.report_year,
    f.report_month_num,
    f.uza_name,
    f.uace_cd,
    f.state,
    f.upt,
    f.vrm,
    f.voms,
    b.baseline_2019_upt,
    ROUND(
        SAFE_DIVIDE(CAST(f.upt AS FLOAT64), CAST(b.baseline_2019_upt AS FLOAT64)) * 100,
        1
    ) AS recovery_pct,
    b.baseline_2019_vrm,
    ROUND(
        SAFE_DIVIDE(CAST(f.vrm AS FLOAT64), CAST(b.baseline_2019_vrm AS FLOAT64)) * 100,
        1
    ) AS vrm_recovery_pct
FROM filtered f
LEFT JOIN baseline_2019 b
    ON f.ntd_id = b.ntd_id
    AND f.mode = b.mode
    AND f.report_month_num = b.report_month_num
ORDER BY f.ntd_id, f.mode, f.report_month
