/* @bruin
name: staging.de_state_indicators
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Long table of regional indicators for the 16 German states and Germany as a whole,
  one row per region x indicator x reference year. Combines Destatis GENESIS tables,
  the VGRdL regional accounts, and Eurostat regional survey indicators, after
  deduplicating each append-only raw table on its natural key (latest extraction wins).

  Derived indicators (computed here from published counts):
    - population_density: population on 31.12. / official land area (latest area on or
      before the reference year; Destatis area tables currently end on 31.12.2023).
    - share_65_plus, share_foreign_nationals: shares of the population on 31.12.
      (Bevoelkerungsfortschreibung, table 12411-0014).
    - net_migration_per_1000: net migration across state borders (interstate plus
      international moves) per 1,000 average residents (mean of the 31.12. populations of
      the previous and the current year).
    - low_wage_share for Germany: low-wage jobs / all jobs summed over the 16 states
      (published per state only); left empty when any state value is suppressed.
  All other values are taken as published. Codes: Destatis GENESIS table documentation
  (https://www-genesis.destatis.de), Eurostat metadata (https://ec.europa.eu/eurostat).

depends:
  - raw.de_genesis_tables
  - raw.de_vgrdl_accounts
  - raw.de_eurostat_nuts1
  - staging.de_states

materialization:
  type: table
  strategy: create+replace

columns:
  - name: region_code
    type: VARCHAR
    description: State code AGS 01-16, or DG for Germany.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: indicator_id
    type: VARCHAR
    description: Indicator identifier; see staging.de_indicator_catalog for labels, units, and sources.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: year
    type: INTEGER
    description: Reference year (for three-year life tables, the last year of the period).
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: reference_period
    type: VARCHAR
    description: Human-readable reference period, e.g. 2025, 31 Dec 2025, April 2025, 2023-2025.
  - name: value
    type: DOUBLE
    description: Indicator value in the unit given by `unit`.
    checks:
      - name: not_null
  - name: unit
    type: VARCHAR
    description: Unit of `value` (EUR, EUR per hour, EUR per m², %, per km², per 1,000 residents, children per woman, years, persons).
  - name: source_id
    type: VARCHAR
    description: destatis_genesis, vgrdl, or eurostat.
    checks:
      - name: accepted_values
        value: [destatis_genesis, vgrdl, eurostat]
  - name: source_table
    type: VARCHAR
    description: Source table or dataset code (GENESIS table, VGRdL workbook sheet, Eurostat dataset).
  - name: status_flag
    type: VARCHAR
    description: Eurostat observation flag where present (b = break in series, u = low reliability, p = provisional); NULL otherwise.

@bruin */

WITH genesis AS (
    SELECT
        table_code,
        time_value,
        SAFE_CAST(SUBSTR(time_value, 1, 4) AS INT64) AS year,
        IF(region_code IN ('', 'DG'), 'DG', region_code) AS region_code,
        dim1_value_code,
        dim2_value_code,
        dim3_value_code,
        value_variable_code,
        value
    FROM raw.de_genesis_tables
    WHERE value IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY table_code, time_value, region_code, dim1_value_code, dim2_value_code, dim3_value_code, value_variable_code
        ORDER BY extracted_at DESC
    ) = 1
),

vgrdl AS (
    SELECT indicator_code, region_name, year, value
    FROM raw.de_vgrdl_accounts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY indicator_code, region_name, year ORDER BY extracted_at DESC) = 1
),

eurostat AS (
    SELECT dataset_code, geo, year, value, status_flag
    FROM raw.de_eurostat_nuts1
    WHERE value IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dataset_code, geo, year ORDER BY extracted_at DESC) = 1
),

-- Population on 31.12. per state; Germany = sum of the 16 states.
population_states AS (
    SELECT region_code, year, value AS population
    FROM genesis
    WHERE table_code = '12411-0010' AND value_variable_code = 'BEVSTD' AND region_code != 'DG'
),

population_all AS (
    SELECT region_code, year, population FROM population_states
    UNION ALL
    SELECT 'DG', year, SUM(population) FROM population_states GROUP BY year HAVING COUNT(*) = 16
),

area AS (
    SELECT region_code, year, value AS area_km2
    FROM genesis
    WHERE table_code = '11111-0001' AND value_variable_code = 'FLC006'
),

density AS (
    SELECT
        p.region_code,
        p.year,
        p.population / a.area_km2 AS value,
        a.year AS area_year
    FROM population_all AS p
    INNER JOIN area AS a
        ON a.region_code = p.region_code AND a.year <= p.year
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.region_code, p.year ORDER BY a.year DESC) = 1
),

-- 12411-0014: dim1 = nationality (''/NATD/NATA), dim2 = sex (''/GESM/GESW), dim3 = single age year.
age_nationality AS (
    SELECT
        region_code,
        year,
        SUM(IF(dim1_value_code = '' AND dim3_value_code = '', value, 0)) AS total,
        SUM(IF(dim1_value_code = '' AND SAFE_CAST(SUBSTR(dim3_value_code, 4, 3) AS INT64) >= 65, value, 0)) AS aged_65_plus,
        SUM(IF(dim1_value_code = 'NATA' AND dim3_value_code = '', value, 0)) AS foreigners
    FROM genesis
    WHERE table_code = '12411-0014' AND dim2_value_code = '' AND value_variable_code = 'BEVSTD'
        AND region_code != 'DG'
    GROUP BY region_code, year
),

age_nationality_de AS (
    SELECT * FROM age_nationality
    UNION ALL
    SELECT 'DG', year, SUM(total), SUM(aged_65_plus), SUM(foreigners)
    FROM age_nationality
    GROUP BY year
    HAVING COUNT(*) = 16
),

migration AS (
    SELECT
        m.region_code,
        m.year,
        m.value * 1000 / ((p_prev.population + p_cur.population) / 2) AS value
    FROM genesis AS m
    INNER JOIN population_all AS p_cur
        ON p_cur.region_code = m.region_code AND p_cur.year = m.year
    INNER JOIN population_all AS p_prev
        ON p_prev.region_code = m.region_code AND p_prev.year = m.year - 1
    WHERE m.table_code = '12711-0020'
        AND m.value_variable_code = 'BEV015'
        AND m.dim1_value_code = ''
        AND m.dim2_value_code = ''
),

low_wage AS (
    SELECT region_code, year, value
    FROM genesis
    WHERE table_code = '62361-0050' AND value_variable_code = 'BES033' AND dim1_value_code = ''
    UNION ALL
    SELECT
        'DG',
        year,
        100 * SUM(IF(value_variable_code = 'BES032', value, 0)) / SUM(IF(value_variable_code = 'BES031', value, 0))
    FROM genesis
    WHERE table_code = '62361-0050' AND dim1_value_code = '' AND value_variable_code IN ('BES031', 'BES032')
    GROUP BY year
    HAVING COUNTIF(value_variable_code = 'BES031') = 16 AND COUNTIF(value_variable_code = 'BES032') = 16
),

indicators AS (
    -- Destatis GENESIS: published values
    SELECT region_code, 'unemployment_rate' AS indicator_id, year, CAST(year AS STRING) AS reference_period,
        value, '%' AS unit, 'destatis_genesis' AS source_id, table_code AS source_table, CAST(NULL AS STRING) AS status_flag
    FROM genesis
    WHERE (table_code = '13211-0007' AND value_variable_code = 'ERW112')
        OR (table_code = '13211-0001' AND value_variable_code = 'ERW112' AND dim1_value_code = '')

    UNION ALL
    SELECT region_code, 'total_fertility_rate', year, CAST(year AS STRING),
        value, 'children per woman', 'destatis_genesis', table_code, NULL
    FROM genesis
    WHERE table_code IN ('12612-0104', '12612-0009') AND value_variable_code = 'BEV066' AND dim1_value_code = 'ALT015B50'

    UNION ALL
    SELECT region_code, 'building_land_price', year, CAST(year AS STRING),
        value, 'EUR per m²', 'destatis_genesis', table_code, NULL
    FROM genesis
    WHERE table_code IN ('61511-0050', '61511-0010')
        AND value_variable_code = 'KAU004' AND dim1_value_code = '' AND dim2_value_code = 'BAULAND01'

    UNION ALL
    SELECT region_code, 'gross_hourly_earnings', year, CONCAT('April ', CAST(year AS STRING)),
        value, 'EUR per hour', 'destatis_genesis', table_code, NULL
    FROM genesis
    WHERE table_code IN ('62361-0051', '62361-0046')
        AND value_variable_code = 'VST007' AND dim1_value_code = '' AND dim2_value_code = ''

    UNION ALL
    SELECT region_code, 'gender_pay_gap', year, CONCAT('April ', CAST(year AS STRING)),
        value, '%', 'destatis_genesis', table_code, NULL
    FROM genesis
    WHERE table_code IN ('62361-0051', '62361-0046')
        AND value_variable_code = 'VST100' AND dim1_value_code = ''

    UNION ALL
    SELECT region_code, IF(dim1_value_code = 'GESW', 'life_expectancy_women', 'life_expectancy_men'),
        year + 2, CONCAT(CAST(year AS STRING), '-', CAST(year + 2 AS STRING)),
        value, 'years', 'destatis_genesis', table_code, NULL
    FROM genesis
    WHERE (table_code = '12621-0004' AND value_variable_code = 'LEB008')
        OR (table_code = '12621-0002' AND value_variable_code = 'LEB007' AND dim2_value_code = 'ALTVOLL000')

    -- Destatis GENESIS: derived values
    UNION ALL
    SELECT region_code, 'population', year, CONCAT('31 Dec ', CAST(year AS STRING)),
        population, 'persons', 'destatis_genesis', '12411-0010', NULL
    FROM population_all

    UNION ALL
    SELECT region_code, 'population_density', year, CONCAT('31 Dec ', CAST(year AS STRING)),
        value, 'per km²', 'destatis_genesis', CONCAT('12411-0010 / 11111-0001 (area ', CAST(area_year AS STRING), ')'), NULL
    FROM density

    UNION ALL
    SELECT region_code, 'share_65_plus', year, CONCAT('31 Dec ', CAST(year AS STRING)),
        100 * aged_65_plus / total, '%', 'destatis_genesis', '12411-0014', NULL
    FROM age_nationality_de
    WHERE total > 0

    UNION ALL
    SELECT region_code, 'share_foreign_nationals', year, CONCAT('31 Dec ', CAST(year AS STRING)),
        100 * foreigners / total, '%', 'destatis_genesis', '12411-0014', NULL
    FROM age_nationality_de
    WHERE total > 0

    UNION ALL
    SELECT region_code, 'net_migration_per_1000', year, CAST(year AS STRING),
        value, 'per 1,000 residents', 'destatis_genesis', '12711-0020 / 12411-0010', NULL
    FROM migration

    UNION ALL
    SELECT region_code, 'low_wage_share', year, CONCAT('April ', CAST(year AS STRING)),
        value, '%', 'destatis_genesis', '62361-0050', NULL
    FROM low_wage

    -- VGRdL regional accounts
    UNION ALL
    SELECT IF(v.region_name = 'Deutschland', 'DG', s.ags_code), v.indicator_code, v.year, CAST(v.year AS STRING),
        v.value, 'EUR', 'vgrdl', IF(v.indicator_code = 'gdp_per_capita', 'R1B1 sheet 3.3', 'Arbeitstabelle PEK/VEK'), NULL
    FROM vgrdl AS v
    LEFT JOIN staging.de_states AS s
        ON s.state_name = v.region_name
    WHERE v.region_name = 'Deutschland' OR s.ags_code IS NOT NULL

    -- Eurostat regional survey indicators
    UNION ALL
    SELECT IF(e.geo = 'DE', 'DG', s.ags_code),
        CASE e.dataset_code
            WHEN 'lfst_r_lfe2emprt' THEN 'employment_rate_20_64'
            WHEN 'edat_lfse_04' THEN 'tertiary_education_share'
            WHEN 'ilc_li41' THEN 'poverty_risk_rate'
        END,
        e.year, CAST(e.year AS STRING), e.value, '%', 'eurostat', e.dataset_code, e.status_flag
    FROM eurostat AS e
    LEFT JOIN staging.de_states AS s
        ON s.nuts1_id = e.geo
    WHERE e.geo = 'DE' OR s.ags_code IS NOT NULL
)

SELECT
    region_code,
    indicator_id,
    year,
    reference_period,
    value,
    unit,
    source_id,
    source_table,
    status_flag
FROM indicators
WHERE value IS NOT NULL
    AND (region_code = 'DG' OR region_code IN (SELECT ags_code FROM staging.de_states))
ORDER BY indicator_id, year, region_code
