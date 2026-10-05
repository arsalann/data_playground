/* @bruin
name: staging.de_indicator_catalog
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Metadata for every regional indicator in staging.de_state_indicators: display labels,
  units, number formats, colour scheme, the fixed reference year used on the dashboard,
  source, licence, definition, and whether the indicator is mapped.

  The dashboard compares states only within one reference year. The target year is 2025
  for all mapped indicators except disposable income, whose latest official state-level
  release (VGRdL, June 2026) ends in 2024; it is mapped in a separate, labelled section.
  Indicators that fail the completeness rule (all 16 states in the reference year) are
  kept here with `in_dashboard = FALSE` and an exclusion reason.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: indicator_id
    type: VARCHAR
    description: Indicator identifier (join key to staging.de_state_indicators).
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: unique
  - name: display_order
    type: INTEGER
    description: Order of the indicator within the dashboard.
  - name: category
    type: VARCHAR
    description: Economy, Society, Latest available, or Supporting.
  - name: short_label
    type: VARCHAR
    description: Short English label without unit, for compact axes and table rows.
  - name: indicator_label
    type: VARCHAR
    description: Full English label including the unit.
  - name: unit_label
    type: VARCHAR
    description: Unit shown on legends and tooltips.
  - name: value_format
    type: VARCHAR
    description: d3-format string for tooltip values.
  - name: label_decimals
    type: INTEGER
    description: Number of decimals for the value labels drawn on the map and bars.
  - name: color_scheme
    type: VARCHAR
    description: Colour scale of the map; single-hue sequential blue (near-white = lowest, dark blue = highest).
  - name: reference_year
    type: INTEGER
    description: Year shown on the dashboard for this indicator.
  - name: reference_period_label
    type: VARCHAR
    description: Reference period as shown to readers (e.g. 31 Dec 2025, April 2025, 2023-2025).
  - name: source_name
    type: VARCHAR
    description: Publisher of the data.
  - name: source_table
    type: VARCHAR
    description: Source table or dataset code.
  - name: source_url
    type: VARCHAR
    description: Link to the source table or release.
  - name: license
    type: VARCHAR
    description: Licence of the source data.
  - name: definition
    type: VARCHAR
    description: Definition of the indicator.
  - name: in_dashboard
    type: BOOLEAN
    description: TRUE if the indicator is mapped on the dashboard.
  - name: exclusion_reason
    type: VARCHAR
    description: Why the indicator is not mapped (NULL when mapped).

@bruin */

SELECT *
FROM UNNEST([
    STRUCT(
        'gdp_per_capita' AS indicator_id, 1 AS display_order, 'Economy' AS category, 'GDP per inhabitant' AS short_label,
        'GDP per inhabitant (EUR, current prices)' AS indicator_label, 'EUR' AS unit_label, ',.0f' AS value_format, 0 AS label_decimals,
        'blues' AS color_scheme, 2025 AS reference_year, '2025' AS reference_period_label,
        'Arbeitskreis VGR der Länder' AS source_name, 'Reihe 1 Band 1, table 3.3' AS source_table,
        'https://www.statistikportal.de/de/vgrdl/ergebnisse-laenderebene/bruttoinlandsprodukt-bruttowertschoepfung' AS source_url,
        'dl-de/by-2-0' AS license,
        'Gross domestic product at current prices divided by the average resident population (domestic concept: output produced in the state, including by commuters from other states).' AS definition,
        TRUE AS in_dashboard, CAST(NULL AS STRING) AS exclusion_reason
    ),
    STRUCT('gross_hourly_earnings', 2, 'Economy', 'Gross hourly earnings', 'Average gross hourly earnings (EUR per hour)', 'EUR per hour', ',.2f', 2,
        'blues', 2025, 'April 2025',
        'Destatis (Verdiensterhebung)', 'GENESIS 62361-0051 / 62361-0046',
        'https://www-genesis.destatis.de/datenbank/online/table/62361-0051', 'dl-de/by-2-0',
        'Average gross hourly earnings of employees in April (monthly earnings survey, Verdiensterhebung), all industries, both sexes.',
        TRUE, NULL),
    STRUCT('gender_pay_gap', 3, 'Economy', 'Gender pay gap', 'Unadjusted gender pay gap (% of men\'s hourly earnings)', '%', '.0f', 0,
        'blues', 2025, 'April 2025',
        'Destatis (Verdiensterhebung)', 'GENESIS 62361-0051 / 62361-0046',
        'https://www-genesis.destatis.de/datenbank/online/table/62361-0051', 'dl-de/by-2-0',
        'Difference between the average gross hourly earnings of men and women, as a percentage of men\'s earnings; not adjusted for occupation, hours, or other structural factors. Published rounded to whole percent.',
        TRUE, NULL),
    STRUCT('unemployment_rate', 4, 'Economy', 'Unemployment rate', 'Registered unemployment rate (% of civilian labour force)', '%', '.1f', 1,
        'blues', 2025, '2025 annual average',
        'Bundesagentur für Arbeit via Destatis', 'GENESIS 13211-0007 / 13211-0001',
        'https://www-genesis.destatis.de/datenbank/online/table/13211-0007', 'dl-de/by-2-0',
        'Registered unemployed persons as a share of all civilian labour force (national definition, Bundesagentur für Arbeit), annual average.',
        TRUE, NULL),
    STRUCT('employment_rate_20_64', 5, 'Economy', 'Employment rate 20-64', 'Employment rate, age 20-64 (%)', '%', '.1f', 1,
        'blues', 2025, '2025',
        'Eurostat (EU Labour Force Survey)', 'lfst_r_lfe2emprt',
        'https://ec.europa.eu/eurostat/databrowser/view/lfst_r_lfe2emprt/default/table', 'CC BY 4.0',
        'Employed persons aged 20-64 as a share of the population aged 20-64 (ILO definition), place of residence.',
        TRUE, NULL),
    STRUCT('building_land_price', 6, 'Economy', 'Building land price', 'Average price of building-ready land (EUR per m²)', 'EUR per m²', ',.0f', 0,
        'blues', 2025, '2025',
        'Destatis (Kaufwerte für Bauland)', 'GENESIS 61511-0050 / 61511-0010',
        'https://www-genesis.destatis.de/datenbank/online/table/61511-0050', 'dl-de/by-2-0',
        'Average purchase value of building-ready land (baureifes Land) sold during the year, all types of building area.',
        TRUE, NULL),
    STRUCT('population_density', 7, 'Society', 'Population density', 'Population density (residents per km²)', 'per km²', ',.0f', 0,
        'blues', 2025, '31 Dec 2025',
        'Destatis (Bevölkerungsfortschreibung, Gebietsfläche)', 'GENESIS 12411-0010 / 11111-0001',
        'https://www-genesis.destatis.de/datenbank/online/table/12411-0010', 'dl-de/by-2-0',
        'Population on 31 December 2025 divided by the official land area on 31 December 2023 (latest area published).',
        TRUE, NULL),
    STRUCT('share_65_plus', 8, 'Society', 'Share aged 65+', 'Residents aged 65 and over (% of population)', '%', '.1f', 1,
        'blues', 2025, '31 Dec 2025',
        'Destatis (Bevölkerungsfortschreibung)', 'GENESIS 12411-0014',
        'https://www-genesis.destatis.de/datenbank/online/table/12411-0014', 'dl-de/by-2-0',
        'Residents aged 65 or older on 31 December as a share of all residents; population update based on the 2022 census.',
        TRUE, NULL),
    STRUCT('share_foreign_nationals', 9, 'Society', 'Share foreign nationals', 'Residents without German citizenship (% of population)', '%', '.1f', 1,
        'blues', 2025, '31 Dec 2025',
        'Destatis (Bevölkerungsfortschreibung)', 'GENESIS 12411-0014',
        'https://www-genesis.destatis.de/datenbank/online/table/12411-0014', 'dl-de/by-2-0',
        'Residents who do not hold German citizenship on 31 December as a share of all residents. Dual nationals with German citizenship count as German.',
        TRUE, NULL),
    STRUCT('total_fertility_rate', 10, 'Society', 'Fertility rate', 'Total fertility rate (children per woman)', 'children per woman', '.2f', 2,
        'blues', 2025, '2025',
        'Destatis (Statistik der Geburten)', 'GENESIS 12612-0104 / 12612-0009',
        'https://www-genesis.destatis.de/datenbank/online/table/12612-0104', 'dl-de/by-2-0',
        'Sum of age-specific birth rates of women aged 15-49 in the year: the average number of children a woman would have at that year\'s rates.',
        TRUE, NULL),
    STRUCT('net_migration_per_1000', 11, 'Society', 'Net migration rate', 'Net migration (moves in minus moves out per 1,000 residents)', 'per 1,000 residents', '.1f', 1,
        'blues', 2025, '2025',
        'Destatis (Wanderungsstatistik)', 'GENESIS 12711-0020 / 12411-0010',
        'https://www-genesis.destatis.de/datenbank/online/table/12711-0020', 'dl-de/by-2-0',
        'Moves into the state minus moves out of the state (from and to other states and abroad) during the year, per 1,000 average residents (mean of the 31 December populations of 2024 and 2025).',
        TRUE, NULL),
    STRUCT('poverty_risk_rate', 12, 'Society', 'At-risk-of-poverty rate', 'At-risk-of-poverty rate (% of population)', '%', '.1f', 1,
        'blues', 2025, '2025',
        'Eurostat (EU-SILC)', 'ilc_li41',
        'https://ec.europa.eu/eurostat/databrowser/view/ilc_li41/default/table', 'CC BY 4.0',
        'Share of persons with equivalised disposable income below 60% of the national median (EU-SILC income reference year 2024).',
        TRUE, NULL),
    STRUCT('tertiary_education_share', 13, 'Society', 'Tertiary education', 'Tertiary education, age 25-64 (%)', '%', '.1f', 1,
        'blues', 2025, '2025',
        'Eurostat (EU Labour Force Survey)', 'edat_lfse_04',
        'https://ec.europa.eu/eurostat/databrowser/view/edat_lfse_04/default/table', 'CC BY 4.0',
        'Share of residents aged 25-64 whose highest completed education is tertiary (ISCED 2011 levels 5-8, including master craftsman and technician qualifications).',
        TRUE, NULL),
    STRUCT('life_expectancy_women', 14, 'Society', 'Life expectancy, women', 'Life expectancy at birth, women (years)', 'years', '.1f', 1,
        'blues', 2025, '2023-2025',
        'Destatis (Sterbetafel)', 'GENESIS 12621-0004 / 12621-0002',
        'https://www-genesis.destatis.de/datenbank/online/table/12621-0004', 'dl-de/by-2-0',
        'Average remaining lifetime at birth from the period life table 2023/25 (three-year average; the latest published state life table).',
        TRUE, NULL),
    STRUCT('life_expectancy_men', 15, 'Society', 'Life expectancy, men', 'Life expectancy at birth, men (years)', 'years', '.1f', 1,
        'blues', 2025, '2023-2025',
        'Destatis (Sterbetafel)', 'GENESIS 12621-0004 / 12621-0002',
        'https://www-genesis.destatis.de/datenbank/online/table/12621-0004', 'dl-de/by-2-0',
        'Average remaining lifetime at birth from the period life table 2023/25 (three-year average; the latest published state life table).',
        TRUE, NULL),
    STRUCT('disposable_income_per_capita', 16, 'Latest available', 'Disposable income per inhabitant', 'Disposable income of private households per inhabitant (EUR)', 'EUR', ',.0f', 0,
        'blues', 2024, '2024',
        'Arbeitskreis VGR der Länder', 'Arbeitstabelle Primäreinkommen / Verfügbares Einkommen',
        'https://www.statistikportal.de/de/vgrdl/ergebnisse-laenderebene/einkommen', 'dl-de/by-2-0',
        'Disposable income of private households including non-profit institutions serving households, per resident (place of residence), current prices.',
        TRUE, NULL),
    STRUCT('low_wage_share', 90, 'Supporting', 'Low-wage job share', 'Jobs paid below the low-wage threshold (% of jobs)', '%', '.1f', 1,
        'blues', 2025, 'April 2025',
        'Destatis (Verdiensterhebung)', 'GENESIS 62361-0050',
        'https://www-genesis.destatis.de/datenbank/online/table/62361-0050', 'dl-de/by-2-0',
        'Share of employment relationships paid below two thirds of the national median gross hourly wage.',
        FALSE, 'Incomplete for 2025: the Mecklenburg-Vorpommern value is suppressed by Destatis (sign "/", not reliable enough).'),
    STRUCT('population', 91, 'Supporting', 'Population', 'Population (persons)', 'persons', ',.0f', 0,
        'blues', 2025, '31 Dec 2025',
        'Destatis (Bevölkerungsfortschreibung)', 'GENESIS 12411-0010',
        'https://www-genesis.destatis.de/datenbank/online/table/12411-0010', 'dl-de/by-2-0',
        'Residents on 31 December, population update based on the 2022 census.',
        FALSE, 'Used as a denominator and for context; not mapped because totals scale with state size.')
])
ORDER BY display_order
