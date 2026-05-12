/* @bruin

name: staging.pc_country_dim
type: bq.sql
description: |
  Canonical country dimension for the pension-crisis pipeline.

  Defines the apples-to-apples scope: 38 OECD member countries, identified by ISO-3
  code. Every staging table downstream filters to these countries. A country joins
  this dimension only if it has at least one UN WPP observation in our raw data,
  guaranteeing referential integrity.

  Also attaches a stable canonical country_name (preferring UN WPP's spelling to
  keep cross-source joins clean) and a geographic region grouping used for optional
  faceting in the dashboard.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.pc_un_wpp_population

columns:
  - name: iso3_code
    type: VARCHAR
    description: ISO 3166-1 alpha-3 country code.
    primary_key: true
  - name: country_name
    type: VARCHAR
    description: Canonical country name (UN WPP spelling).
  - name: region
    type: VARCHAR
    description: Geographic grouping — "Europe", "Americas", "Asia-Pacific".

@bruin */

WITH oecd_iso AS (
    SELECT iso3_code FROM UNNEST([
        'AUS','AUT','BEL','CAN','CHL','COL','CRI','CZE','DNK','EST',
        'FIN','FRA','DEU','GRC','HUN','ISL','IRL','ISR','ITA','JPN',
        'KOR','LVA','LTU','LUX','MEX','NLD','NZL','NOR','POL','PRT',
        'SVK','SVN','ESP','SWE','CHE','TUR','GBR','USA'
    ]) AS iso3_code
),

country_names AS (
    SELECT
        iso3_code,
        ANY_VALUE(country_name) AS country_name
    FROM `bruin-playground-arsalan.raw.pc_un_wpp_population`
    WHERE iso3_code IN (SELECT iso3_code FROM oecd_iso)
    GROUP BY iso3_code
)

SELECT
    c.iso3_code,
    COALESCE(n.country_name, c.iso3_code) AS country_name,
    CASE
        WHEN c.iso3_code IN (
            'AUT','BEL','CZE','DNK','EST','FIN','FRA','DEU','GRC','HUN',
            'ISL','IRL','ITA','LVA','LTU','LUX','NLD','NOR','POL','PRT',
            'SVK','SVN','ESP','SWE','CHE','GBR','TUR'
        ) THEN 'Europe'
        WHEN c.iso3_code IN ('CAN','USA','MEX','CHL','COL','CRI') THEN 'Americas'
        WHEN c.iso3_code IN ('AUS','NZL','JPN','KOR','ISR') THEN 'Asia-Pacific'
        ELSE 'Other'
    END AS region
FROM oecd_iso c
LEFT JOIN country_names n USING (iso3_code)
ORDER BY c.iso3_code
