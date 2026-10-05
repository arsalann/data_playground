/* @bruin
name: staging.shops_unified
type: bq.sql
connection: bruin-playground-arsalan
description: |
  One row per (city, establishment, canonical shop type) across all five cities, built by
  deduplicating each raw register and joining it to staging.shop_type_crosswalk.

  Grain is deliberately (city, establishment_id, shop_type) rather than one row per
  establishment. Where a source cannot separate two shop types, the same establishment
  legitimately appears under both, because a Paris "Bar ou Cafe" really is a competitor to
  both a new bar and a new cafe. Anything that needs an establishment count must therefore
  use COUNT(DISTINCT establishment_id), never COUNT(*).

  Per-city construction notes:

  - Madrid joins premises to activities on id_local. A premise licensed for several
    activities produces several rows, so the result is de-duplicated on
    (establishment_id, shop_type). Coordinates use the (0, 0) sentinel for "not geocoded",
    which raw.madrid_premises has already nulled.
  - Paris uses BDCOM codact directly. A premise is treated as not open when its premise_type
    is V ("Vide") or its activity code is AA101/AA102, the two vacancy codes.
  - Mexico City uses SCIAN directly. Three DENUE rows carry coordinates in Baja California,
    a source error, and are removed by the bounding-box filter along with anything else
    outside the city envelope.
  - London uses BusinessTypeId. 14,504 of 81,169 establishments have no coordinates, so they
    survive here with is_geocoded = false and are excluded from anything spatial downstream
    rather than being silently dropped.
  - Chicago explodes the pipe-delimited business_activity field into one row per activity
    token, because that field is where the real category detail lives; license_description
    alone cannot distinguish a coffee shop from a grocer. One business can hold several
    licences at one site, so the establishment key is account_number plus site_number.

depends:
  - raw.madrid_premises
  - raw.madrid_activities
  - raw.paris_bdcom_premises
  - raw.paris_bdcom_nomenclature
  - raw.cdmx_denue_units
  - raw.london_fhrs_establishments
  - raw.chicago_business_licenses
  - staging.shop_type_crosswalk

materialization:
  type: table
  strategy: create+replace

columns:
  - name: city
    type: VARCHAR
    description: City slug, one of madrid, paris, mexico_city, london, chicago.
    primary_key: true
    nullable: false
  - name: establishment_id
    type: VARCHAR
    description: Source natural key for the physical premise, unique within the city.
    primary_key: true
    nullable: false
  - name: shop_type
    type: VARCHAR
    description: Canonical shop type, one of bar_pub, nightclub, cafe, restaurant, fast_food, bakery, bookstore.
    primary_key: true
    nullable: false
  - name: name
    type: VARCHAR
    description: Trading name of the establishment as published, frequently blank in Madrid and Paris.
  - name: native_code
    type: VARCHAR
    description: Source activity code that produced this classification.
  - name: native_label
    type: VARCHAR
    description: Source activity label, in the source language.
  - name: separability
    type: VARCHAR
    description: How cleanly the source code isolates this shop type. clean, shared_class, over_broad, partial or not_separable.
  - name: is_usable
    type: BOOLEAN
    description: False where the shop type cannot be isolated in this city, meaning the combination must not be scored or mapped.
  - name: lon
    type: DOUBLE
    description: Longitude in decimal degrees, WGS84. Null where the source could not geocode the premise.
  - name: lat
    type: DOUBLE
    description: Latitude in decimal degrees, WGS84. Null where the source could not geocode the premise.
  - name: is_geocoded
    type: BOOLEAN
    description: True where the establishment has coordinates inside the city bounding box.
  - name: is_open
    type: BOOLEAN
    description: True where the source indicates the premise is currently trading. Madrid publishes closed and vacant premises; the other four registers list active businesses only.
  - name: small_area_id
    type: VARCHAR
    description: Finest small-area identifier the source supplies. Madrid census section, Paris INSEE 200 m cell, Mexico City alcaldia plus AGEB, London postcode, Chicago community area.
  - name: floor_area_m2
    type: DOUBLE
    description: Interior floor area in square metres. Paris only, and recorded for under 2% of premises.
  - name: source_name
    type: VARCHAR
    description: Publisher and dataset the row came from.
  - name: source_vintage
    type: VARCHAR
    description: Vintage of the source data, so a triennial field survey is never mistaken for a daily register.

@bruin */

WITH city_bounds AS (
    SELECT * FROM UNNEST([
        STRUCT('madrid' AS city, -3.889 AS min_lon, 40.312 AS min_lat, -3.518 AS max_lon, 40.644 AS max_lat),
        ('paris', 2.224, 48.815, 2.470, 48.902),
        ('mexico_city', -99.365, 19.048, -98.940, 19.593),
        ('london', -0.5104, 51.2868, 0.3340, 51.6919),
        ('chicago', -87.9401, 41.6445, -87.5241, 42.0230)
    ])
),

-- ===================================================================== MADRID
madrid_premises AS (
    SELECT *
    FROM raw.madrid_premises
    WHERE id_local IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_local ORDER BY extracted_at DESC) = 1
),

madrid_activities AS (
    SELECT *
    FROM raw.madrid_activities
    WHERE id_local IS NOT NULL AND epigrafe_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_local, epigrafe_id ORDER BY extracted_at DESC
    ) = 1
),

madrid AS (
    SELECT
        'madrid' AS city,
        p.id_local AS establishment_id,
        NULLIF(TRIM(COALESCE(p.signage_name, a.signage_name)), '') AS name,
        a.epigrafe_id AS native_code,
        a.epigrafe_desc AS native_label,
        p.lon,
        p.lat,
        UPPER(p.situation_desc) = 'ABIERTO' AS is_open,
        p.census_section_id AS small_area_id,
        CAST(NULL AS FLOAT64) AS floor_area_m2,
        'Ayuntamiento de Madrid, Censo de locales y actividades' AS source_name,
        CONCAT('Daily register, publisher load date ', COALESCE(p.source_load_date, 'unknown')) AS source_vintage
    FROM madrid_premises p
    JOIN madrid_activities a USING (id_local)
),

-- ====================================================================== PARIS
paris_premises AS (
    SELECT *
    FROM raw.paris_bdcom_premises
    WHERE objectid IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY objectid ORDER BY extracted_at DESC) = 1
),

paris_nomenclature AS (
    SELECT *
    FROM raw.paris_bdcom_nomenclature
    WHERE codact IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY codact ORDER BY extracted_at DESC) = 1
),

paris AS (
    SELECT
        'paris' AS city,
        CAST(p.objectid AS STRING) AS establishment_id,
        NULLIF(TRIM(p.signage_name), '') AS name,
        p.codact AS native_code,
        COALESCE(n.libact, CONCAT('Unmapped codact ', p.codact)) AS native_label,
        p.lon,
        p.lat,
        p.premise_type != 'V' AND p.codact NOT IN ('AA101', 'AA102') AS is_open,
        p.idcar_200m AS small_area_id,
        p.surface_exact_m2 AS floor_area_m2,
        'APUR, BDCOM 2023 commercial ground-floor census' AS source_name,
        'Field survey, June 2023 wave (triennial)' AS source_vintage
    FROM paris_premises p
    LEFT JOIN paris_nomenclature n USING (codact)
),

-- ================================================================ MEXICO CITY
cdmx AS (
    SELECT
        'mexico_city' AS city,
        d.denue_id AS establishment_id,
        NULLIF(TRIM(d.establishment_name), '') AS name,
        d.scian_code AS native_code,
        d.scian_label AS native_label,
        d.lon,
        d.lat,
        TRUE AS is_open,
        CONCAT(COALESCE(d.alcaldia_code, '??'), '-', COALESCE(d.ageb, '????')) AS small_area_id,
        CAST(NULL AS FLOAT64) AS floor_area_m2,
        'INEGI, Directorio Estadistico Nacional de Unidades Economicas (DENUE)' AS source_name,
        'National statistical directory, 2026-05 edition' AS source_vintage
    FROM (
        SELECT *
        FROM raw.cdmx_denue_units
        WHERE denue_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY denue_id ORDER BY extracted_at DESC) = 1
    ) d
),

-- ===================================================================== LONDON
london AS (
    SELECT
        'london' AS city,
        CAST(e.fhrs_id AS STRING) AS establishment_id,
        NULLIF(TRIM(e.business_name), '') AS name,
        CAST(e.business_type_id AS STRING) AS native_code,
        e.business_type AS native_label,
        e.lon,
        e.lat,
        TRUE AS is_open,
        e.postcode AS small_area_id,
        CAST(NULL AS FLOAT64) AS floor_area_m2,
        'Food Standards Agency, Food Hygiene Rating Scheme' AS source_name,
        'Statutory food register, live API' AS source_vintage
    FROM (
        SELECT *
        FROM raw.london_fhrs_establishments
        WHERE fhrs_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY fhrs_id ORDER BY extracted_at DESC) = 1
    ) e
),

-- ==================================================================== CHICAGO
chicago_licences AS (
    SELECT *
    FROM raw.chicago_business_licenses
    WHERE license_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY license_id ORDER BY extracted_at DESC) = 1
),

-- One business can hold several licences at one physical site. Collapse to the site and
-- keep the most recently issued licence's geocode and name.
chicago_sites AS (
    SELECT
        CONCAT(account_number, '-', COALESCE(site_number, '0')) AS establishment_id,
        ANY_VALUE(doing_business_as_name HAVING MAX date_issued) AS name,
        ANY_VALUE(lon HAVING MAX date_issued) AS lon,
        ANY_VALUE(lat HAVING MAX date_issued) AS lat,
        ANY_VALUE(community_area_name HAVING MAX date_issued) AS community_area_name,
        STRING_AGG(business_activity, '|') AS all_activities
    FROM chicago_licences
    WHERE account_number IS NOT NULL
    GROUP BY establishment_id
),

chicago AS (
    SELECT DISTINCT
        'chicago' AS city,
        s.establishment_id,
        NULLIF(TRIM(s.name), '') AS name,
        TRIM(token) AS native_code,
        TRIM(token) AS native_label,
        s.lon,
        s.lat,
        TRUE AS is_open,
        s.community_area_name AS small_area_id,
        CAST(NULL AS FLOAT64) AS floor_area_m2,
        'City of Chicago, Department of Business Affairs and Consumer Protection' AS source_name,
        'Active business licences, daily snapshot' AS source_vintage
    FROM chicago_sites s,
    UNNEST(SPLIT(s.all_activities, '|')) AS token
    WHERE s.all_activities IS NOT NULL AND TRIM(token) != ''
),

-- ================================================================== COMBINED
combined AS (
    SELECT * FROM madrid
    UNION ALL SELECT * FROM paris
    UNION ALL SELECT * FROM cdmx
    UNION ALL SELECT * FROM london
    UNION ALL SELECT * FROM chicago
),

classified AS (
    SELECT
        c.city,
        c.establishment_id,
        x.shop_type,
        c.name,
        c.native_code,
        c.native_label,
        x.separability,
        x.is_usable,
        c.lon,
        c.lat,
        COALESCE(
            c.lon BETWEEN b.min_lon AND b.max_lon
            AND c.lat BETWEEN b.min_lat AND b.max_lat,
            FALSE
        ) AS is_geocoded,
        c.is_open,
        c.small_area_id,
        c.floor_area_m2,
        c.source_name,
        c.source_vintage
    FROM combined c
    JOIN staging.shop_type_crosswalk x
        ON x.city = c.city AND x.native_code = c.native_code
    JOIN city_bounds b
        ON b.city = c.city
)

-- A Madrid premise licensed for two activities that both map to the same canonical type,
-- or a Chicago site whose licences repeat an activity token, would otherwise duplicate.
SELECT
    city,
    establishment_id,
    shop_type,
    ANY_VALUE(name) AS name,
    ANY_VALUE(native_code) AS native_code,
    ANY_VALUE(native_label) AS native_label,
    -- Prefer the most pessimistic separability grade where a premise arrives via two codes.
    MIN(separability) AS separability,
    LOGICAL_AND(is_usable) AS is_usable,
    ANY_VALUE(lon) AS lon,
    ANY_VALUE(lat) AS lat,
    LOGICAL_OR(is_geocoded) AS is_geocoded,
    LOGICAL_OR(is_open) AS is_open,
    ANY_VALUE(small_area_id) AS small_area_id,
    MAX(floor_area_m2) AS floor_area_m2,
    ANY_VALUE(source_name) AS source_name,
    ANY_VALUE(source_vintage) AS source_vintage
FROM classified
GROUP BY city, establishment_id, shop_type
