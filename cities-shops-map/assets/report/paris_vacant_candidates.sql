/* @bruin
name: report.paris_vacant_candidates
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Vacant Paris premises at real street addresses, with a churn record derived from 20 years of
  survey waves and the observed shop density of the cell each one sits in.

  Nothing here is modelled. APUR surveyors physically walked the ground floor of Paris in June
  2023 and recorded which units were empty, and the same survey has run since 2000, so the
  activity recorded at each address in each wave is on file. No other city in this project can
  produce this: a licence register knows only what exists, never what is empty.

  Churn uses the eight wave columns (2000, 2003, 2005, 2007, 2011, 2014, 2017, 2020).
  `activity_changes` counts how many times the recorded activity at the address changed between
  consecutive waves. An address that has turned over five times in twenty years is telling you
  something no demand figure can: the location does not hold a business.

  Vacancy is identified two ways and either is sufficient: premise_type 'V' ("Vide") or an
  activity code of AA101 "Locaux Vacants" or AA102 "Locaux en travaux".

  This asset previously joined report.site_scores to attach a modelled recommendation score to
  each address. That score has been removed from the project, so what is attached now is the
  observed count of that shop type per 1,000 residents around the address. It describes the
  neighbourhood the unit sits in; it does not rate the unit.

depends:
  - raw.paris_bdcom_premises
  - staging.analysis_grid
  - report.shop_density

materialization:
  type: table
  strategy: create+replace

columns:
  - name: objectid
    type: INTEGER
    description: BDCOM feature identifier for the vacant premise.
    primary_key: true
    nullable: false
  - name: shop_type
    type: VARCHAR
    description: Shop type whose local density is being reported for this address.
    primary_key: true
    nullable: false
  - name: address
    type: VARCHAR
    description: Street address of the vacant premise as surveyed.
  - name: arrondissement
    type: INTEGER
    description: Paris arrondissement number, 1-20.
  - name: lon
    type: DOUBLE
    description: Longitude in decimal degrees, WGS84.
  - name: lat
    type: DOUBLE
    description: Latitude in decimal degrees, WGS84.
  - name: vacancy_reason
    type: VARCHAR
    description: Why the premise is treated as vacant, either the premise type or the vacancy activity code.
  - name: surface_exact_m2
    type: DOUBLE
    description: Interior floor area in square metres where the surveyor recorded it, otherwise null. Present on under 2% of premises.
  - name: surface_band
    type: VARCHAR
    description: Floor-area band as surveyed, populated far more often than the exact figure.
  - name: waves_occupied
    type: INTEGER
    description: Number of the eight survey waves from 2000 to 2020 in which an activity was recorded at this address.
  - name: distinct_activities
    type: INTEGER
    description: Number of distinct activities recorded at this address across the eight waves.
  - name: activity_changes
    type: INTEGER
    description: Number of times the recorded activity changed between consecutive waves. Higher means faster turnover.
  - name: churn_risk
    type: VARCHAR
    description: Churn band. low is 0-1 changes, moderate 2, elevated 3, high 4 or more.
  - name: cell_id
    type: VARCHAR
    description: Grid cell the address sits in.
  - name: shops_per_1k_residents
    type: DOUBLE
    description: Observed count of this shop type per 1,000 residents within 400 m of the cell centre. Null where there are too few residents for the ratio to mean anything.
  - name: shops_400m
    type: INTEGER
    description: Count of this shop type within 400 m of the cell centre.
  - name: population_400m
    type: DOUBLE
    description: Estimated residents within 400 m of the cell centre.
  - name: density_class
    type: INTEGER
    description: Map class of the containing cell. 0 means none of this shop type within 400 m, 1-4 are quartiles of the density, null means too few residents to rate.

@bruin */

WITH premises AS (
    SELECT *
    FROM raw.paris_bdcom_premises
    WHERE objectid IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY objectid ORDER BY extracted_at DESC) = 1
),

vacant AS (
    SELECT
        objectid,
        TRIM(CONCAT(COALESCE(street_number, ''), ' ', COALESCE(street_name, ''))) AS address,
        arrondissement,
        lon,
        lat,
        CASE
            WHEN premise_type = 'V' AND codact IN ('AA101', 'AA102')
                THEN CONCAT('Premise type V (Vide) and activity code ', codact)
            WHEN premise_type = 'V' THEN 'Premise type V (Vide)'
            ELSE CONCAT('Activity code ', codact)
        END AS vacancy_reason,
        surface_exact_m2,
        surface_band,
        [act_2000, act_2003, act_2005, act_2007, act_2011, act_2014, act_2017, act_2020] AS waves
    FROM premises
    WHERE premise_type = 'V' OR codact IN ('AA101', 'AA102')
),

churn AS (
    SELECT
        v.*,
        (SELECT COUNT(w) FROM UNNEST(v.waves) AS w WHERE w IS NOT NULL) AS waves_occupied,
        (SELECT COUNT(DISTINCT w) FROM UNNEST(v.waves) AS w WHERE w IS NOT NULL)
            AS distinct_activities,
        -- Count transitions between consecutive recorded waves where the activity differs.
        (
            SELECT COUNTIF(current_wave != previous_wave)
            FROM (
                SELECT
                    w AS current_wave,
                    LAG(w) OVER (ORDER BY position) AS previous_wave
                FROM UNNEST(v.waves) AS w WITH OFFSET AS position
                WHERE w IS NOT NULL
            )
        ) AS activity_changes
    FROM vacant v
),

grid_params AS (
    SELECT ANY_VALUE(lat_ref) AS lat_ref
    FROM staging.analysis_grid
    WHERE city = 'paris'
),

located AS (
    SELECT
        c.* EXCEPT (waves),
        CAST(FLOOR(c.lon * 111320 * COS(p.lat_ref * ACOS(-1) / 180) / 250) AS INT64) AS cell_x,
        CAST(FLOOR(c.lat * 110540 / 250) AS INT64) AS cell_y
    FROM churn c
    CROSS JOIN grid_params p
    WHERE c.lon IS NOT NULL AND c.lat IS NOT NULL
)

SELECT
    l.objectid,
    d.shop_type,
    l.address,
    l.arrondissement,
    l.lon,
    l.lat,
    l.vacancy_reason,
    l.surface_exact_m2,
    l.surface_band,
    l.waves_occupied,
    l.distinct_activities,
    l.activity_changes,
    CASE
        WHEN l.activity_changes >= 4 THEN 'high'
        WHEN l.activity_changes = 3 THEN 'elevated'
        WHEN l.activity_changes = 2 THEN 'moderate'
        ELSE 'low'
    END AS churn_risk,
    d.cell_id,
    d.shops_per_1k_residents,
    d.shops_400m,
    d.population_400m,
    d.density_class
FROM located l
JOIN report.shop_density d
    ON d.city = 'paris' AND d.cell_x = l.cell_x AND d.cell_y = l.cell_y
