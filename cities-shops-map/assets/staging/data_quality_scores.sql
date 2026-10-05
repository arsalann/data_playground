/* @bruin
name: staging.data_quality_scores
type: bq.sql
connection: bruin-playground-arsalan
description: |
  The dataset-appraisal rubric stored as data, one row per city and shop type, so the
  published score is computed from its sub-scores rather than typed into prose twice.

  Rubric weights, out of 100: source authority 20, spatial coverage 20, category granularity
  25, freshness 15, geocoding 10, access and licence 10. Each sub-score is 0-10. Granularity
  carries the most weight because it is the criterion that most often silently invalidates a
  site-selection study: London's register is otherwise close to perfect and cannot tell a
  cafe from a restaurant.

  Departure from PLAN.md worth being explicit about. The plan scored authority, coverage,
  freshness, geocoding and access once per city and varied only granularity per shop type.
  That turned out to hide the single largest data-quality problem in the project: Mexico City
  has excellent coverage of restaurants and almost no coverage of bars, because bars are
  under-registered. A city-level coverage score of 8 would have scored the Mexico City bar
  map at 93 and said nothing. Coverage is therefore scored per city and shop type here, and
  Mexico City bars score 3.

  Corrections to the PLAN.md matrix, all from reading the live sources rather than
  documentation:
  - Paris nightclub granularity raised, because SA404 "Discotheque et club prive" is a
    dedicated class. The plan concluded Paris had no nightclub class, having searched only
    the CH restauration block.
  - Chicago cafe granularity raised, because the activity token "Preparation and Sale of
    Coffee and/or Drinks" separates 520 licences. The plan looked only at license_description.
  - Chicago nightclub granularity raised, because the Late Hour 4am liquor permit is a
    tighter proxy than Public Place of Amusement.
  - London bakery granularity cut to 0 and coverage to 2. FHRS publishes 14 business types
    and none is a bakery, so the plan's score of 74 credited a universe that does not exist.
  - Madrid geocoding cut from 10 to 9. The register uses (0, 0) for "not geocoded" on 50,762
    of 203,560 premises. For the shop types used here the geocoded share is 90.8%, and for
    street-front premises 96.6%, but it is not the 100% that a 10 would imply.

depends:
  - staging.shop_type_crosswalk

materialization:
  type: table
  strategy: create+replace

columns:
  - name: city
    type: VARCHAR
    description: City slug.
    primary_key: true
    nullable: false
  - name: shop_type
    type: VARCHAR
    description: Canonical shop type.
    primary_key: true
    nullable: false
  - name: authority
    type: INTEGER
    description: Source authority and collection method, 0-10. 10 is a national statistics institute or a field-verified statutory register.
  - name: coverage
    type: INTEGER
    description: Spatial and universe coverage for this shop type, 0-10. 10 is every premise in the municipality including vacant ones.
  - name: granularity
    type: INTEGER
    description: Category granularity for this shop type, 0-10. 0 means the type cannot be separated from others, 10 means a dedicated code with sub-types distinguished.
  - name: freshness
    type: INTEGER
    description: Freshness and update cadence, 0-10. 10 is updated daily.
  - name: geocoding
    type: INTEGER
    description: Geocoding quality, 0-10. 10 is per-premise coordinates plus a small-area unit for every row.
  - name: access
    type: INTEGER
    description: Access and licence, 0-10. 10 is an open licence with direct bulk or API access and no authentication.
  - name: score
    type: DOUBLE
    description: Weighted total out of 100, computed from the sub-scores using the rubric weights.
  - name: score_rank_in_city
    type: INTEGER
    description: Rank of this shop type within its city by score, 1 is best.

@bruin */

WITH sub_scores AS (
    SELECT * FROM UNNEST([
        -- Madrid: municipal register, daily, 455 activity classes.
        STRUCT('madrid' AS city, 'bar_pub' AS shop_type, 9 AS authority, 10 AS coverage, 10 AS granularity, 10 AS freshness, 9 AS geocoding, 8 AS access),
        ('madrid', 'nightclub',  9, 10,  9, 10, 9, 8),
        ('madrid', 'cafe',       9, 10,  9, 10, 9, 8),
        ('madrid', 'restaurant', 9, 10,  8, 10, 9, 8),
        ('madrid', 'fast_food',  9, 10,  9, 10, 9, 8),
        ('madrid', 'bakery',     9, 10, 10, 10, 9, 8),
        ('madrid', 'bookstore',  9, 10,  9, 10, 9, 8),

        -- Paris: field survey, triennial, 220 activity classes, vacancy included.
        ('paris', 'bar_pub',    10, 10,  4, 6, 10, 8),
        ('paris', 'nightclub',  10, 10,  8, 6, 10, 8),
        ('paris', 'cafe',       10, 10,  5, 6, 10, 8),
        ('paris', 'restaurant', 10, 10, 10, 6, 10, 8),
        ('paris', 'fast_food',  10, 10, 10, 6, 10, 8),
        ('paris', 'bakery',     10, 10,  9, 6, 10, 8),
        ('paris', 'bookstore',  10, 10,  9, 6, 10, 8),

        -- Mexico City: national statistical directory, 6-digit SCIAN, 100% geocoded.
        -- Bar and nightclub coverage is cut hard for under-registration.
        ('mexico_city', 'bar_pub',    10, 3, 9, 9, 10, 9),
        ('mexico_city', 'nightclub',  10, 2, 9, 9, 10, 9),
        ('mexico_city', 'cafe',       10, 8, 5, 9, 10, 9),
        ('mexico_city', 'restaurant', 10, 8, 9, 9, 10, 9),
        ('mexico_city', 'fast_food',  10, 8, 8, 9, 10, 9),
        ('mexico_city', 'bakery',     10, 8, 9, 9, 10, 9),
        ('mexico_city', 'bookstore',  10, 8, 9, 9, 10, 9),

        -- London: statutory food register, daily, 14 business types, no retail detail.
        ('london', 'bar_pub',    9, 8, 5, 10, 7, 10),
        ('london', 'nightclub',  9, 8, 3, 10, 7, 10),
        ('london', 'cafe',       9, 8, 3, 10, 7, 10),
        ('london', 'restaurant', 9, 8, 4, 10, 7, 10),
        ('london', 'fast_food',  9, 8, 9, 10, 7, 10),
        ('london', 'bakery',     9, 2, 0, 10, 7, 10),
        ('london', 'bookstore',  9, 0, 0, 10, 7, 10),

        -- Chicago: licence register, daily, coarse licence types but a usable activity field.
        ('chicago', 'bar_pub',    9, 8, 9, 10, 8, 10),
        ('chicago', 'nightclub',  9, 8, 6, 10, 8, 10),
        ('chicago', 'cafe',       9, 8, 4, 10, 8, 10),
        ('chicago', 'restaurant', 9, 8, 7, 10, 8, 10),
        ('chicago', 'fast_food',  9, 8, 7, 10, 8, 10),
        ('chicago', 'bakery',     9, 3, 3, 10, 8, 10),
        ('chicago', 'bookstore',  9, 1, 2, 10, 8, 10)
    ])
),

scored AS (
    SELECT
        city,
        shop_type,
        authority,
        coverage,
        granularity,
        freshness,
        geocoding,
        access,
        ROUND(
            (
                authority * 20
                + coverage * 20
                + granularity * 25
                + freshness * 15
                + geocoding * 10
                + access * 10
            ) / 10.0,
            1
        ) AS score
    FROM sub_scores
)

SELECT
    city,
    shop_type,
    authority,
    coverage,
    granularity,
    freshness,
    geocoding,
    access,
    score,
    CAST(
        RANK() OVER (PARTITION BY city ORDER BY score DESC) AS INT64
    ) AS score_rank_in_city
FROM scored
ORDER BY city, score DESC
