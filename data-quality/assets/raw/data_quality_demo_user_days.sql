/* @bruin

name: raw.data_quality_demo_user_days
type: bq.sql
description: Deterministic synthetic fixture for Data Quality dashboard 1135. Not production Firebase, Adjust or AppLovin data.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

columns:
  - name: dt
    type: DATE
    description: UTC fixture date; inclusive filter boundaries.
    primary_key: true
    nullable: false
  - name: user_pseudo_id
    type: VARCHAR
    description: Synthetic user ID; __unattributed__ represents unassigned AppLovin revenue.
    primary_key: true
    nullable: false
  - name: in_firebase
    type: BOOLEAN
    description: Whether this synthetic user is observed by firebase.
  - name: in_adjust
    type: BOOLEAN
    description: Whether this synthetic user is observed by adjust.
  - name: in_applovin
    type: BOOLEAN
    description: Whether this synthetic user is observed by applovin.
  - name: fb_ad_inter_rev
    type: DOUBLE
    description: fb ad inter rev (USD).
  - name: fb_ad_rewarded_rev
    type: DOUBLE
    description: fb ad rewarded rev (USD).
  - name: fb_ad_inter_count
    type: INTEGER
    description: fb ad inter count (events).
  - name: fb_ad_rewarded_count
    type: INTEGER
    description: fb ad rewarded count (events).
  - name: fb_iap_rev
    type: DOUBLE
    description: fb iap rev (USD).
  - name: fb_iap_count
    type: INTEGER
    description: fb iap count (events).
  - name: fb_play_on_count
    type: INTEGER
    description: fb play on count (events).
  - name: fb_booster_use_count
    type: INTEGER
    description: fb booster use count (events).
  - name: fb_shop_view_count
    type: INTEGER
    description: fb shop view count (events).
  - name: fb_session_count
    type: INTEGER
    description: fb session count (events).
  - name: fb_install_count
    type: INTEGER
    description: fb install count (events).
  - name: aj_ad_inter_rev
    type: DOUBLE
    description: aj ad inter rev (USD).
  - name: aj_ad_rewarded_rev
    type: DOUBLE
    description: aj ad rewarded rev (USD).
  - name: aj_ad_inter_count
    type: INTEGER
    description: aj ad inter count (events).
  - name: aj_ad_rewarded_count
    type: INTEGER
    description: aj ad rewarded count (events).
  - name: aj_iap_rev
    type: DOUBLE
    description: aj iap rev (USD).
  - name: aj_iap_count
    type: INTEGER
    description: aj iap count (events).
  - name: aj_play_on_count
    type: INTEGER
    description: aj play on count (events).
  - name: aj_booster_use_count
    type: INTEGER
    description: aj booster use count (events).
  - name: aj_shop_view_count
    type: INTEGER
    description: aj shop view count (events).
  - name: aj_session_count
    type: INTEGER
    description: aj session count (events).
  - name: aj_install_count
    type: INTEGER
    description: aj install count (events).
  - name: al_ad_inter_rev
    type: DOUBLE
    description: al ad inter rev (USD).
  - name: al_ad_rewarded_rev
    type: DOUBLE
    description: al ad rewarded rev (USD).
  - name: al_ad_inter_count
    type: INTEGER
    description: al ad inter count (events).
  - name: al_ad_rewarded_count
    type: INTEGER
    description: al ad rewarded count (events).
  - name: fb_ad_rev
    type: DOUBLE
    description: fb ad rev (USD).
  - name: aj_ad_rev
    type: DOUBLE
    description: aj ad rev (USD).
  - name: al_ad_rev
    type: DOUBLE
    description: al ad rev (USD).
  - name: extracted_at
    type: TIMESTAMP
    description: UTC fixture generation timestamp.

@bruin */

WITH demo_entities AS (
  SELECT dt, entity_id, DATE_DIFF(dt, DATE('{{ var.fixture_start }}'), DAY) AS day_no
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE('{{ var.fixture_start }}'), DATE('{{ var.fixture_end }}'))) AS dt
  CROSS JOIN UNNEST(GENERATE_ARRAY(0, 1000)) AS entity_id
  WHERE (entity_id = 0 OR MOD(entity_id, 30) = DATE_DIFF(dt, DATE('{{ var.fixture_start }}'), DAY)
      OR MOD(entity_id * 37 + DATE_DIFF(dt, DATE('{{ var.fixture_start }}'), DAY) * 71, 1009) < 490 + CAST(ROUND(30 * SIN(DATE_DIFF(dt, DATE('{{ var.fixture_start }}'), DAY) * 0.6)) AS INT64))
), demo_presence AS (
  SELECT *,
    IF(entity_id = 0, '__unattributed__', FORMAT('demo_user_pseudo_id_%04d', entity_id)) AS user_pseudo_id,
    entity_id != 0 AND (MOD(entity_id, 100) < 84 OR MOD(entity_id, 100) BETWEEN 89 AND 93) AS in_firebase,
    entity_id != 0 AND (MOD(entity_id, 100) < 80 OR MOD(entity_id, 100) BETWEEN 84 AND 88 OR MOD(entity_id, 100) BETWEEN 94 AND 97) AS in_adjust,
    entity_id = 0 OR MOD(entity_id, 100) < 70 OR MOD(entity_id, 100) BETWEEN 80 AND 88 OR MOD(entity_id, 100) >= 98 AS in_applovin,
    IF(entity_id = 0, 250, 8 + MOD(entity_id * 3 + day_no, 11)) AS inter_count,
    IF(entity_id = 0, 120, 2 + MOD(entity_id + day_no, 6)) AS rewarded_count,
    IF(entity_id = 0, 0,
    IF(MOD(entity_id + day_no, 19) = 0, 1, 0)) AS iap_count,
    2 + MOD(entity_id + day_no, 7) AS play_on_count,
    5 + MOD(entity_id * 2 + day_no, 11) AS booster_use_count,
    4 + MOD(entity_id + day_no * 2, 9) AS shop_view_count,
    6 + MOD(entity_id + day_no, 10) AS session_count,
    IF(entity_id > 0 AND MOD(entity_id, 30) = day_no, 1, 0) AS install_count
  FROM demo_entities
), demo_metrics AS (
  SELECT dt, user_pseudo_id, in_firebase, in_adjust, in_applovin,
    IF(in_firebase, inter_count * 0.045 * 0.98 * IF(dt BETWEEN DATE('{{ var.firebase_drop_start }}') AND DATE('{{ var.firebase_drop_end }}'), 0.65, 1.0), 0) AS fb_ad_inter_rev,
    IF(in_firebase, rewarded_count * 0.085 * 0.98 * IF(dt BETWEEN DATE('{{ var.firebase_drop_start }}') AND DATE('{{ var.firebase_drop_end }}'), 0.65, 1.0), 0) AS fb_ad_rewarded_rev,
    IF(in_firebase, CAST(ROUND(inter_count * 0.98 * IF(dt BETWEEN DATE('{{ var.firebase_drop_start }}') AND DATE('{{ var.firebase_drop_end }}'), 0.65, 1.0)) AS INT64), 0) AS fb_ad_inter_count,
    IF(in_firebase, CAST(ROUND(rewarded_count * 0.98 * IF(dt BETWEEN DATE('{{ var.firebase_drop_start }}') AND DATE('{{ var.firebase_drop_end }}'), 0.65, 1.0)) AS INT64), 0) AS fb_ad_rewarded_count,
    IF(in_firebase, iap_count * 4.99 * 0.98, 0) AS fb_iap_rev,
    IF(in_firebase, iap_count, 0) AS fb_iap_count,
    IF(in_firebase, CAST(ROUND(play_on_count * 0.95) AS INT64), 0) AS fb_play_on_count,
    IF(in_firebase, CAST(ROUND(booster_use_count * 0.90) AS INT64), 0) AS fb_booster_use_count,
    IF(in_firebase, CAST(ROUND(shop_view_count * 1.08) AS INT64), 0) AS fb_shop_view_count,
    IF(in_firebase, CAST(ROUND(session_count * 0.82) AS INT64), 0) AS fb_session_count,
    IF(in_firebase, install_count, 0) AS fb_install_count,
    IF(in_adjust, inter_count * 0.045 * 0.94 * 1.0, 0) AS aj_ad_inter_rev,
    IF(in_adjust, rewarded_count * 0.085 * 0.94 * 1.0, 0) AS aj_ad_rewarded_rev,
    IF(in_adjust, CAST(ROUND(inter_count * 0.94 * 1.0) AS INT64), 0) AS aj_ad_inter_count,
    IF(in_adjust, CAST(ROUND(rewarded_count * 0.94 * 1.0) AS INT64), 0) AS aj_ad_rewarded_count,
    IF(in_adjust, iap_count * 4.99 * 0.94, 0) AS aj_iap_rev,
    IF(in_adjust, iap_count, 0) AS aj_iap_count,
    IF(in_adjust, play_on_count, 0) AS aj_play_on_count,
    IF(in_adjust, booster_use_count, 0) AS aj_booster_use_count,
    IF(in_adjust, shop_view_count, 0) AS aj_shop_view_count,
    IF(in_adjust, session_count, 0) AS aj_session_count,
    IF(in_adjust, install_count, 0) AS aj_install_count,
    IF(in_applovin, inter_count * 0.045 * 1.02 * 1.0, 0) AS al_ad_inter_rev,
    IF(in_applovin, rewarded_count * 0.085 * 1.02 * 1.0, 0) AS al_ad_rewarded_rev,
    IF(in_applovin, CAST(ROUND(inter_count * 1.02 * 1.0) AS INT64), 0) AS al_ad_inter_count,
    IF(in_applovin, CAST(ROUND(rewarded_count * 1.02 * 1.0) AS INT64), 0) AS al_ad_rewarded_count
  FROM demo_presence
), comparison AS (
  SELECT *, fb_ad_inter_rev + fb_ad_rewarded_rev AS fb_ad_rev,
    aj_ad_inter_rev + aj_ad_rewarded_rev AS aj_ad_rev,
    al_ad_inter_rev + al_ad_rewarded_rev AS al_ad_rev
  FROM demo_metrics
)
SELECT *, CURRENT_TIMESTAMP() AS extracted_at FROM comparison
