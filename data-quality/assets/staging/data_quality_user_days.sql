/* @bruin

name: staging.data_quality_user_days
type: bq.sql
description: Deterministic synthetic fixture for Data Quality dashboard 1135. Not production Firebase, Adjust or AppLovin data.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.data_quality_demo_user_days

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
  - name: user_hash
    type: VARCHAR
    description: SHA256 of synthetic user identifier.
  - name: is_attributed
    type: BOOLEAN
    description: False for AppLovin unattributed revenue bucket.
  - name: presence_bucket
    type: VARCHAR
    description: Mutually exclusive source membership; fixture membership is stable across days.
  - name: source_presence
    type: VARCHAR
    description: Source names joined with + for user gap tables.

@bruin */

WITH deduped AS (
    SELECT * FROM raw.data_quality_demo_user_days
    WHERE dt IS NOT NULL AND user_pseudo_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dt, user_pseudo_id ORDER BY extracted_at DESC) = 1
)
SELECT *,
    TO_HEX(SHA256(user_pseudo_id)) AS user_hash,
    user_pseudo_id != '__unattributed__' AS is_attributed,
    CASE
      WHEN in_firebase AND in_adjust AND in_applovin THEN 'All 3'
      WHEN in_firebase AND in_adjust THEN 'Firebase + Adjust'
      WHEN in_firebase AND in_applovin THEN 'Firebase + AppLovin'
      WHEN in_adjust AND in_applovin THEN 'Adjust + AppLovin'
      WHEN in_firebase THEN 'Firebase Only'
      WHEN in_adjust THEN 'Adjust Only'
      WHEN in_applovin THEN 'AppLovin Only'
      ELSE 'No Source' END AS presence_bucket,
    ARRAY_TO_STRING(ARRAY_CONCAT(IF(in_firebase, ['Firebase'], []),
    IF(in_adjust, ['Adjust'], []),
      IF(in_applovin, ['AppLovin'], [])), ' + ') AS source_presence
FROM deduped
