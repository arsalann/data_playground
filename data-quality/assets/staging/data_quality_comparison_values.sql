/* @bruin

name: staging.data_quality_comparison_values
type: bq.sql
description: Deterministic synthetic fixture for Data Quality dashboard 1135. Not production Firebase, Adjust or AppLovin data.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.data_quality_user_days

columns:
  - name: dt
    type: DATE
    description: UTC fixture date.
    primary_key: true
    nullable: false
  - name: user_pseudo_id
    type: VARCHAR
    description: Synthetic user ID.
    primary_key: true
    nullable: false
  - name: metric
    type: VARCHAR
    description: Comparable metric name.
    primary_key: true
    nullable: false
  - name: unit
    type: VARCHAR
    description: USD or count.
  - name: in_firebase
    type: BOOLEAN
    description: in firebase
  - name: in_adjust
    type: BOOLEAN
    description: in adjust
  - name: in_applovin
    type: BOOLEAN
    description: in applovin
  - name: is_attributed
    type: BOOLEAN
    description: is attributed
  - name: firebase_value
    type: DOUBLE
    description: Source measurement in unit; NULL when unsupported.
  - name: adjust_value
    type: DOUBLE
    description: Source measurement in unit; NULL when unsupported.
  - name: applovin_value
    type: DOUBLE
    description: Source measurement in unit; NULL when unsupported.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC extraction timestamp.

@bruin */

SELECT d.dt, d.user_pseudo_id, d.in_firebase, d.in_adjust, d.in_applovin, d.is_attributed,
    m.metric, m.unit, m.firebase AS firebase_value, m.adjust AS adjust_value, m.applovin AS applovin_value,
    d.extracted_at
FROM staging.data_quality_user_days AS d
CROSS JOIN UNNEST([
STRUCT('Users' AS metric, 'count' AS unit, CAST(IF(in_firebase AND is_attributed, 1, 0) AS FLOAT64) AS firebase, CAST(IF(in_adjust AND is_attributed, 1, 0) AS FLOAT64) AS adjust, CAST(IF(in_applovin AND is_attributed, 1, 0) AS FLOAT64) AS applovin),
STRUCT('IAP Revenue' AS metric, 'USD' AS unit, CAST(fb_iap_rev AS FLOAT64) AS firebase, CAST(aj_iap_rev AS FLOAT64) AS adjust, CAST(NULL AS FLOAT64) AS applovin),
STRUCT('IAP Purchases' AS metric, 'count' AS unit, CAST(fb_iap_count AS FLOAT64) AS firebase, CAST(aj_iap_count AS FLOAT64) AS adjust, CAST(NULL AS FLOAT64) AS applovin),
STRUCT('Ad Revenue' AS metric, 'USD' AS unit, CAST(fb_ad_rev AS FLOAT64) AS firebase, CAST(aj_ad_rev AS FLOAT64) AS adjust, CAST(al_ad_rev AS FLOAT64) AS applovin),
STRUCT('Interstitial Ad Revenue' AS metric, 'USD' AS unit, CAST(fb_ad_inter_rev AS FLOAT64) AS firebase, CAST(aj_ad_inter_rev AS FLOAT64) AS adjust, CAST(al_ad_inter_rev AS FLOAT64) AS applovin),
STRUCT('Rewarded Ad Revenue' AS metric, 'USD' AS unit, CAST(fb_ad_rewarded_rev AS FLOAT64) AS firebase, CAST(aj_ad_rewarded_rev AS FLOAT64) AS adjust, CAST(al_ad_rewarded_rev AS FLOAT64) AS applovin),
STRUCT('Ad Impressions' AS metric, 'count' AS unit, CAST((fb_ad_inter_count + fb_ad_rewarded_count) AS FLOAT64) AS firebase, CAST((aj_ad_inter_count + aj_ad_rewarded_count) AS FLOAT64) AS adjust, CAST((al_ad_inter_count + al_ad_rewarded_count) AS FLOAT64) AS applovin),
STRUCT('Interstitial Impressions' AS metric, 'count' AS unit, CAST(fb_ad_inter_count AS FLOAT64) AS firebase, CAST(aj_ad_inter_count AS FLOAT64) AS adjust, CAST(al_ad_inter_count AS FLOAT64) AS applovin),
STRUCT('Rewarded Impressions' AS metric, 'count' AS unit, CAST(fb_ad_rewarded_count AS FLOAT64) AS firebase, CAST(aj_ad_rewarded_count AS FLOAT64) AS adjust, CAST(al_ad_rewarded_count AS FLOAT64) AS applovin),
STRUCT('Play-On Accepts' AS metric, 'count' AS unit, CAST(fb_play_on_count AS FLOAT64) AS firebase, CAST(aj_play_on_count AS FLOAT64) AS adjust, CAST(NULL AS FLOAT64) AS applovin),
STRUCT('Booster Use' AS metric, 'count' AS unit, CAST(fb_booster_use_count AS FLOAT64) AS firebase, CAST(aj_booster_use_count AS FLOAT64) AS adjust, CAST(NULL AS FLOAT64) AS applovin),
STRUCT('Shop Views' AS metric, 'count' AS unit, CAST(fb_shop_view_count AS FLOAT64) AS firebase, CAST(aj_shop_view_count AS FLOAT64) AS adjust, CAST(NULL AS FLOAT64) AS applovin),
STRUCT('Sessions' AS metric, 'count' AS unit, CAST(fb_session_count AS FLOAT64) AS firebase, CAST(aj_session_count AS FLOAT64) AS adjust, CAST(NULL AS FLOAT64) AS applovin),
STRUCT('Installs' AS metric, 'count' AS unit, CAST(fb_install_count AS FLOAT64) AS firebase, CAST(aj_install_count AS FLOAT64) AS adjust, CAST(NULL AS FLOAT64) AS applovin)
]) AS m
