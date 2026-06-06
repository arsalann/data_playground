/* @bruin
name: raw.klaviyo_flows
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic fake Klaviyo flow metadata for template completeness.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: id
    type: VARCHAR
    description: Klaviyo flow identifier
    primary_key: true
    nullable: false
  - name: name
    type: VARCHAR
    description: Flow display name
  - name: status
    type: VARCHAR
    description: Flow status

@bruin */

SELECT *
FROM UNNEST([
    STRUCT('flow_welcome' AS id, 'Welcome Series' AS name, 'live' AS status),
    STRUCT('flow_abandoned_cart' AS id, 'Abandoned Cart' AS name, 'live' AS status),
    STRUCT('flow_back_in_stock' AS id, 'Back in Stock' AS name, 'live' AS status),
    STRUCT('flow_post_purchase' AS id, 'Post-purchase Cross-sell' AS name, 'live' AS status)
])
