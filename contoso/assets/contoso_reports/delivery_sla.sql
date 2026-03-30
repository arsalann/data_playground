/* @bruin

name: contoso_reports.delivery_sla
type: bq.sql
description: |
  Executive-level monthly delivery SLA performance report for Contoso's consumer electronics fulfillment network.
  Aggregates carrier performance across 9 countries and Online fulfillment, tracking key logistics KPIs including
  delivery rates, lead time distributions, and cost efficiency. Used by Operations leadership to monitor SLA
  compliance and identify underperforming carrier-country combinations.

  Data shows typical delivery rates of 93% overall, with significant variation by carrier (40-100% range).
  Lead times average 9.5 days end-to-end, with P95 around 15-16 days. Cost per shipment ranges from $15-34
  depending on carrier and destination. The "Online" store_country represents direct-to-consumer shipments
  that don't originate from physical store locations.

  Refreshes daily as part of the operations reporting pipeline. Historical data spans 2016-present covering
  approximately 1M+ shipments aggregated to ~4,000+ monthly carrier-country combinations.
connection: bruin-playground-eu
tags:
  - domain:operations
  - domain:logistics
  - data_type:fact_table
  - grain:monthly
  - pipeline_role:mart
  - sensitivity:internal
  - report_type:executive_kpi

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_staging.shipping_performance

secrets:
  - key: bruin-playground-eu
    inject_as: bruin-playground-eu

columns:
  - name: year
    type: INTEGER
    description: Calendar year of order placement (not shipment year)
    primary_key: true
  - name: month
    type: INTEGER
    description: Calendar month of order placement (1-12), grouped with year for monthly reporting grain
    primary_key: true
  - name: carrier
    type: VARCHAR
    description: Shipping carrier code (4 carriers total, likely major providers like DHL, FedEx, UPS, etc.)
    primary_key: true
  - name: store_country
    type: VARCHAR
    description: Fulfillment country or "Online" for direct-to-consumer shipments (9 total including Australia, Canada, France, Germany, Italy, Netherlands, Online, United Kingdom, United States)
    primary_key: true
  - name: shipment_count
    type: INTEGER
    description: Total number of shipments dispatched in this month-carrier-country combination, includes all shipment statuses
  - name: delivered_count
    type: INTEGER
    description: Number of shipments with final status "Delivered", used as numerator for delivery rate calculation
  - name: returned_count
    type: INTEGER
    description: Number of shipments with final status "Returned", typically customer-initiated returns after attempted delivery
  - name: lost_count
    type: INTEGER
    description: Number of shipments with final status "Lost", represents carrier failures and undelivered packages
  - name: delivery_rate
    type: DOUBLE
    description: Successful delivery percentage (delivered_count / shipment_count * 100), ranges 40-100% with average ~93%, key SLA metric
  - name: avg_days_to_ship
    type: DOUBLE
    description: Average fulfillment time in days from order placement to dispatch, measures internal processing efficiency (typically 1.5 days)
  - name: avg_days_in_transit
    type: DOUBLE
    description: Average carrier transit time in days from dispatch to delivery, measures carrier performance (typically 8 days)
  - name: avg_total_lead_days
    type: DOUBLE
    description: Average end-to-end delivery time in days from order to customer receipt, key customer experience metric (typically 9.5 days)
  - name: p95_lead_days
    type: INTEGER
    description: 95th percentile total lead time in days, represents worst-case customer experience for SLA planning (typically 15-16 days)
  - name: total_ship_cost
    type: DOUBLE
    description: Total shipping costs in USD for all shipments in this month-carrier-country combination, used for cost analysis and carrier negotiations
  - name: avg_cost_per_shipment
    type: DOUBLE
    description: Average shipping cost per shipment in USD, ranges $15-34, key metric for carrier cost efficiency comparisons

@bruin */

SELECT
    year,
    month,
    carrier,
    store_country,
    COUNT(*) AS shipment_count,
    COUNTIF(shipment_status = 'Delivered') AS delivered_count,
    COUNTIF(shipment_status = 'Returned') AS returned_count,
    COUNTIF(shipment_status = 'Lost') AS lost_count,
    ROUND(COUNTIF(shipment_status = 'Delivered') / COUNT(*) * 100, 2) AS delivery_rate,
    ROUND(AVG(days_to_ship), 2) AS avg_days_to_ship,
    ROUND(AVG(days_in_transit), 2) AS avg_days_in_transit,
    ROUND(AVG(total_lead_days), 2) AS avg_total_lead_days,
    CAST(APPROX_QUANTILES(total_lead_days, 100)[OFFSET(95)] AS INTEGER) AS p95_lead_days,
    ROUND(SUM(ship_cost), 2) AS total_ship_cost,
    ROUND(AVG(ship_cost), 2) AS avg_cost_per_shipment
FROM contoso_staging.shipping_performance
GROUP BY 1, 2, 3, 4
ORDER BY year, month, carrier, store_country
