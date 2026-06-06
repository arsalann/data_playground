# bruin-shop

End-to-end Bruin ecommerce template validation project using deterministic fake apparel data with the same schema consumed by the generated ecommerce staging and report layers.

## Data Sources

- **Fake US markets, special events, and acquisition funnel:** `raw.us_markets`, `raw.special_events`, and `raw.marketing_funnel` generate state/city demand, injected campaign/outage/stockout/defect events, channel spend, impressions, clicks, sessions, and conversions.
- **Fake Shopify-style apparel orders, customers, products, and inventory:** generated in BigQuery SQL under `assets/raw/` for T-shirts, pants, shoes, and accessories.
- **Fake GA4 sessions, hourly outage traffic, and page-level ecommerce events:** generated from the shared funnel and Shopify-style orders in BigQuery SQL under `assets/raw/`.
- **Fake Stripe payment intents and refunds:** generated from Shopify-style orders so payment attempts, successful charges, and refund records reconcile with the operational order facts.
- **Fake Meta/Facebook Ads, Google Ads, and Klaviyo campaign activity:** generated from the shared funnel in BigQuery SQL under `assets/raw/`.

The ecommerce template originally generated external `ingestr` raw assets for Shopify, Klaviyo, Facebook Ads, Google Ads, and GA4. Those modeled assets were replaced with same-schema fake BigQuery raw assets so the dashboard-ready project can be validated and run end to end without third-party credentials.

Current generated window: **2025-01-01 through 2026-06-05**. The refreshed model contains **680,778 orders**, **371,000 customer profiles**, **347,765 ordering customer emails**, **342,570 paid customers**, **120 products**, **51 state/district codes**, and **70 city markets**. Daily orders have a **1,292-order median** with deliberate event outliers from **365** orders on the outage low end to **2,667** orders on campaign-spike days; orders contain **1 to 10 items** with a **$58.95 average order value**. The model now includes **$38.7M net revenue**, **12.8M web sessions**, **$7.0M paid-media spend**, **3,760,690 GA4 page/ecommerce event rows**, **680,778 Stripe payment intents**, and **23,705 Stripe refund records**.

Separate ingestr assets have been added for source-connection proofing. Most materialize into BigQuery tables with `_test` suffixes and are intentionally not referenced by staging, reports, or dashboards. HubSpot materializes into permanent raw tables without the `_test` suffix.

## Injected Special Events

The fake dataset includes deterministic outlier events for dashboard storytelling, incident triage, and anomaly-detection workflows. The event catalog is defined in `raw.special_events`; downstream ads, GA4, Shopify-style orders, and report assets inherit the same event identifiers.

| Event | Date/time | What was injected |
|---|---:|---|
| Google search broad-match test failure | **2026-01-12 through 2026-01-18** | A one-week paid-search campaign failure. Spend is lifted by about 8%, but CTR and click-to-order conversion are sharply reduced. The generated impact table shows **0.63% CTR**, weak conversion, and negative contribution profit for the window. |
| Website checkout outage and recovery | **2026-02-04 10:00 through 2026-02-04 21:59 UTC**, with recovery on **2026-02-05** | A 12-hour outage in `raw.ga4_hourly_sessions` pushes traffic close to zero during the outage hours and suppresses purchases. Shopify order timestamps are shaped to the same outage/recovery pattern, and hourly GA4 purchase events are counted from paid Shopify orders. The following day gets a modest return-to-shop bump from customers who came back to finish orders. |
| Black Tote Bag defect refund incident | **2026-02-20 through 2026-02-24** | A product-quality issue for `prod_accessories_09` / **Black Tote Bag**. Orders for the affected product are forced into a partially refunded pattern above 90%; current generated output is **96.47% refund rate** for the incident window. |
| Instagram trail-shoe launch and stockout | **2026-03-10 through 2026-03-21** | Paid social promotes `prod_shoes_04` / **Heather Gray Trail Shoes**. The launch phase from **2026-03-10 through 2026-03-17** increases CTR/conversion and forces the shoe as the primary product on many paid-social orders. The stockout phase from **2026-03-18 through 2026-03-21** keeps Instagram spend and clicks running, but the promoted shoe is no longer sold and paid-social orders collapse to near zero. |
| Instagram spring outfit campaign win | **2026-04-08 through 2026-04-14** | A short successful paid-social campaign with higher CTR and click-to-order conversion, producing a temporary revenue and contribution-profit bump. |
| Google Memorial Day search campaign win | **2026-05-11 through 2026-05-17** | A successful seasonal paid-search campaign with improved CTR and conversion, producing a short revenue and ROAS lift before the normal Memorial Day demand window. |

## Assets

### Raw

- `raw.us_markets` - fake US market dimension covering all states plus DC.
- `raw.special_events` - fake campaign failure, website outage, stockout campaign, successful campaign, and product-defect event catalog.
- `raw.marketing_funnel` - fake channel/date/state/city funnel facts used by ads, GA4, Klaviyo, and order generation, including injected event multipliers.
- `raw.shopify_orders` - fake Shopify-style apparel order facts generated one-for-one from funnel conversions, with customer assignment modeled through deterministic new, recent-repeat, mid-age-repeat, and loyal-repeat lifecycle pools.
- `raw.shopify_customers` - fake Shopify-style customer profiles.
- `raw.shopify_products` - fake active apparel product catalog.
- `raw.shopify_inventory` - fake inventory levels.
- `raw.ga4_sessions` - fake GA4 channel-session rows; purchase events reconcile to paid or partially refunded Shopify-style orders by date, channel, state, and city.
- `raw.ga4_hourly_sessions` - fake hourly GA4 traffic used to show the 12-hour website outage and recovery bump; hourly purchase events are counted from paid Shopify-style orders by hour.
- `raw.ga4_events` - fake GA4 page-path and ecommerce event counts, including home, collection, product, cart, checkout, payment, purchase, and order-confirmation activity.
- `raw.stripe_payment_intents` - fake Stripe-style payment intents generated one-for-one from Shopify-style order attempts.
- `raw.stripe_refunds` - fake Stripe-style refunds generated for partially refunded Shopify-style orders.
- `raw.facebook_ad_insights` and `raw.facebook_campaigns` - fake Meta Ads facts and metadata.
- `raw.google_ad_insights` and `raw.google_campaigns` - fake Google Ads facts and metadata.
- `raw.klaviyo_campaigns`, `raw.klaviyo_metrics`, and `raw.klaviyo_flows` - fake Klaviyo campaign and flow data.

External ingestr raw assets:

- `raw.shopify_orders_test`, `raw.shopify_products_test`, and `raw.shopify_customers_test` - Shopify ingestr checks.
- `raw.hubspot_contacts`, `raw.hubspot_companies`, and `raw.hubspot_deals` - HubSpot ingestr assets.
- `raw.stripe_customers_test` and `raw.stripe_payment_intents_test` - Stripe ingestr checks.
- `raw.klaviyo_profiles_test`, `raw.klaviyo_events_test`, and `raw.klaviyo_campaigns_test` - Klaviyo ingestr checks.
- `raw.ga4_custom_report_test` - GA4 custom-report ingestr check.
- `raw.google_ads_campaigns_test` and `raw.facebook_campaigns_test` - optional ad-platform ingestr checks.

### Staging

- `staging.stg_orders` - deduplicated and standardized Shopify-style orders with channel, geography, item count, COGS, and shipping cost.
- `staging.stg_customers` - deduplicated customers with exact lifetime counts/spend recomputed from paid order facts, with first-seen timestamps anchored to first paid or partially refunded order where available.
- `staging.stg_products` - active apparel product catalog with price, inventory, SKU, and unit COGS.
- `staging.stg_web_sessions` - normalized GA4 sessions with exact purchase events by channel and market.
- `staging.stg_marketing_spend` - combined daily channel spend and campaign activity with geography for paid channels.

### Reports

- `reports.rpt_daily_revenue` - daily revenue, order, AOV, discount, tax, item, COGS, shipping, gross-profit, and cancellation metrics.
- `reports.rpt_daily_kpis` - daily executive KPI table joining revenue, customer, session, and spend metrics.
- `reports.rpt_marketing_roi` - daily channel ROI with exact generated order attribution by channel.
- `reports.rpt_customer_cohorts` - customer cohort retention and revenue metrics.
- `reports.rpt_product_performance` - active product catalog plus primary-product sales, revenue, and gross-profit context.
- `reports.rpt_special_event_impact` - injected event outcomes with spend, sessions, orders, revenue, contribution profit, refund rate, and baseline deltas.
- `reports.rpt_payment_reconciliation` - daily Shopify-vs-Stripe payment reconciliation with successful-order and refund-record gap checks.

### DAC Dashboards

- `Bruin Shop Overview` - executive summary across sales, marketing, operations, and catalog.
- `Bruin Shop Sales` - gross/net revenue, AOV, discounts, cancellations, and payment status.
- `Bruin Shop Marketing` - paid ROAS, web-channel conversion, and channel performance detail.
- `Bruin Shop Operations` - payment exceptions, Shopify/Stripe reconciliation, fulfillment status, and inventory value.
- `Bruin Shop Customers and Catalog` - customer repeat behavior, cohort coverage, catalog, and inventory detail.
- `Bruin Shop Special Events` - event-specific analysis with date, channel, and event-type filters.

## Run Commands

```bash
bruin validate bruin-shop/
bruin query --connection bruin-playground-arsalan --query "SELECT COUNT(*) AS row_count FROM reports.rpt_daily_kpis"
```

When using the local gitignored config created for this validation workspace:

```bash
bruin validate --config-file .context/bruin-shop.bruin.yml bruin-shop/
bruin run --config-file .context/bruin-shop.bruin.yml --full-refresh --downstream bruin-shop/assets/raw/shopify_products.sql
bruin run --config-file .context/bruin-shop.bruin.yml --full-refresh --downstream bruin-shop/assets/raw/us_markets.sql
bruin run --config-file .context/bruin-shop.bruin.yml bruin-shop/assets/raw/shopify_products_test.asset.yml
```

Use the first two `--downstream` commands to refresh the dashboard-ready SQL model. Run connector-test assets separately; they require valid source credentials and are not needed by the DAC dashboards.

Connection-test credentials are read from environment variables in `.context/bruin-shop.bruin.yml`:

```bash
export BRUIN_SHOPIFY_STORE_URL="your-dev-store.myshopify.com"
export BRUIN_SHOPIFY_CLIENT_ID="..."
export BRUIN_SHOPIFY_CLIENT_SECRET="shpss_..."
export BRUIN_HUBSPOT_PRIVATE_APP_TOKEN="pat-..."
export BRUIN_STRIPE_SECRET_KEY="sk_test_..."
export BRUIN_KLAVIYO_PRIVATE_API_KEY="pk_..."
export BRUIN_GA4_SERVICE_ACCOUNT_FILE="/absolute/path/to/ga4-service-account.json"
export BRUIN_GA4_PROPERTY_ID="123456789"
export BRUIN_GOOGLE_ADS_CUSTOMER_ID="1234567890"
export BRUIN_GOOGLE_ADS_DEVELOPER_TOKEN="..."
export BRUIN_GOOGLE_ADS_SERVICE_ACCOUNT_FILE="/absolute/path/to/google-ads-service-account.json"
export BRUIN_FACEBOOK_ADS_ACCESS_TOKEN="..."
export BRUIN_FACEBOOK_ADS_ACCOUNT_ID="act_1234567890"
```

Run only the source you are testing. After each ingestr run, verify the table:

```bash
bruin query --config-file .context/bruin-shop.bruin.yml --connection bruin-playground-arsalan --query "SELECT COUNT(*) AS row_count FROM raw.shopify_products_test"
```

Optional: seed a Shopify development store with small synthetic connector-test data:

```bash
BRUIN_SHOPIFY_STORE_URL="phbrz1-ht.myshopify.com" \
BRUIN_SHOPIFY_CLIENT_ID="..." \
BRUIN_SHOPIFY_CLIENT_SECRET="shpss_..." \
python3 bruin-shop/scripts/seed_shopify_test_data.py \
  --products 10 \
  --customers 30 \
  --orders 5
```

This script is only for `_test` connector proofing, not the dashboard-ready modeled data. Trial stores can be throttled to roughly one created order per minute. The script defaults to a 65-second delay between order creates; paid stores can lower it with `--order-delay-seconds 0`.

Ingest seeded Shopify data with an interval that includes the seed timestamp:

```bash
bruin run --start-date 2026-06-05T00:00:00 --end-date 2026-06-05T23:59:59 --config-file .context/bruin-shop.bruin.yml bruin-shop/assets/raw/shopify_products_test.asset.yml
bruin run --start-date 2026-06-05T00:00:00 --end-date 2026-06-05T23:59:59 --config-file .context/bruin-shop.bruin.yml bruin-shop/assets/raw/shopify_customers_test.asset.yml
bruin run --start-date 2026-06-05T00:00:00 --end-date 2026-06-05T23:59:59 --config-file .context/bruin-shop.bruin.yml bruin-shop/assets/raw/shopify_orders_test.asset.yml
```

Dashboard commands:

```bash
dac --config .context/bruin-shop.bruin.yml validate --dir bruin-shop/dashboard-dac
dac --config .context/bruin-shop.bruin.yml check --dir bruin-shop/dashboard-dac
dac --config .context/bruin-shop.bruin.yml serve --dir bruin-shop/dashboard-dac --port 8323
```

Local dashboard URL for this validation run: `http://localhost:8323`

## Known Limitations

- Data is deterministic fake data, not real connected Shopify, GA4, ad-platform, or Klaviyo data.
- Customer lifecycle behavior is deterministic scenario data, not a predictive retention model; it is designed to make first purchase, repeat purchase, and cohort-retention relationships internally coherent.
- The fake raw assets preserve the fields used by this template's staging layer plus the generated cost/geography fields, not every field available from the real APIs.
- Marketing revenue attribution is exact inside the generated dataset because orders are created from `raw.marketing_funnel.conversions`; it is not evidence of real incrementality or platform attribution behavior.
- Special events are deterministic scenario injections for dashboard storytelling and anomaly testing. They are intentionally stronger than normal seasonality and should not be interpreted as observed operational incidents.
- Product performance attributes each order to one generated primary product; full SKU-level line-item arrays are out of scope.
- Shipment analysis is limited to Shopify order-level fulfillment status because the template does not include shipment packages, carriers, tracking events, or delivery timestamps.
- Payment analysis uses generated Shopify financial status plus synthetic Stripe payment intents and refunds. The `_test` Stripe ingestr assets remain source-connection checks and are not consumed by the dashboard-ready modeled layer.
