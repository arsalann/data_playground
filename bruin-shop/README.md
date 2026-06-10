# bruin-shop

End-to-end Bruin ecommerce template validation project using deterministic fake apparel data with the same schema consumed by the generated ecommerce staging and report layers.

## AI Agent Data Access Rule

When a user asks about Bruin Shop data, AI agents must strictly and only use tables inside the `bruin_shop_*` datasets:

- `bruin_shop_raw`
- `bruin_shop_staging`
- `bruin_shop_reports`

Do not query the legacy shared `raw`, `staging`, or `reports` datasets for Bruin Shop questions. Those unprefixed table names are deprecated for this pipeline and may be absent or stale. Use `bruin query` against fully qualified `bruin-playground-arsalan.bruin_shop_*.<table>` tables when inspecting data.

## Data Sources

- **Fake US markets, special events, and acquisition funnel:** `bruin_shop_raw.us_markets`, `bruin_shop_raw.special_events`, and `bruin_shop_raw.marketing_funnel` generate state/city demand, injected campaign/outage/stockout/defect events, channel spend, impressions, clicks, sessions, and conversions.
- **Fake Shopify-style apparel orders, customers, products, and inventory:** generated in BigQuery SQL under `assets/raw/` for T-shirts, pants, shoes, and accessories.
- **Fake GA4 sessions, hourly outage traffic, and page-level ecommerce events:** generated from the shared funnel and Shopify-style orders in BigQuery SQL under `assets/raw/`.
- **Fake Stripe payment intents and refunds:** generated from Shopify-style orders so payment attempts, successful charges, and refund records reconcile with the operational order facts.
- **Fake Meta/Facebook Ads, Google Ads, and Klaviyo campaign activity:** generated from the shared funnel in BigQuery SQL under `assets/raw/`.

The ecommerce template originally generated external `ingestr` raw assets for Shopify, Klaviyo, Facebook Ads, Google Ads, and GA4. Those modeled assets were replaced with same-schema fake BigQuery raw assets so the dashboard-ready project can be validated and run end to end without third-party credentials.

Current generated window: **2025-01-01 through 2026-06-08**. The refreshed model contains **685,313 orders**, **371,000 customer profiles**, **348,382 ordering customer emails**, **343,224 paid customers**, **120 products**, **51 state/district codes**, and **70 city markets**. Daily orders have a **1,288-order median** with deliberate event outliers from **365** orders on the outage low end to **2,667** orders on campaign-spike days; orders contain **1 to 10 items** with a **$58.88 average order value**. The model now includes **$38.9M net revenue**, **12.9M web sessions**, **$7.0M paid-media spend**, **3,784,505 GA4 page/ecommerce event rows**, **685,313 Stripe payment intents**, and **23,864 Stripe refund records**.

Separate Shopify and HubSpot ingestr assets are active for source-connection proofing because this validation workspace has local connections for those sources. The remaining connector-test definitions are parked as `.asset.yml.tmpl` template files so Bruin excludes them from pipeline discovery until valid local credentials are available.

`pipeline.yml` declares the active connection defaults explicitly:

- `google_cloud_platform: bruin-playground-arsalan` for BigQuery destination work.
- `shopify: bruin-shop-shopify-test` for active Shopify ingestr proofing assets.
- `hubspot: bruin-shop-hubspot-test` for active HubSpot ingestr proofing assets.

## Modeling Contract

The dashboard-ready data is deterministic fake data, but it is generated with explicit reconciliation contracts so it behaves like a coherent ecommerce business instead of independent random tables.

- Orders are generated one-for-one from `bruin_shop_raw.marketing_funnel.conversions`, so channel, campaign, market, session, order, revenue, COGS, shipping, and contribution-profit metrics reconcile by date and geography.
- Customer assignment in `bruin_shop_raw.shopify_orders` is lifecycle-modeled. Each market accumulates new customers over time; repeat orders draw only from previously acquired customers and are biased toward recent, mid-age, and loyal repeat pools.
- The modeled Shopify SQL assets declare lineage dependencies on the corresponding active Shopify `_test` ingestr assets. This keeps connector proofing visible in Bruin lineage, while the dashboard model still generates its analytical rows in SQL rather than reading the small dev-store seed tables.
- `bruin_shop_staging.stg_customers.first_seen_at` is anchored to the first paid or partially refunded order when one exists. That keeps paid cohort retention internally consistent: month 0 retention is 100%, and later months show repeat activity rather than first-purchase leakage.
- GA4 purchase events, checkout/order-confirmation page events, Shopify successful orders, Stripe succeeded payment intents, and Stripe refund records are generated from the same successful-order set.
- Special-event identifiers flow through ads, sessions, orders, Stripe payments/refunds, reports, and DAC filters, so dashboard slices preserve the scenario context.

Current customer lifecycle checks:

| Metric | Current value |
|---|---:|
| Paid customers | 343,224 |
| Paid or partially refunded orders | 661,342 |
| One-order paid customers | 42.57% |
| Repeat paid customers | 57.43% |
| Customers with 5+ paid orders | 2.33% |
| Cohort month-0 retention | 100.00% |
| January 2025 cohort month-1 retention | 26.84% |
| January 2025 cohort month-6 retention | 5.64% |

## Injected Special Events

The fake dataset includes deterministic outlier events for dashboard storytelling, incident triage, and anomaly-detection workflows. The event catalog is defined in `bruin_shop_raw.special_events`; downstream ads, GA4, Shopify-style orders, and report assets inherit the same event identifiers.

| Event | Date/time | What was injected |
|---|---:|---|
| Google search broad-match test failure | **2026-01-12 through 2026-01-18** | A one-week paid-search campaign failure. Spend is lifted by about 8%, but CTR and click-to-order conversion are sharply reduced. The generated impact table shows **0.63% CTR**, weak conversion, and negative contribution profit for the window. |
| Website checkout outage and recovery | **2026-02-04 10:00 through 2026-02-04 21:59 UTC**, with recovery on **2026-02-05** | A 12-hour outage in `bruin_shop_raw.ga4_hourly_sessions` pushes traffic close to zero during the outage hours and suppresses purchases. Shopify order timestamps are shaped to the same outage/recovery pattern, and hourly GA4 purchase events are counted from paid Shopify orders. The following day gets a modest return-to-shop bump from customers who came back to finish orders. |
| Black Tote Bag defect refund incident | **2026-02-20 through 2026-02-24** | A product-quality issue for `prod_accessories_09` / **Black Tote Bag**. Orders for the affected product are forced into a partially refunded pattern above 90%; current generated output is **96.47% refund rate** for the incident window. |
| Instagram trail-shoe launch and stockout | **2026-03-10 through 2026-03-21** | Paid social promotes `prod_shoes_04` / **Heather Gray Trail Shoes**. The launch phase from **2026-03-10 through 2026-03-17** increases CTR/conversion and forces the shoe as the primary product on many paid-social orders. The stockout phase from **2026-03-18 through 2026-03-21** keeps Instagram spend and clicks running, but the promoted shoe is no longer sold and paid-social orders collapse to near zero. |
| Instagram spring outfit campaign win | **2026-04-08 through 2026-04-14** | A short successful paid-social campaign with higher CTR and click-to-order conversion, producing a temporary revenue and contribution-profit bump. |
| Google Memorial Day search campaign win | **2026-05-11 through 2026-05-17** | A successful seasonal paid-search campaign with improved CTR and conversion, producing a short revenue and ROAS lift before the normal Memorial Day demand window. |
| Google Search - Summer Sale | **2026-06-07 through 2026-06-08** | A short paid-search summer sale campaign with elevated seasonal demand and order volume. Campaign ID: `google_search_summer_sale`. |

## Assets

### Raw

- `bruin_shop_raw.us_markets` - fake US market dimension covering all states plus DC.
- `bruin_shop_raw.special_events` - fake campaign failure, website outage, stockout campaign, successful campaign, and product-defect event catalog.
- `bruin_shop_raw.marketing_funnel` - fake channel/date/state/city funnel facts used by ads, GA4, Klaviyo, and order generation, including injected event multipliers.
- `bruin_shop_raw.shopify_orders` - fake Shopify-style apparel order facts generated one-for-one from funnel conversions, with customer assignment modeled through deterministic new, recent-repeat, mid-age-repeat, and loyal-repeat lifecycle pools.
- `bruin_shop_raw.shopify_customers` - fake Shopify-style customer profiles.
- `bruin_shop_raw.shopify_products` - fake active apparel product catalog.
- `bruin_shop_raw.shopify_inventory` - fake inventory levels.
- `bruin_shop_raw.ga4_sessions` - fake GA4 channel-session rows; purchase events reconcile to paid or partially refunded Shopify-style orders by date, channel, state, and city.
- `bruin_shop_raw.ga4_hourly_sessions` - fake hourly GA4 traffic used to show the 12-hour website outage and recovery bump; hourly purchase events are counted from paid Shopify-style orders by hour.
- `bruin_shop_raw.ga4_events` - fake GA4 page-path and ecommerce event counts, including home, collection, product, cart, checkout, payment, purchase, and order-confirmation activity.
- `bruin_shop_raw.stripe_payment_intents` - fake Stripe-style payment intents generated one-for-one from Shopify-style order attempts.
- `bruin_shop_raw.stripe_refunds` - fake Stripe-style refunds generated for partially refunded Shopify-style orders.
- `bruin_shop_raw.facebook_ad_insights` and `bruin_shop_raw.facebook_campaigns` - fake Meta Ads facts and metadata.
- `bruin_shop_raw.google_ad_insights` and `bruin_shop_raw.google_campaigns` - fake Google Ads facts and metadata.
- `bruin_shop_raw.klaviyo_campaigns` and `bruin_shop_raw.klaviyo_metrics` - fake Klaviyo campaign and metric data.

Active external ingestr assets:

- `bruin_shop_raw.shopify_orders_test`, `bruin_shop_raw.shopify_products_test`, and `bruin_shop_raw.shopify_customers_test` - Shopify ingestr checks.
- `bruin_shop_raw.hubspot_contacts`, `bruin_shop_raw.hubspot_companies`, and `bruin_shop_raw.hubspot_deals` - HubSpot ingestr checks.

Parked external ingestr definitions:

- `stripe_customers_test.asset.yml.tmpl` and `stripe_payment_intents_test.asset.yml.tmpl` - Stripe ingestr checks.
- `klaviyo_profiles_test.asset.yml.tmpl`, `klaviyo_events_test.asset.yml.tmpl`, and `klaviyo_campaigns_test.asset.yml.tmpl` - Klaviyo ingestr checks.
- `ga4_custom_report_test.asset.yml.tmpl` - GA4 custom-report ingestr check.
- `google_ads_campaigns_test.asset.yml.tmpl` and `facebook_campaigns_test.asset.yml.tmpl` - optional ad-platform ingestr checks.

### Staging

- `bruin_shop_staging.stg_orders` - deduplicated and standardized Shopify-style orders with channel, geography, item count, COGS, and shipping cost.
- `bruin_shop_staging.stg_customers` - deduplicated customers with exact lifetime counts/spend recomputed from paid order facts, with first-seen timestamps anchored to first paid or partially refunded order where available.
- `bruin_shop_staging.stg_products` - active apparel product catalog with price, inventory, SKU, and unit COGS.
- `bruin_shop_staging.stg_web_sessions` - normalized GA4 sessions with exact purchase events by channel and market.
- `bruin_shop_staging.stg_marketing_spend` - combined daily channel spend and campaign activity with geography for paid channels.

### Reports

- `bruin_shop_reports.rpt_daily_revenue` - daily revenue, order, AOV, discount, tax, item, COGS, shipping, gross-profit, and cancellation metrics.
- `bruin_shop_reports.rpt_daily_kpis` - daily executive KPI table joining revenue, customer, session, and spend metrics.
- `bruin_shop_reports.rpt_marketing_roi` - daily channel ROI with exact generated order attribution by channel.
- `bruin_shop_reports.rpt_customer_cohorts` - customer cohort retention and revenue metrics.
- `bruin_shop_reports.rpt_product_performance` - active product catalog plus primary-product sales, revenue, and gross-profit context.
- `bruin_shop_reports.rpt_special_event_impact` - injected event outcomes with spend, sessions, orders, revenue, contribution profit, refund rate, and baseline deltas.
- `bruin_shop_reports.rpt_payment_reconciliation` - daily Shopify-vs-Stripe payment reconciliation with successful-order and refund-record gap checks.

### DAC Dashboards

- `Bruin Shop` - single tabbed DAC dashboard with Overview, Sales, Marketing, Operations, Customers and Catalog, and Special Events tabs. It covers executive KPIs, revenue quality, paid ROAS, web-channel conversion, payment exceptions, Shopify/Stripe reconciliation, fulfillment status, inventory value, customer repeat behavior, cohort coverage, catalog detail, and event-specific analysis.

## Run Commands

```bash
bruin validate bruin-shop/
bruin query --connection bruin-playground-arsalan --query "SELECT COUNT(*) AS row_count FROM bruin_shop_reports.rpt_daily_kpis"
```

When using the local gitignored config created for this validation workspace:

```bash
bruin run --config-file .context/bruin-shop.bruin.yml --no-validation --full-refresh bruin-shop/assets/raw/special_events.sql
bruin validate --config-file .context/bruin-shop.bruin.yml bruin-shop/
bruin run --config-file .context/bruin-shop.bruin.yml --full-refresh --downstream bruin-shop/assets/raw/shopify_products.sql
bruin run --config-file .context/bruin-shop.bruin.yml --full-refresh --downstream bruin-shop/assets/raw/us_markets.sql
bruin run --config-file .context/bruin-shop.bruin.yml bruin-shop/assets/raw/shopify_products_test.asset.yml
```

Use the `special_events`, `shopify_products`, and `us_markets` roots to refresh the dashboard-ready SQL model. On a brand-new BigQuery project, create the `bruin_shop_raw`, `bruin_shop_staging`, and `bruin_shop_reports` datasets first, then use `--no-validation` for the first materialization pass because upstream tables do not exist yet. Run active Shopify and HubSpot connector-test assets separately; they require valid source credentials and are not needed by the DAC dashboards. Parked `.tmpl` connector definitions should only be restored when valid local credentials are available for that source.

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
bruin query --config-file .context/bruin-shop.bruin.yml --connection bruin-playground-arsalan --query "SELECT COUNT(*) AS row_count FROM bruin_shop_raw.shopify_products_test"
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

## Validation

Latest validation run in this workspace:

```bash
bruin query --connection bruin-playground-arsalan --query "CREATE SCHEMA IF NOT EXISTS `bruin-playground-arsalan.bruin_shop_raw` OPTIONS(location='US')"
bruin query --connection bruin-playground-arsalan --query "CREATE SCHEMA IF NOT EXISTS `bruin-playground-arsalan.bruin_shop_staging` OPTIONS(location='US')"
bruin query --connection bruin-playground-arsalan --query "CREATE SCHEMA IF NOT EXISTS `bruin-playground-arsalan.bruin_shop_reports` OPTIONS(location='US')"
bruin run --no-validation --full-refresh bruin-shop/assets/raw/us_markets.sql
bruin run --no-validation --full-refresh bruin-shop/assets/raw/special_events.sql
bruin run --no-validation --full-refresh bruin-shop/assets/raw/shopify_products.sql
bruin run --no-validation --full-refresh <each dashboard-ready SQL asset in dependency order>
bruin validate bruin-shop
dac validate --dir bruin-shop/dashboard-dac
dac check --dir bruin-shop/dashboard-dac
```

Results:

- The `bruin_shop_raw`, `bruin_shop_staging`, and `bruin_shop_reports` datasets were created in BigQuery.
- All dashboard-ready SQL assets were rebuilt under the `bruin_shop_*` datasets.
- `bruin validate` passed all 44 assets.
- `dac validate` passed the `Bruin Shop` tabbed dashboard.
- `dac check` passed the `Bruin Shop` tabbed dashboard with 99 widgets.
- Local dashboard URL: `http://localhost:8323`.

Core reconciliation checks:

| Check | Result |
|---|---:|
| GA4 purchase events vs paid/partially refunded Shopify orders | 0 gap |
| GA4 order-confirmation page views vs paid Shopify orders | 0 gap |
| Stripe payment intents vs Shopify order attempts | 0 gap |
| Stripe succeeded intents vs paid/partially refunded Shopify orders | 0 gap |
| Stripe refund records vs partially refunded Shopify orders | 0 gap |
| Shopify order emails missing customer profiles | 0 rows |

## Known Limitations

- Data is deterministic fake data, not real connected Shopify, GA4, ad-platform, or Klaviyo data.
- Customer lifecycle behavior is deterministic scenario data, not a predictive retention model; it is designed to make first purchase, repeat purchase, and cohort-retention relationships internally coherent.
- The fake raw assets preserve the fields used by this template's staging layer plus the generated cost/geography fields, not every field available from the real APIs.
- Marketing revenue attribution is exact inside the generated dataset because orders are created from `bruin_shop_raw.marketing_funnel.conversions`; it is not evidence of real incrementality or platform attribution behavior.
- Special events are deterministic scenario injections for dashboard storytelling and anomaly testing. They are intentionally stronger than normal seasonality and should not be interpreted as observed operational incidents.
- Product performance attributes each order to one generated primary product; full SKU-level line-item arrays are out of scope.
- Shipment analysis is limited to Shopify order-level fulfillment status because the template does not include shipment packages, carriers, tracking events, or delivery timestamps.
- Payment analysis uses generated Shopify financial status plus synthetic Stripe payment intents and refunds. The parked `.tmpl` Stripe ingestr definitions remain source-connection checks and are not consumed by the dashboard-ready modeled layer.
