# self-heal-shop

BigQuery-backed fixture pipeline for testing self-healing pipeline skills against e-commerce data. It generates deterministic orders and a deterministic product catalog, then injects duplicate keys, a product schema rename, a revenue anomaly, and recent empty-source windows.

This pipeline is safe for local `bruin run` testing. It is not a production-pattern pipeline and does not exercise Bruin Cloud actions directly.

For Bruin Cloud testing, schedule this pipeline daily and run a one-time daily backfill from `2026-03-01` through `2026-05-15` to create clean healthy history. Injected failures start on `2026-05-16`.

## Critical Agent Warning

**DO NOT TARGET THE PYTHON DATA GENERATORS.** `assets/raw/orders.py` and `assets/raw/products.py` exist only to create deterministic fixture data and inject known failures. They are not part of the self-healing scenario surface. Do not diagnose, repair, open PRs against, backfill because of, or classify the Python generator logic. Use them only to load fixture data, then run the self-healing skills against the generated BigQuery tables, SQL staging assets, Bruin checks, Cloud run/check state, and `.context/` findings.

## Assets

- `self_heal_test_raw.orders` (`assets/raw/orders.py`) generates daily e-commerce orders with country, product, amount, user, and order date.
- `self_heal_test_raw.products` (`assets/raw/products.py`) generates a small product catalog and intentionally renames `category` to `product_category` after the drift date.
- `self_heal_test_staging.daily_orders` (`assets/staging/daily_orders.sql`) aggregates daily order counts, distinct users, country coverage, and revenue.
- `self_heal_test_staging.daily_revenue` (`assets/staging/daily_revenue.sql`) aggregates revenue by date, country, and product category.

## Skill Scenarios

| Scenario | Date/window | Trigger asset/check | Expected skill path | Expected classification |
|---|---:|---|---|---|
| Duplicate order IDs | Starts `2026-05-16` | BigQuery table `self_heal_test_raw.orders`; downstream staging deduplicates by `order_id` | `pipeline-triage` -> `data-quality-investigate` -> `maintenance-pr` plan if transform/check change is proposed -> `pipeline-report` | `quality-fail`, likely `late-arriving-data` or `dedup-window-too-short` depending investigation framing |
| Country concentration revenue spike | `2026-05-20` | BigQuery table `self_heal_test_raw.orders`, check `daily_revenue_within_2x_28d_median`; metric `self_heal_test_staging.daily_orders.revenue_usd` | `pipeline-triage` -> `anomaly-investigate` -> `pipeline-report` | `anomaly`, `single-dimension-driver` with `country=TR` |
| Product category rename | Active when `BRUIN_END_DATE >= 2026-05-18` | BigQuery table `self_heal_test_raw.products` contains `product_category`; downstream staging accepts either `category` or `product_category` | `pipeline-diagnose` -> `schema-drift-check` -> `maintenance-pr` -> `pipeline-report` | `schema-drift`, `column-renamed` |
| Source stall | `2026-05-24` through `2026-05-25` | BigQuery table `self_heal_test_raw.orders` has no new rows for those dates after fixture load | `pipeline-triage` -> `freshness-sla-check` -> `pipeline-report` | `stale` / `source-down` or `table-frozen`, depending Cloud/table evidence |
| Backfill after fix | Any scoped historical date range after a schema or dedup fix | Warehouse asset `self_heal_test_raw.orders` has append materialization; downstream staging uses `create+replace` | `pipeline-backfill` dry run -> approval if needed -> `pipeline-report` | Approval required for append reruns where rows already exist |

## What This Pipeline Covers

- `pipeline-triage`: schema-drift, quality-fail, stale, and anomaly routing.
- `pipeline-diagnose`: column-not-found pattern matching and recent repo context review.
- `schema-drift-check`: `column-renamed` classification and downstream impact listing.
- `data-quality-investigate`: duplicate natural-key investigation and first-failure interval discovery.
- `anomaly-investigate`: single-dimension driver attribution.
- `freshness-sla-check`: source-stall and table-frozen classification.
- `maintenance-pr`: finding-gated PR shape for a routine column rename or dedup/check adjustment.
- `pipeline-backfill`: dry-run planning for scoped historical reruns after a fix.
- `pipeline-report`: final incident, warning, or digest summary.

It does not directly test Slack posting, GitHub PR creation, Bruin Cloud rerun execution, capacity failures, code-regression attribution, or transient Cloud failures. Those require Cloud/Slack/GitHub context or synthetic Cloud run metadata outside this data fixture.

## Useful Commands

```bash
bruin validate self-heal-shop --output json
bruin run self-heal-shop --start-date 2026-03-01 --end-date 2026-05-15

# Duplicate-key quality fixture.
bruin run self-heal-shop/assets/raw/orders.py --start-date 2026-05-16 --end-date 2026-05-17
bruin run --only checks self-heal-shop/assets/raw/orders.py

# Revenue anomaly fixture. Run enough baseline history for the 28-day median check.
bruin run self-heal-shop/assets/raw/orders.py --start-date 2026-03-01 --end-date 2026-05-21
bruin run self-heal-shop/assets/staging/daily_orders.sql
bruin run --only checks self-heal-shop/assets/raw/orders.py

# Product schema-drift fixture.
bruin run self-heal-shop/assets/raw/products.py --start-date 2026-05-18 --end-date 2026-05-19
bruin run self-heal-shop/assets/staging/daily_revenue.sql

# Lineage and static context.
bruin lineage self-heal-shop/assets/staging/daily_revenue.sql --output json --full
```

## Expected Notes for Agents

- Run scenarios separately when testing alert routing. Whole-pipeline runs after downstream maintenance should allow staging to rebuild from tolerated raw duplicate keys and product category renames.
- Exclude Python generator logic in `assets/raw/orders.py` and `assets/raw/products.py` from self-healing task scope. The Bruin metadata checks may still be adjusted when their blocking behavior no longer matches downstream contracts.
- `daily_revenue_within_2x_28d_median` is intentionally a tracked-metric guardrail; agents should still slice by dimensions instead of only reporting that the check failed.
- The product rename is a routine maintenance-PR candidate only if all downstream references are updated in scope.
- Treat local `bruin run` as allowed only because this is a self-heal test pipeline.
