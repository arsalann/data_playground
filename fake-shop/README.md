# fake-shop

Local DuckDB fixture pipeline for testing self-healing pipeline skills against e-commerce data. It generates deterministic orders and a deterministic product catalog, then injects duplicate keys, a product schema rename, a revenue anomaly, and recent empty-source windows.

This pipeline is safe for local `bruin run` testing. It is not a production-pattern pipeline and does not exercise Bruin Cloud actions directly.

## Assets

- `raw.orders` (`assets/raw/orders.py`) generates daily e-commerce orders with country, product, amount, user, and order date.
- `raw.products` (`assets/raw/products.py`) generates a small product catalog and intentionally renames `category` to `product_category` after the drift date.
- `staging.daily_orders` (`assets/staging/daily_orders.sql`) aggregates daily order counts, distinct users, country coverage, and revenue.
- `staging.daily_revenue` (`assets/staging/daily_revenue.sql`) aggregates revenue by date, country, and product category.

## Skill Scenarios

| Scenario | Date/window | Trigger asset/check | Expected skill path | Expected classification |
|---|---:|---|---|---|
| Duplicate order IDs | Starts `2026-05-15` | `raw.orders.order_id` `unique` column check | `pipeline-triage` -> `data-quality-investigate` -> `maintenance-pr` plan if transform/check change is proposed -> `pipeline-report` | `quality-fail`, likely `late-arriving-data` or `dedup-window-too-short` depending investigation framing |
| Country concentration revenue spike | `2026-05-20` | `raw.orders` check `daily_revenue_within_4x_28d_median`; metric `staging.daily_orders.revenue_usd` | `pipeline-triage` -> `anomaly-investigate` -> `pipeline-report` | `anomaly`, `single-dimension-driver` with `country=TR` |
| Product category rename | Active when `BRUIN_END_DATE >= 2026-04-01` | `staging.daily_revenue` references `raw.products.category` after source emits `product_category` | `pipeline-diagnose` -> `schema-drift-check` -> `maintenance-pr` -> `pipeline-report` | `schema-drift`, `column-renamed` |
| Recent source stall | Today and yesterday | `raw.orders` returns 0 rows for the latest two dates | `pipeline-triage` -> `freshness-sla-check` -> `pipeline-report` | `stale` / `source-down` or `table-frozen`, depending Cloud/table evidence |
| Backfill after fix | Any scoped historical date range after a schema or dedup fix | `raw.orders` uses `append`; downstream staging uses `create+replace` | `pipeline-backfill` dry run -> approval if needed -> `pipeline-report` | Approval required for append reruns where rows already exist |

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
bruin validate fake-shop --output json

# Duplicate-key quality fixture.
bruin run fake-shop/assets/raw/orders.py --start-date 2026-05-15 --end-date 2026-05-15
bruin run --only checks fake-shop/assets/raw/orders.py

# Revenue anomaly fixture. Run enough baseline history for the 28-day median check.
bruin run fake-shop/assets/raw/orders.py --start-date 2026-04-15 --end-date 2026-05-20
bruin run fake-shop/assets/staging/daily_orders.sql
bruin run --only checks fake-shop/assets/raw/orders.py

# Product schema-drift fixture.
bruin run fake-shop/assets/raw/products.py --start-date 2026-04-01 --end-date 2026-04-01
bruin run fake-shop/assets/staging/daily_revenue.sql

# Lineage and static context.
bruin lineage fake-shop/assets/staging/daily_revenue.sql --output json --full
```

## Expected Notes for Agents

- Run scenarios separately. A whole-pipeline run after `2026-04-01` can fail at `staging.daily_revenue` before later quality/anomaly review finishes.
- `daily_revenue_within_4x_28d_median` is intentionally a tracked-metric guardrail; agents should still slice by dimensions instead of only reporting that the check failed.
- The product rename is a routine maintenance-PR candidate only if all downstream references are updated in scope.
- Treat local `bruin run` as allowed only because this is a fake-data test pipeline.
