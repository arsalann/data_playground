# Contoso v2 Multi-Department Data Pipeline

Fully-synthetic, story-driven port of the original `contoso/` pipeline. No Contoso V2 parquet dependency — every raw table is generated end-to-end with regime-aware seasonality, real-world event windows, weekday/holiday spikes, sparse keys, mixture distributions, and integrity quirks so the data reads like a real 16-year operating history.

## Coverage

- **Date range**: 2010-01-01 → 2026-05-01
- **Connection**: `gcp-default` (project `bruin-demo-data`)
- **Schemas**: `contoso_v2_raw`, `contoso_v2_staging`, `contoso_v2_reports`

## Story arc (named regimes)

| Period | Regime | Notable effects |
|---|---|---|
| 2010–2014 | `early_growth` | Small company, ~150 employees, ~30K orders/yr by 2014, 8 stores |
| 2015–2017 | `expansion` | E-commerce push, +25%/yr revenue, hiring acceleration, store count 8→40 |
| 2018–2019 | `mature_ops` | +6%/yr, optimization, IT/data hiring picks up |
| 2020-03–2020-06 | `covid_shock` | Store revenue −60%, online +180%, 25 stores temp-closed, supply-chain transit +14d, hiring freeze, support tickets 2× volume, ~6% layoffs |
| 2020-07–2021-12 | `recovery` | Online-led recovery, e-commerce share permanently elevated |
| 2022 | `inflation` | Unit costs +12%, ad spend efficiency drops, AP invoice amounts +8% |
| 2023–2024 | `ai_boom` | AI-product demand surge, hiring spike (Engineering), ad spend doubles |
| 2025 | `stabilization` | Slower hiring, ops optimization |
| 2026-01–2026-05 | `recent` | Partial year — data ends here so analysts see "as of" 2026-05-01 |

## Realism additions vs. v1

- **Seasonality**: Q4 lift (Nov 1.4×, Dec 1.6×), Black Friday 3.5×, Cyber Monday 3.0×, Boxing Day, July 4, Memorial/Labor Day, back-to-school
- **Weekday pattern**: Sat/Sun 1.15×, Mon 0.85×, Wed peak 1.05×
- **Mixture distributions**: payroll bonuses Q1/Q4, gamma-distributed transit times, lognormal invoice amounts, Pareto vendor spend
- **Outliers and exceptions**: cancellations (4%), returns with negative quantities (1.5%), supply-chain delayed shipments, payroll reversals, GL reversal entries, duplicate invoices, line-level clearance discounts >40%
- **Sparse keys**: ~12% gaps in employee/order/ticket IDs (closed/voided/deleted)
- **Integrity quirks**: 1.5% NULL/malformed records, 2% ad tracking errors, 1% returns dated before delivery
- **Event-driven shifts**: COVID stockouts, inflation cost increases, AI-boom hiring, channel-mix evolution (in-store 70%→35%, paid social 5%→25%)

## Assets

### Raw (23 tables)

| Department | Table | Approximate rows |
|---|---|---|
| Sales | `contoso_v2_raw.sales` | ~3.6M |
| Sales | `contoso_v2_raw.orders` | ~3M |
| Sales | `contoso_v2_raw.order_rows` | ~3.6M |
| Sales | `contoso_v2_raw.customers` | ~140K |
| Sales | `contoso_v2_raw.products` | ~2.5K |
| Sales | `contoso_v2_raw.stores` | 75 |
| Sales | `contoso_v2_raw.dates` | ~5.9K |
| Sales | `contoso_v2_raw.currency_exchange` | ~24K |
| HR | `contoso_v2_raw.departments` | 12 |
| HR | `contoso_v2_raw.employees` | ~3.2K |
| HR | `contoso_v2_raw.payroll` | ~700K |
| HR | `contoso_v2_raw.job_postings` | ~1.8K |
| Finance | `contoso_v2_raw.gl_journal_entries` | ~3M |
| Finance | `contoso_v2_raw.budgets` | ~3.5K |
| Finance | `contoso_v2_raw.accounts_payable` | ~30K |
| Marketing | `contoso_v2_raw.campaigns` | ~600 |
| Marketing | `contoso_v2_raw.ad_spend_daily` | ~120K |
| Marketing | `contoso_v2_raw.campaign_attribution` | ~1.2M |
| Engineering | `contoso_v2_raw.sprint_tickets` | ~30K |
| Engineering | `contoso_v2_raw.deployments` | ~50K |
| Operations | `contoso_v2_raw.inventory_snapshots` | ~1.8M |
| Operations | `contoso_v2_raw.shipments` | ~3M |
| Support | `contoso_v2_raw.support_tickets` | ~250K |

### Staging (10 models) and Reports (7 models)

Schemas mirror the v1 pipeline; queries reference `contoso_v2_*` schemas.

## Run

```bash
# Validate
bruin validate contoso-v2/

# Run a single asset
bruin run contoso-v2/assets/contoso_v2_raw/dates.py

# Run the whole pipeline
bruin run contoso-v2/
```

## Prerequisites

- BigQuery connection `gcp-default` (`bruin-demo-data` project) configured in `~/.bruin.yml`.
