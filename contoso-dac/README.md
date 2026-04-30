# Contoso Multi-Department Data Pipeline

Simulates a realistic consumer electronics retailer with data across 7 business units: Sales, HR, Finance, Marketing, Engineering, Operations, and Customer Support.

## Data Sources

- **Contoso V2 (SQLBI)**: 8 tables of retail sales data (1M orders scale) from the [Contoso Data Generator V2](https://github.com/sql-bi/Contoso-Data-Generator-V2-Data). MIT license.
- **Synthetic**: 15 additional tables generated with Python Faker, linked to Contoso's real keys for cross-department joins.

## Assets

### Raw (23 tables)

| Department | Table | Rows | Source |
|---|---|---|---|
| Sales | `contoso_raw.sales` | ~2.3M | Contoso V2 |
| Sales | `contoso_raw.orders` | ~980K | Contoso V2 |
| Sales | `contoso_raw.order_rows` | ~2.3M | Contoso V2 |
| Sales | `contoso_raw.customers` | ~105K | Contoso V2 |
| Sales | `contoso_raw.products` | ~2.5K | Contoso V2 |
| Sales | `contoso_raw.stores` | 74 | Contoso V2 |
| Sales | `contoso_raw.dates` | ~4K | Contoso V2 |
| Sales | `contoso_raw.currency_exchange` | ~100K | Contoso V2 |
| HR | `contoso_raw.departments` | 12 | Synthetic |
| HR | `contoso_raw.employees` | ~3K | Synthetic |
| HR | `contoso_raw.payroll` | ~108K | Synthetic |
| HR | `contoso_raw.job_postings` | ~800 | Synthetic |
| Finance | `contoso_raw.gl_journal_entries` | ~500K | Synthetic |
| Finance | `contoso_raw.budgets` | ~2.2K | Synthetic |
| Finance | `contoso_raw.accounts_payable` | ~12K | Synthetic |
| Marketing | `contoso_raw.campaigns` | ~200 | Synthetic |
| Marketing | `contoso_raw.ad_spend_daily` | ~50K | Synthetic |
| Marketing | `contoso_raw.campaign_attribution` | ~400K | Synthetic |
| Engineering | `contoso_raw.sprint_tickets` | ~8K | Synthetic |
| Engineering | `contoso_raw.deployments` | ~1.2K | Synthetic |
| Operations | `contoso_raw.inventory_snapshots` | ~300K | Synthetic |
| Operations | `contoso_raw.shipments` | ~980K | Synthetic |
| Support | `contoso_raw.support_tickets` | ~150K | Synthetic |

### Staging (10 models)

| Model | Description |
|---|---|
| `contoso_staging.sales_fact` | Denormalized sales with customer/product/store dims, USD amounts |
| `contoso_staging.employee_directory` | Enriched employees with department, store, tenure, manager |
| `contoso_staging.financial_summary_monthly` | GL actuals vs budget by department and account |
| `contoso_staging.marketing_performance` | Campaign ROAS, CPA, attributed revenue |
| `contoso_staging.support_metrics_monthly` | Monthly ticket volume, resolution time, CSAT |
| `contoso_staging.inventory_health` | Latest stock levels with stockout risk classification |
| `contoso_staging.shipping_performance` | Delivery metrics by carrier |
| `contoso_staging.engineering_velocity` | Sprint story points, cycle time, deployment frequency |
| `contoso_staging.payroll_summary_monthly` | Headcount and payroll cost by department |
| `contoso_staging.executive_kpis_monthly` | Cross-department monthly rollup |

### Reports (7 models)

| Model | Description |
|---|---|
| `contoso_reports.revenue_by_segment` | Monthly revenue by country and category with MoM/YoY growth |
| `contoso_reports.profit_and_loss` | Monthly P&L statement (Revenue, COGS, Gross Profit, OpEx, Operating Income) |
| `contoso_reports.marketing_channel_roi` | Quarterly channel-level funnel metrics, ROAS, and spend efficiency |
| `contoso_reports.workforce_analytics` | Quarterly headcount, turnover, tenure, and payroll cost by department |
| `contoso_reports.delivery_sla` | Monthly carrier SLA with on-time rates and P95 lead times |
| `contoso_reports.customer_support_report` | Monthly support metrics by channel and category with agent utilization |
| `contoso_reports.engineering_report` | Monthly velocity, bug ratio, deployment frequency, and rollback rates |

## Run Commands

```bash
# Validate pipeline
bruin validate contoso/

# Run individual raw asset
bruin run contoso/assets/raw/dates.py

# Run all raw assets
bruin run contoso/assets/raw/

# Run full pipeline (raw + staging)
bruin run contoso/
```

## Prerequisites

- `7z` (p7zip) installed for extracting Contoso V2 Parquet archives
- BigQuery connection `bruin-playground-arsalan` configured in `.bruin.yml`
