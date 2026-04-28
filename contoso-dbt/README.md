# contoso-dbt

A dbt + dlt port of the original Bruin `contoso/` pipeline. Runs side-by-side with the bruin version so the two can be compared.

- **Ingestion**: dlt (Python) — loads 23 raw tables into BigQuery
- **Transform**: dbt — 10 staging models + 7 report models
- **Destination**: BigQuery project `bruin-playground-arsalan`, datasets `contoso_dbt_raw / contoso_dbt_staging / contoso_dbt_reports`

## Layout

```
contoso-dbt/
├── ingest/               # dlt ingestion (reuses contoso/assets/contoso_raw/*.py via importlib)
│   └── pipeline.py
├── models/
│   ├── sources.yml       # 23 raw tables under source('contoso_raw', ...)
│   ├── staging/          # 10 models → contoso_dbt_staging
│   └── reports/          #  7 models → contoso_dbt_reports
├── macros/
│   └── generate_schema_name.sql
├── context/              # AI-agent context layer (generated) — bruin-style asset YAMLs
├── run_pipeline.sh       # dlt ingest + dbt build (end-to-end)
├── generate_context.sh   # bruin import + ai enhance → context/
├── dbt_project.yml
├── profiles.yml
├── packages.yml
└── ingest/requirements.txt
```

## Setup

Auth uses Google Application Default Credentials (`gcloud auth application-default login`) by default. Set `GOOGLE_APPLICATION_CREDENTIALS` if you prefer a service-account keyfile.

Everything else (venv, deps, dbt packages) is handled by `run_pipeline.sh` on first run.

## Run

All commands assume you're at the repo root.

```bash
# Full pipeline: dlt ingest -> dbt deps -> dbt build (+ tests)
./contoso-dbt/run_pipeline.sh

# Skip flags:
./contoso-dbt/run_pipeline.sh --skip-ingest    # dbt only
./contoso-dbt/run_pipeline.sh --skip-build     # dlt only
```

Raw manual invocations (equivalent):

```bash
python contoso-dbt/ingest/pipeline.py
dbt deps --project-dir contoso-dbt --profiles-dir contoso-dbt
dbt build --project-dir contoso-dbt --profiles-dir contoso-dbt
```

## AI-agent context layer

`generate_context.sh` uses bruin (`import database` → `ai enhance` → `validate`) to produce `context/` — a documentation-only bruin pipeline where every dbt-materialized table has a SQL + YAML asset with AI-enhanced descriptions, column docs, and tags. Point AI agents at this directory for analysis / docs / query generation.

```bash
./contoso-dbt/generate_context.sh                    # full run (claude-enhanced)
./contoso-dbt/generate_context.sh --skip-enhance     # import only (no AI)
./contoso-dbt/generate_context.sh --skip-import      # re-enhance existing
```

The script writes an isolated `context/.bruin.yml` so it doesn't depend on (or modify) the repo-root bruin config. `ai enhance` can corrupt YAML columns — a `bruin validate` runs at the end to catch that.

## Parity with bruin

| Layer | bruin | dbt |
|---|---|---|
| Raw | `contoso_raw.*` (23 tables) | `contoso_dbt_raw.*` |
| Staging | `contoso_staging.*` (10) | `contoso_dbt_staging.stg_*` |
| Reports | `contoso_reports.*` (7) | `contoso_dbt_reports.rpt_*` |

Spot-check parity with queries like:

```sql
SELECT SUM(revenue_usd) FROM `bruin-playground-arsalan.contoso_staging.sales_fact`;
SELECT SUM(revenue_usd) FROM `bruin-playground-arsalan.contoso_dbt_staging.stg_sales_fact`;
```
