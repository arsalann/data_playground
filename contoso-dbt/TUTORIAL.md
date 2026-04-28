# Tutorial — adding a Bruin context layer to an existing dbt pipeline

This walks through what was built in `contoso-dbt/` and is meant as the
basis for a public tutorial. **The focus is the context layer**: how to use
Bruin to turn an already-running dbt pipeline + warehouse into something an
AI agent can navigate and query confidently.

The dbt half is summarized in one section because the audience is assumed
to already have a working dbt project and warehouse.

---

## What we built (end state)

```
contoso-dbt/
├── ingest/pipeline.py         # dlt — loads raw tables to BigQuery
├── models/                    # dbt — staging + reports
│   ├── sources.yml
│   ├── staging/stg_*.sql      (10)
│   └── reports/rpt_*.sql      (7)
├── context/                   # ← the AI-agent context layer (what this tutorial is about)
│   ├── .bruin.yml             # Isolated Bruin config (ADC auth)
│   ├── pipeline.yml
│   └── assets/
│       ├── contoso_dbt_raw/*.asset.yml      (23)
│       ├── contoso_dbt_staging/*.asset.yml  (10)
│       └── contoso_dbt_reports/*.asset.yml  (7)
├── AGENTS.md                  # Instructions for AI agents using this pipeline
├── generate_context.sh        # One-shot regenerator for context/
└── run_pipeline.sh            # dlt ingest + dbt build
```

The context layer is **40 YAMLs** (one per materialized table) with
descriptions, tags, column types, column docs, and quality checks — all
generated from the live warehouse, then enhanced by Claude.

---

## Step 1 — the dbt pipeline (assumed prerequisite)

Standard dbt-on-BigQuery setup, abbreviated:

- `dbt_project.yml` + `profiles.yml` (we used OAuth / gcloud ADC, location EU)
- `models/sources.yml` declares 23 raw tables (loaded by dlt to `contoso_dbt_raw`)
- 10 staging models (`stg_*.sql`) → `contoso_dbt_staging`
- 7 report models (`rpt_*.sql`) → `contoso_dbt_reports`
- A `generate_schema_name.sql` macro so `+schema: staging` lands as
  `contoso_dbt_staging` (not `contoso_dbt_contoso_dbt_staging`)
- `dbt build` to materialize everything

End of dbt prerequisites. The rest of the tutorial assumes those tables
exist in the warehouse.

---

## Step 2 — build the context layer with Bruin

### 2.1 — install Bruin

```bash
curl -LsSf https://getbruin.com/install/cli | sh
bruin --version
```

### 2.2 — write an *isolated* Bruin config

The repo may already have a `.bruin.yml` for other pipelines. Don't pollute
it. Write a scoped config inside the context directory so this work is
self-contained:

```yaml
# contoso-dbt/context/.bruin.yml
default_environment: default
environments:
  default:
    connections:
      google_cloud_platform:
        - name: contoso_dbt_bq
          project_id: bruin-playground-arsalan
          location: EU
          use_application_default_credentials: true   # uses gcloud ADC
```

> **Gotcha:** the field is `use_application_default_credentials`, not
> `use_default_credentials`. The latter silently fails.

> **Gotcha:** `bruin connections test` loads *every* connection in scope.
> If a sibling `.bruin.yml` references a missing keyfile, every command
> errors. Always pass `--config-file path/to/your/.bruin.yml` to scope it.

### 2.3 — write a `pipeline.yml`

The context layer is technically a Bruin pipeline (even though it never
runs transforms — it's documentation-only):

```yaml
# contoso-dbt/context/pipeline.yml
name: contoso_dbt_context
schedule: daily
start_date: "2016-01-01"
default_connections:
  google_cloud_platform: "contoso_dbt_bq"
```

### 2.4 — import the warehouse schemas as Bruin assets

This is the magic step. `bruin import database` introspects the warehouse
and writes one `<schema>.<table>.asset.yml` per table, prefilled with
column names + BigQuery types:

```bash
bruin import database \
  --config-file contoso-dbt/context/.bruin.yml \
  --connection contoso_dbt_bq \
  --schemas contoso_dbt_raw \
  --schemas contoso_dbt_staging \
  --schemas contoso_dbt_reports \
  contoso-dbt/context
```

After this you have skeleton YAMLs with no descriptions or tags yet — just
column types.

### 2.5 — filter out internal/bookkeeping tables

dlt writes its own `_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`
tables. They aren't useful as agent context, so drop them:

```bash
find contoso-dbt/context/assets -name "_dlt_*.asset.yml" -delete
```

(Adapt for your loader: Fivetran has `fivetran_*`, Airbyte has `_airbyte_*`,
etc.)

### 2.6 — AI-enhance the assets

```bash
bruin ai enhance --claude contoso-dbt/context/assets
```

For every asset, Bruin sends the column list + sample data to Claude and
fills in:
- a multi-paragraph **description** (purpose, grain, lineage, typical use)
- semantic **tags** (`domain:retail`, `layer:staging`, `sensitivity:pii`, …)
- per-column **descriptions** with business meaning
- **quality checks** (`not_null`, `unique`, `accepted_values`)

This takes minutes per asset; for 40 assets expect a long-running command.

> **Gotcha:** `ai enhance` ignores the `--config-file` flag and falls back
> to global config. You'll see "fill columns failed" warnings if your
> global config is broken. They're cosmetic — the import step already
> filled the columns; the enhance still completes correctly.

> **Gotcha:** `ai enhance` has been known to corrupt the YAML `columns:`
> block on rare assets. Always validate immediately after (next step).

### 2.7 — validate

```bash
bruin validate --config-file contoso-dbt/context/.bruin.yml contoso-dbt/context
```

Expected: `✓ Successfully validated 40 assets across 1 pipeline, all good.`
If any YAML is corrupt, regenerate just that one with
`bruin ai enhance --claude path/to/asset.yml`.

### 2.8 — wrap in a script (`generate_context.sh`)

Bundle 2.2–2.7 into one idempotent runner. Ours supports
`--skip-import` (re-enhance only) and `--skip-enhance` (fast structure
refresh). See `contoso-dbt/generate_context.sh` for the full version.

---

## Step 3 — give the agent an interface to the warehouse

The context layer is half the equation; the agent also needs to **query**
the warehouse. Bruin gives us two complementary surfaces.

### 3.1 — Bruin MCP (server) for tooling questions

The Bruin MCP server exposes `bruin_get_overview`, `bruin_get_docs_tree`,
`bruin_get_doc_content`. These let the agent learn *how Bruin itself
works* — asset types, connection config, command flags — without reading
external docs.

Register it once in the agent's MCP config; it's stateless.

### 3.2 — Bruin CLI for actual queries

For running SQL the agent uses the CLI directly:

```bash
bruin query \
  --config-file contoso-dbt/context/.bruin.yml \
  --connection contoso_dbt_bq \
  --query "SELECT category_name, SUM(revenue_usd) AS rev
           FROM \`bruin-playground-arsalan.contoso_dbt_reports.rpt_revenue_by_segment\`
           WHERE year = 2024 GROUP BY 1 ORDER BY rev DESC LIMIT 10"
```

Because the connection uses ADC, no keyfiles change hands — the agent
inherits the user's `gcloud auth application-default login` session.

### 3.3 — `AGENTS.md` as the agent entry point

Add a top-level `AGENTS.md` (or `CLAUDE.md`) telling the agent the
canonical workflow:

1. **Read** `context/assets/<schema>/<table>.asset.yml` before querying.
2. Use **Bruin MCP** for "how does Bruin do X?" questions.
3. Use **Bruin CLI** (`bruin query --config-file …`) for actual SQL.
4. Cite the asset(s) read in the answer.

See `contoso-dbt/AGENTS.md` for the full version, including parity-check
recipes and "things to avoid" (don't hand-edit generated YAMLs, don't mix
configs across pipelines, row counts in descriptions are snapshot-time
only).

---

## Lessons learned

- **Isolate your `.bruin.yml`.** A broken sibling connection breaks every
  command. Always `--config-file`-scope when working in a sub-pipeline.
- **ADC > keyfiles** for agent workflows. No secrets to rotate, no files
  to gitignore, and the agent runs as the human's identity.
- **Filter loader-internal tables before enhance** — otherwise Claude
  burns time describing `_dlt_pipeline_state`.
- **Always `bruin validate` after `ai enhance`.** Cheap insurance against
  rare YAML corruption.
- **Re-run `generate_context.sh` after schema changes.** Descriptions and
  row counts are snapshot-time; a column rename without regeneration leaves
  the agent quietly wrong.
- **Don't hand-edit generated YAMLs.** They're regenerable artifacts.
  Improve them upstream (in the dbt model + `schema.yml`) so the next
  import + enhance picks up the change.

---

## Adapting this to your own pipeline

The minimal recipe for an existing dbt + warehouse setup:

1. `bruin import database --schemas <yours...> ./context`
2. Delete loader-internal asset YAMLs.
3. `bruin ai enhance --claude ./context/assets`
4. `bruin validate ./context`
5. Add an `AGENTS.md` that points agents at `./context/assets/` and at
   `bruin query` for execution.

That's the whole context layer. Everything else in this directory
(`generate_context.sh`, the isolated `.bruin.yml`, parity scripts) is
ergonomics on top.
