# AGENTS.md

Utilize Bruin MCP and use Bruin CLI to run assets and query the tables. Reference Bruin docs.

This repository contains data pipelines built with **Bruin**, warehoused in **BigQuery**, with dashboards built using **Bruin DAC** (Dashboard-as-Code). Raw data ingestion is done in **Python**.

**Dashboard rule:** All new dashboards in this repo MUST be built with Bruin DAC. Streamlit is the legacy pattern — do not start new Streamlit dashboards. When modifying an existing Streamlit dashboard, consider migrating it to DAC if the change is non-trivial. See `DAC.md` for the working reference on DAC conventions, quirks, and our local fork features.

**Visualization rule:** Every chart and map in every dashboard MUST strictly follow `VISUALIZATIONS.md` at the repo root. That document is the single source of truth for chart structure, color/accessibility, truthful axes, encoding discipline, labels, layout, and framework-specific rules (DAC, MapLibre, Altair, Matplotlib polar). Violations are bugs.

## Project Discovery

Before proposing, scoping, or creating a new pipeline, read [`ideas.md`](ideas.md). It is the repository's source-of-truth catalog for data sources, project ideas, existing work, and known limitations. Use it to avoid duplicate work, identify viable source combinations, and select a project with an appropriate analytical angle. Follow its instructions to evaluate additional public datasets before committing to a new project.

## Repository Structure

```
data_playground/
├── .bruin.yml              # Root Bruin config (connections, credentials)
├── .gitignore
├── requirements.txt        # Root-level Python dependencies
├── AGENTS.md               # You are here
├── prompt.md               # Generic template for prompting new pipelines
├── credentials/            # Service account keys (gitignored)
├── <pipeline-name>/        # Each pipeline is a top-level directory
│   ├── pipeline.yml        # Pipeline config (schedule, connections)
│   ├── README.md           # Pipeline-specific docs (data sources, assets, run commands)
│   ├── assets/
│   │   ├── raw/            # Ingestion layer (Python or SQL)
│   │   ├── staging/        # Transformation layer (SQL)
│   │   └── report/         # Analytical/aggregation SQL consumed by DAC
│   └── dashboard-dac/      # Bruin DAC dashboard project
│       ├── dashboards/
│       │   ├── <name>.yml
│       │   └── queries/    # SQL files referenced by dashboard widgets
│       └── semantic/       # Optional semantic models
└── ...
```

## Pipeline Structure

Every pipeline follows the same three-layer pattern plus a DAC dashboard. Use `polymarket-weather/` as the reference implementation for DAC dashboards (`polymarket-weather/dashboard-dac/`). Use `berlin-weather/` for the simplest end-to-end pipeline shape, `stackoverflow-trends/` for advanced ingestion patterns (multiple data sources, API ingestion, append + dedup), and `baby-bust/` for comprehensive data validation and `bruin ai enhance`.

### 1. `pipeline.yml`

Defines the pipeline metadata and default connections.

```yaml
name: <pipeline-name>
schedule: daily
start_date: "2009-01-01"

default_connections:
  google_cloud_platform: "bruin-playground-arsalan"
```

- `name` must match the directory name.
- `schedule` can be `daily`, `hourly`, `weekly`, `monthly`, or a cron expression.
- `default_connections` sets which BigQuery project/connection assets use unless overridden.

### 2. `pipeline-name/README.md`

Every pipeline must have its own README covering:
- Data sources used (with links)
- All assets by layer (raw, staging, reports) with brief descriptions
- Key run commands (`bruin run`, `bruin validate`, `streamlit run`)
- Known limitations or data gaps

### 3. `assets/raw/` — Ingestion Layer (Python or SQL)

Raw assets fetch data from external sources and materialize it into BigQuery.

- **Python** (`type: python`) — for fetching from external APIs or files. Use an embedded Bruin YAML header in a docstring.
- **SQL** (`type: bq.sql`) — for querying existing BigQuery datasets (e.g. public datasets). Use an embedded Bruin YAML header in a SQL comment.

**File naming**: the file name (without extension) must match the table name in the asset's `name` field. For example, asset `raw.stackoverflow_api_monthly` lives in `stackoverflow_api_monthly.py`.

**Table naming**: `raw.<descriptive_name>` — all raw tables live in the `raw` schema.

**Template**:

```python
"""@bruin
name: raw.<table_name>
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Brief description of what this asset ingests.
  Include the data source URL and license if applicable.

materialization:
  type: table
  strategy: append

columns:
  - name: <column_name>
    type: <VARCHAR|INTEGER|DOUBLE|TIMESTAMP|BOOLEAN|DATE>
    description: <what this column contains, including units>
    primary_key: true  # only on the natural key

@bruin"""

import logging
import os
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_data(start_date: str, end_date: str) -> pd.DataFrame:
    # Fetch from API / file / etc.
    ...


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "<default>")
    end_date = os.environ.get("BRUIN_END_DATE", "<default>")

    logger.info("Interval: %s to %s", start_date, end_date)
    df = fetch_data(start_date, end_date)
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Fetched %d rows", len(df))
    return df
```

**Rules**:
- Always use `image: python:3.11`.
- Always include an `extracted_at` timestamp column (use `datetime.now(timezone.utc)`).
- Always include structured logging (`logging.basicConfig` + `logger = logging.getLogger(__name__)`).
- Document every column with a description including units where applicable.
- Mark exactly one column (or composite key) as `primary_key: true`.
- Use `BRUIN_START_DATE` and `BRUIN_END_DATE` environment variables for date-bounded fetches.
- Place a `requirements.txt` alongside the Python file with only the dependencies needed for that layer.

#### Materialization Strategy

| Strategy | Use when |
|---|---|
| `create+replace` | Small/immutable reference data (e.g. ticker lists, tag catalogs) |
| `append` | Large/incremental data (e.g. daily prices, hourly readings). **Always deduplicate in staging.** |
| `merge` | When you need upsert behavior on the raw table itself |
| `delete+insert` | When re-processing a date partition should replace old rows |

**Prefer `append` for most raw ingestion assets.** This allows safe re-runs and backfills without losing previously ingested data. Deduplication is handled in staging SQL using `ROW_NUMBER() ... ORDER BY extracted_at DESC`.

#### API Ingestion Best Practices

When fetching from external APIs:

- **Chunk large date ranges**: Break requests into 30-day (or smaller) windows to avoid timeouts and stay within API limits.
- **Retry with backoff**: Use exponential backoff for transient errors (429, 502, 503, timeouts). 5 retries is a good default.
- **Handle rate limits gracefully**: If you hit a rate limit mid-run, return whatever data was already fetched rather than crashing. Log a warning about the partial result.
- **Add delay between requests**: Use `time.sleep(0.5)` (or more) between API calls to avoid throttling.
- **Log progress**: Log each chunk/batch with counts so you can monitor long-running ingestions.
- **Environment variables for testing**: Use env vars like `STOCK_TICKER_LIMIT` to limit scope during development, so you don't need to fetch all 500+ tickers every test run.

#### Socrata Ingestion

Bruin supports Socrata through `type: ingestr` assets. Use this for Socrata-powered open-data portals before writing custom Python ingestion, unless you need API behavior that ingestr cannot express.

The local `.bruin.yml` may define these Socrata source connections:

| Connection | Domain | Example datasets |
|---|---|---|
| `socrata-nyc-open-data` | `data.cityofnewyork.us` | NYC 311 (`erm2-nwe9`), motor vehicle collisions (`h9gi-nx95`) |
| `socrata-chicago-open-data` | `data.cityofchicago.org` | Chicago crimes (`ijzp-q8t2`), CTA ridership (`6iiy-9s97`, `5neh-572f`) |
| `socrata-seattle-open-data` | `data.seattle.gov` | Building permits (`76t5-zqzr`), trade permits (`c87v-5hwh`) |
| `socrata-ny-health` | `health.data.ny.gov` | Hospital-acquired infections (`utrt-zdsi`), maternity information (`net3-iygw`) |
| `socrata-cdc` | `data.cdc.gov` | Nutrition/obesity BRFSS (`hn4x-zwk7`), policy/environment data (`k8w5-7ju6`) |

Socrata connections are domain-specific. For a new portal, add a new `socrata` entry to local `.bruin.yml` with `domain`, `app_token`, and, if needed, `username`/`password`. Never commit `.bruin.yml` or paste Socrata credentials into tracked files.

Use `.asset.yml` for Socrata ingestr assets:

```yaml
name: raw.<table_name>
type: ingestr
connection: bruin-playground-arsalan
description: |
  Ingests <dataset title> from <Socrata domain> dataset <dataset_id>.

parameters:
  source_connection: socrata-nyc-open-data
  source_table: "erm2-nwe9"
  destination: bigquery
  incremental_strategy: merge
  incremental_key: updated_at

columns:
  - name: id
    type: VARCHAR
    primary_key: true
    description: Socrata row identifier or stable natural key.
```

Only set `incremental_strategy` and `incremental_key` after verifying the dataset has a stable timestamp or monotonically increasing column. If no reliable incremental key exists, load with replace/default behavior and deduplicate downstream in staging.

#### Secrets

When an asset needs API credentials, declare them in the Bruin header under `secrets`. The keys must match the secret names in `.bruin.yml`:

```yaml
secrets:
  - key: epias_username
  - key: epias_password
```

Access them in Python via `os.environ["epias_username"]`.

**Dependencies** (`assets/raw/requirements.txt`):

```
pandas
requests
```

Add additional packages as needed (e.g. `yfinance`, `lxml`, `python-dateutil`).

### 4. `assets/staging/` — Transformation Layer (SQL)

Staging assets transform raw data into analysis-ready tables using BigQuery SQL. They reference upstream raw tables via `depends`.

**File naming**: `<entity>_<grain>.sql` — e.g. `weather_daily.sql`, `games_monthly.sql`.

**Table naming**: `staging.<descriptive_name>` — all staging tables live in the `staging` schema.

**Template**:

```sql
/* @bruin
name: staging.<table_name>
type: bq.sql
connection: bruin-playground-arsalan
description: |
  What this transformation does and why.
  Link to any reference material (e.g. code tables, standards).

depends:
  - raw.<upstream_table>

materialization:
  type: table
  strategy: create+replace

columns:
  - name: <column_name>
    type: <DATE|INTEGER|VARCHAR|DOUBLE|BOOLEAN>
    description: <description>
    primary_key: true
    nullable: false

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.<upstream_table>
    WHERE <primary_key> IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY <natural_key_columns> ORDER BY extracted_at DESC) = 1
)

SELECT
    ...
FROM deduped
ORDER BY <primary_key>
```

**Rules**:
- Always declare `depends` listing every upstream asset by its full `schema.table` name.
- **Always deduplicate raw data** using a `deduped` CTE with `ROW_NUMBER() OVER (PARTITION BY <natural_key> ORDER BY extracted_at DESC) = 1`. This is critical when raw assets use `append` strategy.
- Add derived columns: temporal dimensions (year, month, season, day_of_week), human-readable labels, boolean flags, unit conversions, derived ratios.
- Use `COALESCE` for nullable numeric fields to default to 0.
- Use `CASE` expressions for categorization and code-to-label mappings.
- Document every output column.
- SQL should be pure `SELECT` — let Bruin handle the DDL via `materialization`.
- Staging always uses `create+replace` strategy (it rebuilds from raw each time).

#### Multiple Data Sources

When the same entity comes from multiple sources (e.g. BigQuery public data + API supplement):
- Keep each source as a separate raw asset.
- In staging, `UNION ALL` the sources and deduplicate with `ROW_NUMBER()`, preferring the richer source for overlapping periods.
- Use `CAST(NULL AS INT64)` for columns that only exist in one source.

#### Common Staging Patterns

- **Unpivot**: Turn wide-format raw tables (one column per category) into long format using `UNION ALL` with `source_name` / `category` columns. E.g. energy sources, financial statement line items.
- **Enrichment joins**: Join with reference/dimension tables (e.g. `raw.stock_tickers` for sector/industry).
- **Window functions**: Moving averages (`AVG ... ROWS BETWEEN N PRECEDING`), rolling highs/lows, daily returns, period-over-period growth.
- **Ratio derivation**: Margins, ROE, debt-to-equity, share percentages — compute in staging, not in reports.

### 5. `dashboard-dac/` — Reports Layer (Bruin DAC)

**All dashboards in this repo are built with Bruin DAC.** DAC is "Dashboard-as-Code" — you write a YAML (or `.dashboard.tsx`) file plus `.sql` files, and DAC validates them and serves a React/Recharts frontend. Queries run through `bruin query` against whatever Bruin connection you specify (BigQuery for us).

See `DAC.md` at the repo root for the living working reference: install steps, CLI cheat sheet, quirks, fork-only features (`yLabel`, `yRight`, `yRightLabel`, `seriesNames`, `hideName`), and conventions. Read `DAC.md` *first* before re-fetching upstream docs — it captures every wall we've already hit.

**The `create-dashboard` skill** (located at `.claude/skills/create-dashboard/`) is the authoritative guide for DAC YAML/TSX syntax: project layout, widget types, filters, semantic models, and SQL conventions. Invoke it (or read its `SKILL.md`) whenever you build or modify a dashboard.

#### Project layout

Each pipeline gets its own DAC project at `<pipeline>/dashboard-dac/`:

```
<pipeline>/dashboard-dac/
├── dashboards/
│   ├── <dashboard-name>.yml        # one file per dashboard
│   └── queries/
│       ├── <widget_name>.sql       # SQL referenced by `file:` widgets
│       └── ...
└── semantic/                       # optional, only if using semantic models
```

DAC discovers `.bruin.yml` by walking *up* the directory tree from `--dir`, so the repo-root `.bruin.yml` is picked up automatically. Do not duplicate it per project.

Reference implementation: `polymarket-weather/dashboard-dac/`.

#### Required CLI workflow

```bash
# Validate schema and references (run after every edit to YAML)
dac validate --dir <pipeline>/dashboard-dac

# Validate + execute every query end-to-end (run before declaring a dashboard done)
dac check --dir <pipeline>/dashboard-dac

# Live-reload dev server with SSE
dac serve --dir <pipeline>/dashboard-dac --port 8321
# → open http://localhost:8321

# Debug a single widget's query
dac query --dir <pipeline>/dashboard-dac --dashboard "<Dashboard Name>" --widget "<Widget Name>"

# List discovered dashboards
dac ls --dir <pipeline>/dashboard-dac
```

When starting `dac serve`, always tell the user the localhost URL. Default port we use in this repo is `8321`. `--debug` enables verbose logs; `--environment NAME` switches `.bruin.yml` environments.

#### Dashboard YAML — minimum viable example

```yaml
name: My Dashboard                           # required
description: One-sentence intent.
connection: bruin-playground-arsalan         # matches .bruin.yml
theme: ibm-cb-dark                           # see DAC.md for themes
refresh: { interval: "5m" }                  # optional

filters:                                     # optional
  - name: date_range
    type: date-range
    default: last_30_days

rows:                                        # 12-column grid; sum of `col` per row ≤ 12
  - widgets:
      - name: Revenue                        # required, minLength: 1
        type: metric
        sql: |
          SELECT SUM(amount) AS value
          FROM `project.staging.sales`
        column: value
        prefix: "$"
        format: number
        col: 3
```

Widget query sources (mutually exclusive): `query:` (named in top-level `queries:`), `sql:` (inline), `file:` (relative path to a `.sql` file), `metric:` (semantic), or direct semantic fields (`dimension`, `metrics`, ...).

Widget types: `metric, chart, table, text, divider, image`.

Chart types: `line, bar, area, pie, scatter, bubble, combo, histogram, boxplot, funnel, sankey, heatmap, calendar, sparkline, waterfall, xmr, dumbbell`.

#### Hard-won quirks (read these before debugging)

The full list lives in `DAC.md` under "Hard-won quirks". The high-impact ones:

- **Legends only render on some chart types.** `line`, `bar` (unstacked), `area`, scatter, bubble, heatmap render **no legend**. `pie`, `funnel`, `combo`, `calendar` do. To get a legend on a multi-series chart, either use `chart: combo` or rely on the local fork's `line` legend (see below).
- **No native dual y-axis.** Upstream DAC instantiates one `<YAxis />` per chart. For `chart: line` we maintain a local fork at `.context/dac-fork/` that adds `yLabel`, `yRight`, `yRightLabel`, `seriesNames`, and `hideName` — see `DAC.md` § 10. For non-line charts, scale one series in SQL onto the other's range and explain it in the footnote text widget.
- **Column names must be plain identifiers.** `bruin query` rejects spaces, parens, dashes, accents. Use `snake_case` in SQL output, then map to display names via `seriesNames:` on the widget (line-chart fork only).
- **ISO-timestamp x-axes get auto-stripped to `Apr 6`-style labels.** To preserve hour-of-day, emit a non-ISO STRING in SQL: `FORMAT_TIMESTAMP('%H:%M', ts_local_paris) AS time_label`.
- **Themes are color-only.** Font sizes, paddings, widget-title strip ("name" rendered above the chart) are hardcoded in Tailwind. The fork's `hideName: true` suppresses the title strip; otherwise it's always there.
- **Text widget Markdown is plain `react-markdown`.** No raw HTML, no GFM. Maximum heading size from YAML is `# h1` (~19.5px).

#### Data Visualization Standards

**All visualization standards live in `VISUALIZATIONS.md` at the repo root. Every chart in every dashboard MUST strictly follow that document.** It is the single source of truth for chart structure, color and accessibility, truthful representation, encoding discipline, label readability, annotation, layout, and framework-specific rules (DAC, Altair, Matplotlib polar). Treat any violation as a bug.

Highlights you cannot skip (full text in `VISUALIZATIONS.md`):
- **Per-chart 3-row structure**: header text widget → chart widget (`hideName: true`) → footnote text widget, each at `col: 12`. Title = what the chart is; description = insight with magnitude; footnote = sources + tools + limitations.
- **Wong (2011) colorblind palette** only, paired with a second channel (stroke-dash, shape, position, or encoding-key text). Never rely on color alone.
- **Zero baseline** on bar/area charts. **No pie, no 3D, no dual y-axes** (sole exception: local-fork `yRight` on `chart: line`).
- **Snake_case SQL columns**, map to display labels via `seriesNames:` (line-chart fork) or the header encoding-key line.
- Every dashboard ends with a **Methodology** text widget.

## Bruin Asset Metadata Reference

All Bruin metadata lives inside a comment block at the top of the file:
- **Python**: `"""@bruin ... @bruin"""`
- **SQL**: `/* @bruin ... @bruin */`

### Key Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | `schema.table_name` — determines the destination table |
| `type` | Yes | `python` for Python assets, `bq.sql` for BigQuery SQL |
| `image` | Python only | Python Docker image, use `python:3.11` |
| `connection` | Yes | BigQuery connection name from `.bruin.yml` |
| `description` | Yes | Multi-line description of what the asset does |
| `depends` | SQL only | List of upstream asset names (`schema.table`) |
| `materialization.type` | Yes | `table` or `view` |
| `materialization.strategy` | Yes | `create+replace`, `merge`, `delete+insert`, `append`, etc. |
| `columns` | Yes | Full column definitions with `name`, `type`, `description` |

### Column Types

Use these types for column definitions: `VARCHAR`, `INTEGER`, `DOUBLE`, `BOOLEAN`, `DATE`, `TIMESTAMP`.

## Bruin CLI Quick Reference

```bash
bruin run <path/to/asset>                                    # Run a single asset
bruin run <path/to/pipeline/>                                # Run entire pipeline
bruin run --downstream <path>                                # Run asset + all downstream
bruin run --start-date 2024-07-01 --end-date 2024-12-31 <path>  # Run with date range
bruin validate <path>                                        # Validate asset/pipeline definitions
bruin format <path>                                          # Auto-format asset files
bruin lineage <path>                                         # Show asset dependency graph
bruin connections list                                       # List configured connections
bruin connections ping <name>                                # Test a connection
```

- Always run individual assets during development, not entire pipelines.
- Use `--start-date` / `--end-date` for backfilling specific date ranges without re-ingesting everything.
- Use `--downstream` when you want to run a raw asset and automatically rebuild its staging dependents.

## DAC CLI Quick Reference

The `dac` binary lives on `$PATH` at `~/.local/bin/dac`. At the start of every DAC session, upgrade to the latest release and refresh the `create-dashboard` skill — DAC ships breaking schema changes frequently, and the skill ships alongside the CLI:

```bash
dac upgrade                                              # pulls the latest stable dac into ~/.local/bin
dac skills install create-dashboard --force              # refreshes .claude/skills/create-dashboard/SKILL.md
dac version                                              # confirm
```

Run both commands before touching any dashboard YAML/TSX. If `dac validate` rejects properties that used to work (`hideName`, `yLabel`, `seriesNames`, widget-level `height`, etc.), you are on a stale schema — re-run the two install commands above and consult the updated `SKILL.md`.

```bash
dac validate --dir <pipeline>/dashboard-dac              # Schema + reference checks (fast)
dac check    --dir <pipeline>/dashboard-dac              # Validate + execute every query
dac query    --dir <pipeline>/dashboard-dac \
             --dashboard "<Name>" --widget "<Widget>"    # Debug one widget's SQL
dac serve    --dir <pipeline>/dashboard-dac --port 8321  # Dev server → http://localhost:8321
dac ls       --dir <pipeline>/dashboard-dac              # List discovered dashboards
dac connections                                          # Ping every connection
```

- Always run `dac validate` after editing YAML; run `dac check` before declaring a dashboard done.
- Use `--debug` for verbose logs, `--environment NAME` to switch environments.
- When starting `dac serve`, surface the localhost URL to the user (default `http://localhost:8321`).
- Cache invalidates on file change; query results are cached 5 minutes.

## Testing & Development Workflow

Follow this order when building or modifying a pipeline:

1. **Validate first**: `bruin validate <pipeline-dir>/` — catches header/config errors before any execution.
2. **Test raw assets individually** with a small date range (2-3 days) or a limited scope (e.g. `STOCK_TICKER_LIMIT=5`).
3. **Verify data in BigQuery** after each raw asset — check row counts, date ranges, column types.
4. **Test staging SQL** once raw tables exist — run individually, then check output row counts and derived metrics.
5. **Test full pipeline** with a slightly larger window (e.g. 1 week) using `bruin run <pipeline-dir>/`.
6. **Build the DAC dashboard**:
   - `dac validate --dir <pipeline>/dashboard-dac` after every YAML edit.
   - `dac check --dir <pipeline>/dashboard-dac` once you think the dashboard is complete (runs every widget query end-to-end).
   - `dac serve --dir <pipeline>/dashboard-dac --port 8321` and verify in browser at `http://localhost:8321`. Hand the user the URL when you start the server — they review rendered output themselves.

For financial or quarterly data: the source API may only return recent quarters regardless of date range. Test with whatever the API actually provides rather than forcing specific dates.

## Creating a New Pipeline — Checklist

1. Create a new top-level directory named after the pipeline.
2. Add `pipeline.yml` with name, schedule, start_date, and default_connections.
3. Add a `README.md` documenting data sources, all assets, and run commands (including the `dac serve` command and localhost URL).
4. Create `assets/raw/` with Python ingestion scripts and a `requirements.txt`.
5. Create `assets/staging/` with SQL transformations that `depends` on the raw assets. Always deduplicate.
6. Create `assets/report/` SQL aggregations consumed by the dashboard (optional — many dashboards can read directly from staging).
7. Create `dashboard-dac/dashboards/<name>.yml` plus any `dashboard-dac/dashboards/queries/*.sql`. Reference the `create-dashboard` skill (in `.claude/skills/create-dashboard/SKILL.md`) for widget syntax. Reference `DAC.md` for repo-specific conventions and quirks.
8. Validate with `bruin validate <pipeline-dir>/` and `dac validate --dir <pipeline>/dashboard-dac`.
9. Test each raw asset individually with a small subset of data.
10. Test staging assets once raw data exists.
11. Test the full pipeline end-to-end.
12. Run `dac check` and then `dac serve` to verify the dashboard renders correctly.

## Dependency Resolution

Bruin resolves Python dependencies by walking up the file tree from the asset to find the nearest `requirements.txt`. Keep a separate `requirements.txt` for the `raw/` layer so dependencies stay isolated. DAC dashboards have no Python dependencies — they run entirely through `dac` + `bruin query`.

## Secrets Management

**No secrets, credentials, API keys, tokens, or passwords may ever be committed to this repository.** This is a hard rule with no exceptions.

### What is gitignored

The following are excluded via `.gitignore` and must never be committed:
- `.bruin.yml` — contains connection credentials (API keys, passwords, service account paths)
- `**/secrets.toml` — legacy Streamlit secrets files (DAC dashboards do not use this — see below)
- `credentials/` — service account JSON files
- `*.pem`, `*.key`, `*.p12`, `*.pfx` — private key files
- `.env`, `.env.*` — environment variable files

### Rules

- **Never hardcode secrets in Python or SQL.** Use `os.environ["KEY_NAME"]` in Python and Bruin `secrets:` declarations in the asset YAML header.
- **Never commit `.bruin.yml`** — this file contains all connection credentials. It stays local.
- **Never create service account JSON files outside `credentials/`** — that directory is gitignored.
- **If you accidentally commit a secret, the credential must be rotated immediately.** Removing the file from git tracking does not remove it from history. Use `git-filter-repo` to rewrite history and force-push.

### DAC credentials

DAC dashboards do NOT need their own secrets file. They query through `bruin query`, which uses the connection defined in the repo-root `.bruin.yml`. For BigQuery, that connection uses Application Default Credentials (`gcloud auth application-default login`) — there is nothing to copy or template per dashboard. If `dac connections` shows the `google_cloud_platform` row as healthy, the dashboard can query BigQuery.

### Legacy: Streamlit secrets (existing dashboards only)

Pre-DAC pipelines have a `.streamlit/secrets.toml` in their reports directory. That file is gitignored globally via `**/secrets.toml`. Do not create new ones — all new dashboards use DAC. When migrating an existing Streamlit dashboard to DAC, you can delete the `.streamlit/` directory in the same PR.

## Things to Avoid

- Do not put non-asset files inside `assets/`. Use separate directories for ad-hoc queries or analyses.
- Do not hardcode dates in SQL — use Bruin templating or `BRUIN_START_DATE`/`BRUIN_END_DATE` in Python.
- Do not use `.yml` extension for asset definitions — use `.asset.yml` if defining assets in YAML.
- **Do not commit `.bruin.yml`, credentials, `secrets.toml`, `.env`, private keys, or any file containing secrets** — they are all gitignored. If `git status` shows any of these files as untracked or modified, do not stage them.
- Do not write `CREATE TABLE` or `INSERT` in SQL assets — let Bruin's `materialization` handle DDL.
- Do not give the asset file a different name than the asset name (file name and asset name must match — asset name is `<dataset>.<table_name>` which is `<parent_folder_name>.<asset_file_name>`).
- Do not use `create+replace` for large incremental data — use `append` and deduplicate in staging.
- Do not crash on API rate limits — return partial data and log a warning.
- Do not skip logging — every Python asset must have structured logging with progress output.
- Do not leave throwaway test scripts in the root directory or inside `assets/`.
- When running `bruin run <pipeline>/` on pipelines with `append` raw assets, the default date interval is today — which may return no data for historical APIs. Run raw assets explicitly with `--start-date`/`--end-date` for initial loads, then run staging assets separately.
- For flaky APIs (e.g., World Bank), use chunked requests (10-year windows) with high `per_page` values and retry logic with exponential backoff. Single large requests are more likely to timeout.
- When pivoting long-to-wide in staging (e.g., indicator rows → columns), validate every pivoted column against raw by joining on natural key and checking for zero diff. This catches silent data loss from incorrect indicator codes or join fanout.
- After running `bruin ai enhance`, always re-run `bruin validate` and `bruin run` on the affected assets to verify the enhanced metadata doesn't break anything. The enhance command adds quality checks (not_null, accepted_values, min/max) that may fail if the data has edge cases. **Never do bulk regex edits on YAML column definitions** — if `bruin ai enhance` corrupts the YAML, rewrite the section manually.
- **Do not start a new Streamlit dashboard.** All new dashboards must be built with Bruin DAC. If a request seems to imply Streamlit (e.g. "add an interactive widget that takes user input and runs Python"), check whether DAC's `filters:` system covers the use case before reaching for Streamlit. Streamlit dashboards already in the repo are legacy — leave them be unless you're migrating.
- **Do not run Streamlit alongside DAC for the same pipeline.** Pick one. New work → DAC.
- **Do not invent DAC features that don't exist.** The widget schema is in `DAC.md`; if a property isn't in the "All widget properties" list there or in the fork-only fields, it will be ignored or fail validation. When in doubt, check `DAC.md` first and only then re-fetch upstream.
- **Do not use SQL column names with spaces, parens, dashes, or accents in DAC widgets.** `bruin query` rejects them. Use `snake_case` in SQL output; map to display labels via `seriesNames:` (line-chart fork) or via the encoding-key line in the header text widget.
- **Do not push ISO timestamps into a DAC chart x-axis if you want sub-day labels.** The formatter strips the time. Emit a STRING column (e.g. `FORMAT_TIMESTAMP('%H:%M', ts)`) and order rows in SQL.
- **Do not hoard front-end test screenshots.** Screenshots produced by Playwright (or any other tool) for visually verifying dashboard / front-end changes are working artifacts, not deliverables. Clear the `screenshots/` directory once the changes they verified are landed — delete the `.png` files and any one-off `_*.mjs` capture scripts written for that session. Keep only screenshots explicitly requested by the user for documentation. `**/screenshots/` is gitignored repo-wide; if a stray screenshot folder is tracked, delete it. Never commit dozens of screenshots from a single review pass.

## Geospatial Data Rules

These rules are **mandatory** when working with geospatial data (OSMnx, GeoPandas, GHSL, etc.):

- **Prefer MapLibre for map visualizations.** Use [MapLibre](https://maplibre.org/) as the default tool for interactive maps. Use another mapping library only where MapLibre cannot meet a documented technical requirement. New dashboards must still follow the DAC dashboard rule; do not introduce Streamlit for a map.
- **Consistent spatial methodology.** When comparing cities or regions, every entity MUST use identical spatial parameters: same query function, same radius, same resolution, same projection. Never mix `graph_from_place` (admin boundaries) across cities — admin boundary sizes vary wildly (e.g., "City of London" = 1 sq mi vs "Chicago" = 234 sq mi). Use `graph_from_point(center, dist=RADIUS)` with a fixed radius for all cities.
- **Verify query scope before charting.** Before building any visualization, verify what each geospatial query actually returned. Log the bounding box or area. Compare areas across all entities to catch inconsistencies. A query for "Barcelona" might return the city, the province, or a single neighborhood depending on the API and query string.
- **Document the methodology explicitly.** State the exact spatial parameters (radius, center coordinates, projection, data version) in the README and in the dashboard's methodology section. Future users must be able to reproduce the analysis.
- **GHSL GeoPackage handling.** The GHSL R2024A release contains 16 thematic layers in a single GeoPackage that must be joined on `ID_UC_G0`. It uses Mollweide projection — convert centroids to WGS84 (EPSG:4326) for lat/lon coordinates. Use `pycountry` with fuzzy matching + manual overrides for country name → ISO code mapping.
- **OSMnx Overpass API.** Set `ox.settings.timeout = 300` for large network downloads. Add `time.sleep(2)` between city downloads to respect rate limits. Use `CITY_LIMIT` env var for testing. Full-city queries for megacities (Tokyo, Istanbul) can exceed 2,000 km² and hang — always use bounded queries.
- **Name matching across datasets.** When joining datasets from different sources (e.g., GHSL city names to OSMnx queries), use proximity matching (lat/lon within threshold + country code match) rather than string matching. City names vary across datasets (e.g., "Mumbai" vs "Bombay", "Brasília" vs "Lago Norte"). Filter out known mismatches from all charts.

## Data Analysis Process

This is the process for building analysis-driven dashboards. Follow this order:

### Phase 1: Data inventory
Before writing any chart code, list every field available across all data sources. Understand the data range, granularity, null rates, and distributions. Query the staging tables directly to verify what you actually have.

### Phase 2: Find non-obvious insights
A dashboard that just shows "here's the data" is not analysis. Look for:
- **Correlations** — do two variables move together? (e.g., GDP vs grid-ness, temperature vs street length)
- **Outliers** — which cities/countries break the pattern? Why?
- **Derived metrics** — intersection density (intersections / area), orientation order (entropy-derived), population growth rates
- **Cross-domain joins** — combine datasets that weren't designed to go together (GHSL urban data + OSMnx street analysis + World Bank economic indicators)

### Phase 3: Build charts iteratively
Start with the most insightful chart. Show it to the user. Get feedback. Iterate. Each chart must answer a specific question — if the takeaway is "the data exists," cut the chart. Expect to:
- Remove boring or redundant charts
- Add new charts based on what the data reveals
- Filter aggressively (e.g., cities over 5M population only, exclude data mismatches)
- Adjust visual encoding (dot sizes, label sizes, axis scales) based on what makes the data readable

### Phase 4: Validate everything
- Cross-check data claims against the actual data
- Verify join quality (are matched cities actually correct?)
- Filter out known data quality issues (e.g., GHSL proximity mismatches)
- Include units on all metrics
- Include methodology section with data source links and limitations

## Visualization Framework Rules

MapLibre map-visualization guidance, Altair gotchas (legacy Streamlit only), Matplotlib polar-plot orientation, DAC chart structure / palette / encoding, and any other framework-specific viz rules all live in `VISUALIZATIONS.md`. Read that file before writing any chart or map code.
