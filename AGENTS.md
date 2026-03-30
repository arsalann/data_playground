# AGENTS.md

Utilize Bruin MCP and use Bruin CLI to run assets and query the tables. Reference Bruin docs.

This repository contains data pipelines built with **Bruin**, warehoused in **BigQuery**, with **Streamlit** dashboards visualized using **Altair**. Raw data ingestion is done in **Python**.

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
│   └── assets/
│       ├── raw/            # Ingestion layer (Python or SQL)
│       ├── staging/        # Transformation layer (SQL)
│       └── reports/        # Dashboards & analytical queries
└── ...
```

## Pipeline Structure

Every pipeline follows the same three-layer pattern. Use `berlin-weather/` as the reference implementation and `stackoverflow-trends/` for advanced patterns (multiple data sources, API ingestion, append + dedup).

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

### 5. `assets/reports/` — Reports Layer (Streamlit + Altair)

This layer contains Streamlit dashboards and their supporting SQL queries.

**Files**:
- `streamlit_app.py` — the main dashboard application.
- `<analysis_name>.sql` — standalone analytical queries loaded at runtime by the Streamlit app. These are plain SQL files (no Bruin header) that query the BigQuery staging tables directly using fully-qualified table names (`project.schema.table`).
- `requirements.txt` — dependencies for the dashboard.

**Dependencies** (`assets/reports/requirements.txt`):

```
streamlit
google-cloud-bigquery
db-dtypes
altair
pandas
```

**Streamlit App Conventions**:

```python
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="<Dashboard Title>", layout="wide")

PROJECT_ID = "bruin-playground-arsalan"
base_path = Path(__file__).parent


@st.cache_resource
def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def run_raw(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


def run_query(filename: str) -> pd.DataFrame:
    sql = (base_path / filename).read_text()
    return get_client().query(sql).to_dataframe()
```

- Use `st.secrets["gcp_service_account"]` for credentials (stored in `.streamlit/secrets.toml`, gitignored).
- Load main data with `run_raw()` for inline SQL, `run_query()` for SQL files in the same directory.
- Use `@st.cache_resource` for the BigQuery client.

**Altair Chart Conventions**:

- **All charts must be colorblind-friendly.** Use the Wong (2011) palette from *Nature Methods* (`#D55E00` vermillion, `#56B4E9` sky blue, `#E69F00` orange, `#009E73` bluish green, `#CC79A7` reddish purple, `#0072B2` blue, `#F0E442` yellow, `#999999` grey). Never rely on color alone to convey meaning — always pair color with a secondary visual channel (stroke dash pattern, marker shape, or direct labels). For heatmaps and diverging scales, use `blueorange` (not `redblue` or `redgreen`).
- Use a consistent color palette with a highlight color for the "current" or "focus" item: `HIGHLIGHT = "#D55E00"`, `DEFAULT = "#56B4E9"`.
- Use `alt.condition` to highlight a specific bar/point based on a boolean column (e.g. `is_current`).
- Always include tooltips with `alt.Tooltip` for interactivity.
- Set `height=340` as the default chart height (or `380` for taller charts).
- Use `cornerRadiusTopLeft=4, cornerRadiusTopRight=4` on `mark_bar()` for rounded bar tops.
- Overlay reference lines (e.g. historical average) using `mark_rule` + `mark_text` layered onto the main chart with `+`.
- Use `st.altair_chart(..., use_container_width=True)` to render.

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

## Testing & Development Workflow

Follow this order when building or modifying a pipeline:

1. **Validate first**: `bruin validate <pipeline-dir>/` — catches header/config errors before any execution.
2. **Test raw assets individually** with a small date range (2-3 days) or a limited scope (e.g. `STOCK_TICKER_LIMIT=5`).
3. **Verify data in BigQuery** after each raw asset — check row counts, date ranges, column types.
4. **Test staging SQL** once raw tables exist — run individually, then check output row counts and derived metrics.
5. **Test full pipeline** with a slightly larger window (e.g. 1 week) using `bruin run <pipeline-dir>/`.
6. **Run Streamlit** locally to verify the dashboard renders: `streamlit run <pipeline>/assets/reports/streamlit_app.py`.

For financial or quarterly data: the source API may only return recent quarters regardless of date range. Test with whatever the API actually provides rather than forcing specific dates.

## Creating a New Pipeline — Checklist

1. Create a new top-level directory named after the pipeline.
2. Add `pipeline.yml` with name, schedule, start_date, and default_connections.
3. Add a `README.md` documenting data sources, all assets, and run commands.
4. Create `assets/raw/` with Python ingestion scripts and a `requirements.txt`.
5. Create `assets/staging/` with SQL transformations that `depends` on the raw assets. Always deduplicate.
6. Create `assets/reports/` with a Streamlit app, supporting SQL files, and `requirements.txt`.
7. Validate with `bruin validate <pipeline-dir>/`.
8. Test each raw asset individually with a small subset of data.
9. Test staging assets once raw data exists.
10. Test the full pipeline end-to-end.

## Dependency Resolution

Bruin resolves Python dependencies by walking up the file tree from the asset to find the nearest `requirements.txt`. Keep a separate `requirements.txt` per layer (`raw/`, `reports/`) so dependencies stay isolated.

## Things to Avoid

- Do not put non-asset files inside `assets/`. Use separate directories for ad-hoc queries or analyses.
- Do not hardcode dates in SQL — use Bruin templating or `BRUIN_START_DATE`/`BRUIN_END_DATE` in Python.
- Do not use `.yml` extension for asset definitions — use `.asset.yml` if defining assets in YAML.
- Do not commit `.bruin.yml`, credentials, or `.streamlit/secrets.toml` — they are gitignored.
- Do not write `CREATE TABLE` or `INSERT` in SQL assets — let Bruin's `materialization` handle DDL.
- Do not give the asset file a different name than the asset name (file name and asset name must match — asset name is `<dataset>.<table_name>` which is `<parent_folder_name>.<asset_file_name>`).
- Do not use `create+replace` for large incremental data — use `append` and deduplicate in staging.
- Do not crash on API rate limits — return partial data and log a warning.
- Do not skip logging — every Python asset must have structured logging with progress output.
- Do not leave throwaway test scripts in the root directory or inside `assets/`.