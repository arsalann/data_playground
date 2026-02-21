# AGENTS.md

This repository contains data pipelines built with **Bruin**, warehoused in **BigQuery**, with **Streamlit** dashboards visualized using **Altair**. Raw data ingestion is done in **Python**.

## Repository Structure

```
data_playground/
├── .bruin.yml              # Root Bruin config (connections, credentials)
├── .gitignore
├── requirements.txt        # Root-level Python dependencies
├── AGENTS.md               # You are here
├── credentials/            # Service account keys (gitignored)
├── <pipeline-name>/        # Each pipeline is a top-level directory
│   ├── pipeline.yml        # Pipeline config (schedule, connections)
│   └── assets/
│       ├── raw/            # Ingestion layer (Python)
│       ├── staging/        # Transformation layer (SQL)
│       └── reports/        # Dashboards & analytical queries
└── ...
```

## Pipeline Structure

Every pipeline follows the same three-layer pattern. Use `berlin-weather/` as the reference implementation.

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

### 2. `assets/raw/` — Ingestion Layer (Python)

Raw assets fetch data from external sources and materialize it into BigQuery. They are Python scripts with an embedded Bruin YAML header.

**File naming**: `<source_name>_raw.py` — always suffix with `_raw`.

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
  strategy: create+replace

columns:
  - name: <column_name>
    type: <VARCHAR|INTEGER|DOUBLE|TIMESTAMP|BOOLEAN|DATE>
    description: <what this column contains, including units>
    primary_key: true  # only on the natural key

@bruin"""

import pandas as pd
import requests
import os
from datetime import datetime


def fetch_data(start_date: str, end_date: str) -> pd.DataFrame:
    # Fetch from API / file / etc.
    ...


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "<default>")
    end_date = os.environ.get("BRUIN_END_DATE", "<default>")

    df = fetch_data(start_date, end_date)
    df["extracted_at"] = datetime.now()

    return df
```

**Rules**:
- Always use `image: python:3.11`.
- Always include an `extracted_at` timestamp column.
- Use `create+replace` strategy for small/immutable datasets; use `merge` or `delete+insert` for large/incremental ones.
- Document every column with a description including units where applicable.
- Mark exactly one column as `primary_key: true`.
- Use `BRUIN_START_DATE` and `BRUIN_END_DATE` environment variables for date-bounded fetches.
- Place a `requirements.txt` alongside the Python file with only the dependencies needed for that layer.

**Dependencies** (`assets/raw/requirements.txt`):

```
pandas
requests
```

### 3. `assets/staging/` — Transformation Layer (SQL)

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

SELECT
    ...
FROM raw.<upstream_table>
WHERE <filter out nulls/bad data>
ORDER BY <primary_key>
```

**Rules**:
- Always declare `depends` listing every upstream asset by its full `schema.table` name.
- Add derived columns: temporal dimensions (year, month, season, day_of_week), human-readable labels, boolean flags, unit conversions.
- Use `COALESCE` for nullable numeric fields to default to 0.
- Use `CASE` expressions for categorization and code-to-label mappings.
- Document every output column.
- SQL should be pure `SELECT` — let Bruin handle the DDL via `materialization`.

### 4. `assets/reports/` — Reports Layer (Streamlit + Altair)

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
bruin run <path/to/asset>              # Run a single asset
bruin run <path/to/pipeline/>          # Run entire pipeline
bruin run --downstream <path>          # Run asset + all downstream
bruin validate <path>                  # Validate asset/pipeline definitions
bruin format <path>                    # Auto-format asset files
bruin lineage <path>                   # Show asset dependency graph
bruin connections list                 # List configured connections
bruin connections ping <name>          # Test a connection
```

Always run individual assets during development, not entire pipelines.

## Creating a New Pipeline — Checklist

1. Create a new top-level directory named after the pipeline.
2. Add `pipeline.yml` with name, schedule, start_date, and default_connections.
3. Create `assets/raw/` with a Python ingestion script and its `requirements.txt`.
4. Create `assets/staging/` with SQL transformations that `depends` on the raw assets.
5. Create `assets/reports/` with a Streamlit app, supporting SQL files, and `requirements.txt`.
6. Validate with `bruin validate <pipeline-dir>/`.
7. Run individual assets with `bruin run <path/to/asset>` to test.

## Dependency Resolution

Bruin resolves Python dependencies by walking up the file tree from the asset to find the nearest `requirements.txt`. Keep a separate `requirements.txt` per layer (`raw/`, `reports/`) so dependencies stay isolated.

## Things to Avoid

- Do not put non-asset files inside `assets/`. Use separate directories for ad-hoc queries or analyses.
- Do not hardcode dates in SQL — use Bruin templating or `BRUIN_START_DATE`/`BRUIN_END_DATE` in Python.
- Do not use `.yml` extension for asset definitions — use `.asset.yml` if defining assets in YAML.
- Do not commit `.bruin.yml`, credentials, or `.streamlit/secrets.toml` — they are gitignored.
- Do not write `CREATE TABLE` or `INSERT` in SQL assets — let Bruin's `materialization` handle DDL.
