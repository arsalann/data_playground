# ingestr-cli-v1

`ingestr-cli-v1` is a small Bruin pipeline that demonstrates the basic
features of `ingestr v1` from the CLI. It generates synthetic orders in local
Postgres with a Bruin Python asset, then uses `ingestr ingest` directly to copy
the table into local DuckDB.

This example intentionally does not use a Bruin `type: ingestr` YAML asset. The
goal is to show the CLI workflow: full refresh, query validation, incremental
merge, and query validation again.

## Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Runs local Postgres 16 on port `55433` |
| `pipeline.yml` | Bruin pipeline metadata and default connection names |
| `assets/orders.py` | Python source-data generator for `public.orders` |
| `run_ingestr_cli_v1.sh` | End-to-end CLI demo commands |
| `walkthrough.tape` | VHS recording script for the walkthrough GIF |
| `walkthrough.gif` | Generated terminal walkthrough |

## Connections

Add these connections to the repo-root `.bruin.yml` in the `default`
environment:

```yaml
duckdb:
  - name: ingestr-cli-v1-duckdb
    path: ingestr-cli-v1/ingestr_cli_v1.duckdb

postgres:
  - name: ingestr-cli-v1-pg
    username: ingestr_cli_v1
    password: ingestr_cli_v1
    host: localhost
    port: 55433
    database: ingestr_cli_v1
    ssl_mode: disable
```

## What The Demo Shows

1. Postgres readiness and Bruin connection checks.
2. Full-refresh source-data generation with `assets/orders.py`.
3. Source validation with `bruin query`.
4. Full-refresh Postgres-to-DuckDB ingestion with `ingestr ingest`.
5. Destination validation with `bruin query`.
6. Incremental source-data generation for May and June 2026.
7. Incremental Postgres-to-DuckDB merge with `ingestr ingest`.
8. Final destination validation with `bruin query`.

The synthetic generator emits 100,000 rows per calendar month whose first day
falls within the Bruin run interval. The first full-refresh command uses the
pipeline `start_date` and `--end-date 2026-04-01`, so it generates monthly
orders through April 2026. The second Bruin run generates May and June 2026.

## Run It

Start Postgres:

```bash
cd ingestr-cli-v1
docker compose up -d
cd ..
```

Run the CLI demo:

```bash
./ingestr-cli-v1/run_ingestr_cli_v1.sh
```

The full list of commands is in
`/Users/bear/conductor/workspaces/data_playground/montevideo/ingestr-cli-v1/run_ingestr_cli_v1.sh`.

The walkthrough breaks down into these terminal steps:

### 1. Build the DuckDB destination URI

The script starts from the repo root and stores the DuckDB destination URI in a
variable so every `ingestr ingest` command writes to the same local database.

```bash
cd "$(dirname "$0")/.."

DUCKDB_URI="duckdb:///${PWD}/ingestr-cli-v1/ingestr_cli_v1.duckdb"
```

### 2. Wait for Postgres to be ready

This waits until the local Postgres container accepts connections, then prints a
short readiness message.

```bash
until docker exec ingestr-cli-v1-postgres pg_isready -U ingestr_cli_v1 -d ingestr_cli_v1 >/dev/null 2>&1; do sleep 1; done; echo postgres-ready
```

### 3. Test the Bruin connections

This confirms both local connections are configured correctly in the repo-root
`.bruin.yml`.

```bash
bruin connections test --name ingestr-cli-v1-pg && bruin connections test --name ingestr-cli-v1-duckdb
```

### 4. Generate the full-refresh source data

This runs the Bruin Python asset that creates synthetic source orders in
Postgres. The `--full-refresh` flag clears and rebuilds the source table for the
historical demo range.

```bash
bruin run --end-date 2026-04-01 --full-refresh --environment default ingestr-cli-v1/assets/orders.py
```

### 5. Validate the source table in Postgres

This checks the source row count and date range before moving data with
`ingestr`.

```bash
bruin query --connection ingestr-cli-v1-pg --query "select count(*), min(created_at), max(created_at) from public.orders"
```

### 6. Run the full-refresh ingestr load

This copies `public.orders` from Postgres into `main.orders` in DuckDB. The
primary key and incremental key are declared even on the full-refresh run so the
next incremental merge uses the same table contract.

```bash
ingestr ingest --source-uri 'postgresql://ingestr_cli_v1:ingestr_cli_v1@localhost:55433/ingestr_cli_v1' --source-table 'public.orders' --dest-uri "$DUCKDB_URI" --dest-table 'main.orders' --primary-key 'order_id' --incremental-key 'updated_at' --incremental-strategy 'merge' --full-refresh --yes --progress log
```

### 7. Validate the full-refresh DuckDB table

This confirms DuckDB now has the expected row count and date range after the
full-refresh load.

```bash
bruin query --connection ingestr-cli-v1-duckdb --query "select count(*), min(created_at), max(created_at) from main.orders"
```

### 8. Generate the incremental source data

This runs the same source generator for the next interval without
`--full-refresh`, adding the newer orders to Postgres.

```bash
bruin run --start-date 2026-05-01 --end-date 2026-06-01 --environment default ingestr-cli-v1/assets/orders.py
```

### 9. Validate the expanded Postgres source table

This confirms the source table now includes the incremental rows.

```bash
bruin query --connection ingestr-cli-v1-pg --query "select count(*), min(created_at), max(created_at) from public.orders"
```

### 10. Run the incremental ingestr merge

This uses `updated_at` as the incremental key and `order_id` as the primary key
to merge the new interval into DuckDB.

```bash
ingestr ingest --source-uri 'postgresql://ingestr_cli_v1:ingestr_cli_v1@localhost:55433/ingestr_cli_v1' --source-table 'public.orders' --dest-uri "$DUCKDB_URI" --dest-table 'main.orders' --primary-key 'order_id' --incremental-key 'updated_at' --incremental-strategy 'merge' --interval-start '2026-05-01' --interval-end '2026-07-01' --yes --progress log
```

### 11. Validate the final DuckDB table

This final check confirms the destination table reflects the full-refresh data
plus the incremental merge.

```bash
bruin query --connection ingestr-cli-v1-duckdb --query "select count(*), min(created_at), max(created_at) from main.orders"
```

## Expected Shape

The first phase loads the historical source range into Postgres and then into
DuckDB as a full refresh. The second phase appends May and June 2026 orders to
Postgres and uses `--incremental-strategy merge` with `--incremental-key
updated_at` and `--primary-key order_id` to merge just the new interval into
DuckDB.

## Tear Down

```bash
cd ingestr-cli-v1
docker compose down -v
rm -f ingestr_cli_v1.duckdb
```
