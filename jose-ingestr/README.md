# jose-ingestr

Test pipeline for the latest `ingestr` (currently `v0.14.155`) inside a Bruin
pipeline. It loads synthetic orders into a **local Postgres** running in Docker
and then ingests them into a **local DuckDB** file using
`incremental_strategy: merge`.

## What's in this pipeline

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Spins up a local Postgres 16 on port `55432` |
| `pipeline.yml` | Monthly schedule, `start_date: 2025-06-01`, default connections `jose-pg` and `jose-duckdb` |
| `assets/orders.py` | Python asset, materializes **100,000 rows per calendar month** in the run interval into `jose-pg.public.orders` (strategy `append`) |
| `assets/orders_to_duckdb.asset.yml` | ingestr asset, `jose-pg.public.orders` → `jose-duckdb.main.orders`, merge on `order_id`, incremental key `updated_at` |

The Python asset reads `BRUIN_START_DATE` / `BRUIN_END_DATE` and produces
exactly **100,000 rows for every calendar month** whose first day falls inside
the interval, with `created_at`/`updated_at` randomly spread across the days of
that month. Running 2025-06-01 → 2026-03-31 (10 months) therefore yields
1,000,000 rows in one shot. The RNG is seeded per month so backfills are
deterministic.

## Connections

Two connections were added to the **repo-root** `.bruin.yml`:

```yaml
duckdb:
  - name: jose-duckdb
    path: jose-ingestr/jose.duckdb
postgres:
  - name: jose-pg
    username: jose
    password: jose
    host: localhost
    port: 55432
    database: jose
    ssl_mode: disable
```

If you blow away the docker volume or change ports, update the connection
block accordingly.

## Prerequisites

- Docker Desktop running
- `bruin` CLI installed
- The bundled `ingestr` that ships with Bruin is used by default

## Step-by-step

### 1. Start the local Postgres

```bash
cd jose-ingestr
docker compose up -d
until docker exec jose-ingestr-postgres pg_isready -U jose -d jose >/dev/null 2>&1; do sleep 1; done
cd ..
```

Verify the connection:

```bash
bruin connections test --name jose-pg
```

### 2. Validate the pipeline

```bash
bruin validate jose-ingestr
```

You should see "Successfully validated 2 assets across 1 pipeline".

> [!NOTE]
> In every `bruin run` command below, replace `path/to/jose-ingestr` with the
> absolute path to this folder, e.g.
> `/Users/you/repos/data_playground/jose-ingestr`.

### 3. Backfill the Python asset (Jun 2025 → Mar 2026)

Generate 10 months × 100,000 = 1,000,000 rows in Postgres.

```bash
bruin run \
  --start-date 2025-06-01T00:00:00.000Z \
  --end-date 2026-03-31T23:59:59.999999999Z \
  --full-refresh \
  --environment default \
  "path/to/jose-ingestr/assets/orders.py"
```

Confirm the sample data landed in Postgres:

```bash
bruin query --connection jose-pg --query "select count(*) from public.orders"
```

Expected: **1,000,000**.

### 4. Backfill the ingestr asset (Jun 2025 → Mar 2026)

Full-refresh the DuckDB destination from the Postgres source over the same
interval.

```bash
bruin run \
  --start-date 2025-06-01T00:00:00.000Z \
  --end-date 2026-03-31T23:59:59.999999999Z \
  --full-refresh \
  --environment default \
  "path/to/jose-ingestr/assets/orders_to_duckdb.asset.yml"
```

Confirm DuckDB now mirrors Postgres:

```bash
bruin query --connection jose-duckdb --query "select count(*) from main.orders"
```

Expected: **1,000,000**.

### 5. Incremental run — Python asset for April 2026

Append another 100,000 April rows to Postgres.

```bash
bruin run \
  --start-date 2026-04-01T00:00:00.000Z \
  --end-date 2026-04-30T23:59:59.999999999Z \
  --environment default \
  "path/to/jose-ingestr/assets/orders.py"
```

Confirm Postgres now has 11 months of data:

```bash
bruin query --connection jose-pg --query "select count(*) from public.orders"
```

Expected: **1,100,000**.

### 6. Incremental run — ingestr asset for April 2026

This is the test the pipeline was built for. With `incremental_strategy: merge`
and `incremental_key: updated_at`, ingestr only pulls the April rows from
Postgres and merges them into DuckDB on `order_id`.

```bash
bruin run \
  --start-date 2026-04-01T00:00:00.000Z \
  --end-date 2026-04-30T23:59:59.999999999Z \
  --environment default \
  "path/to/jose-ingestr/assets/orders_to_duckdb.asset.yml"
```

Confirm DuckDB picked up the new April rows:

```bash
bruin query --connection jose-duckdb --query "select count(*) from main.orders"
```

Expected: **1,100,000** (1M backfill + 100k April).

## Tear-down

```bash
cd jose-ingestr
docker compose down -v
rm -f jose.duckdb
```

This wipes both the Postgres volume and the local DuckDB file.

## Notes & gotchas

- The Python asset uses `strategy: append`, so re-running the same interval
  will **duplicate** rows in Postgres. The ingestr step is still idempotent
  because it merges by `order_id`. To re-seed a month in Postgres cleanly,
  truncate the table first or pass `--full-refresh`.
- `--end-date YYYY-MM-DD` is parsed as start-of-day, which makes ingestr's
  interval filter clip the last day. Use an explicit end-of-day RFC3339
  timestamp (`2026-03-31T23:59:59.999999999Z`) as shown in the run commands.
- Bruin auto-forwards the run interval to ingestr as `--interval-start` /
  `--interval-end`, which is what makes the April-only incremental work.
- DuckDB only supports one writer at a time — close any other process holding
  `jose.duckdb` before running.
