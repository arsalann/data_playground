#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

DUCKDB_URI="duckdb:///${PWD}/ingestr-cli-v1/ingestr_cli_v1.duckdb"

until docker exec ingestr-cli-v1-postgres pg_isready -U ingestr_cli_v1 -d ingestr_cli_v1 >/dev/null 2>&1; do sleep 1; done; echo postgres-ready
bruin connections test --name ingestr-cli-v1-pg && bruin connections test --name ingestr-cli-v1-duckdb
bruin run --end-date 2026-04-01 --full-refresh --environment default ingestr-cli-v1/assets/orders.py
bruin query --connection ingestr-cli-v1-pg --query "select count(*), min(created_at), max(created_at) from public.orders"
ingestr ingest --source-uri 'postgresql://ingestr_cli_v1:ingestr_cli_v1@localhost:55433/ingestr_cli_v1' --source-table 'public.orders' --dest-uri "$DUCKDB_URI" --dest-table 'main.orders' --primary-key 'order_id' --incremental-key 'updated_at' --incremental-strategy 'merge' --full-refresh --yes --progress log
bruin query --connection ingestr-cli-v1-duckdb --query "select count(*), min(created_at), max(created_at) from main.orders"
bruin run --start-date 2026-05-01 --end-date 2026-06-01 --environment default ingestr-cli-v1/assets/orders.py
bruin query --connection ingestr-cli-v1-pg --query "select count(*), min(created_at), max(created_at) from public.orders"
ingestr ingest --source-uri 'postgresql://ingestr_cli_v1:ingestr_cli_v1@localhost:55433/ingestr_cli_v1' --source-table 'public.orders' --dest-uri "$DUCKDB_URI" --dest-table 'main.orders' --primary-key 'order_id' --incremental-key 'updated_at' --incremental-strategy 'merge' --interval-start '2026-05-01' --interval-end '2026-07-01' --yes --progress log
bruin query --connection ingestr-cli-v1-duckdb --query "select count(*), min(created_at), max(created_at) from main.orders"
