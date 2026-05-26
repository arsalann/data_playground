Output walkthrough.gif

Set Shell "zsh"
Set FontSize 14
Set Width 900
Set Height 720
Set Padding 24
Set Margin 12
Set MarginFill "#3C5488"
Set BorderRadius 0
Set TypingSpeed 25ms
Set PlaybackSpeed 4.0
Set WaitTimeout 180s

Hide
Type `export PS1='%F{cyan}ingestr-cli-v1%f $ '`
Enter
Type "clear"
Enter
Type "cd ingestr-cli-v1"
Enter
Sleep 10s
Type "docker compose up -d"
Enter
Sleep 10s
Type "cd .."
Enter
Sleep 10s
Type "clear"
Enter

Type `echo "Step 1) Confirm Postgres is ready"`
Enter
Sleep 10s
Type "until docker exec ingestr-cli-v1-postgres pg_isready -U ingestr_cli_v1 -d ingestr_cli_v1 >/dev/null 2>&1; do sleep 1; done; echo postgres-ready"
Enter
Sleep 10s
Type "clear"
Enter
Show

Type `echo "Step 1) Test Bruin connections"`
Enter
Sleep 10s
Type "bruin connections test --name ingestr-cli-v1-pg && bruin connections test --name ingestr-cli-v1-duckdb"
Enter
Sleep 10s
Type "clear"
Enter

Hide
Type "bruin run --end-date 2026-04-01 --full-refresh --environment default ingestr-cli-v1/assets/orders.py"
Enter
Sleep 10s
Type "clear"
Enter
Show

Type `echo "Step 2) Validate generated source orders in Postgres"`
Enter
Sleep 10s
Type `bruin query --connection ingestr-cli-v1-pg --query "select count(*), min(created_at), max(created_at) from public.orders"`
Enter
Sleep 10s
Type "clear"
Enter

Type `echo "Step 3) Full-refresh ingest from Postgres to DuckDB"`
Enter
Sleep 10s
Type `ingestr ingest --source-uri 'postgresql://ingestr_cli_v1:ingestr_cli_v1@localhost:55433/ingestr_cli_v1' --source-table 'public.orders' --dest-uri "duckdb:////Users/bear/conductor/workspaces/data_playground/montevideo/ingestr-cli-v1/ingestr_cli_v1.duckdb" --dest-table 'main.orders' --primary-key 'order_id' --incremental-key 'updated_at' --incremental-strategy 'merge' --full-refresh --yes --progress log`
Enter
Sleep 10s
Type "clear"
Enter

Type `echo "Step 4) Validate full-refresh rows in DuckDB"`
Enter
Sleep 10s
Type `bruin query --connection ingestr-cli-v1-duckdb --query "select count(*), min(created_at), max(created_at) from main.orders"`
Enter
Sleep 10s
Type "clear"
Enter

Hide
Type "bruin run --start-date 2026-05-01 --end-date 2026-06-01 --environment default ingestr-cli-v1/assets/orders.py"
Enter
Sleep 10s
Type "clear"
Enter
Show

Type `echo "Step 5) Validate incremental source rows in Postgres"`
Enter
Sleep 10s
Type `bruin query --connection ingestr-cli-v1-pg --query "select count(*), min(created_at), max(created_at) from public.orders"`
Enter
Sleep 10s
Type "clear"
Enter

Type `echo "Step 6) Incremental merge into DuckDB"`
Enter
Sleep 10s
Type `ingestr ingest --source-uri 'postgresql://ingestr_cli_v1:ingestr_cli_v1@localhost:55433/ingestr_cli_v1' --source-table 'public.orders' --dest-uri "duckdb:////Users/bear/conductor/workspaces/data_playground/montevideo/ingestr-cli-v1/ingestr_cli_v1.duckdb" --dest-table 'main.orders' --primary-key 'order_id' --incremental-key 'updated_at' --incremental-strategy 'merge' --interval-start '2026-05-01' --interval-end '2026-07-01' --yes --progress log`
Enter
Sleep 10s
Type "clear"
Enter

Type `echo "Step 7) Validate incremental rows in DuckDB"`
Enter
Sleep 10s
Type `bruin query --connection ingestr-cli-v1-duckdb --query "select count(*), min(created_at), max(created_at) from main.orders"`
Enter
Sleep 10s
