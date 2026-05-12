Output walkthrough.gif

Set Shell "zsh"
Set FontSize 14
Set Width 750
Set Height 750
Set Padding 24
Set Margin 12
Set MarginFill "#FF5569"
Set BorderRadius 0
Set TypingSpeed 30ms
Set PlaybackSpeed 2.0
Set WaitTimeout 90s

Hide
Type `export PS1='%F{magenta}jose-ingestr%f $ '`
Enter
Type "clear"
Enter
Show

Hide
Type "docker compose up -d"
Enter
Sleep 3s
Type "until docker exec jose-ingestr-postgres pg_isready -U jose -d jose >/dev/null 2>&1; do sleep 1; done"
Enter
Sleep 2s
Type "clear"
Enter
Show

Hide
Type `echo "Step 1) Test the Postgres connection"`
Enter
Show
Sleep 2s
Type "bruin connections test --name jose-pg"
Enter
Sleep 3s
Sleep 2s
Hide
Type "clear"
Enter
Show

Hide
Type `echo "Step 2) Validate the pipeline"`
Enter
Show
Sleep 2s
Type "cd .."
Enter
Sleep 400ms
Type "bruin validate jose-ingestr"
Enter
Sleep 3s
Sleep 2s
Hide
Type "clear"
Enter
Show

Hide
Type `bruin run --start-date 2025-06-01T00:00:00.000Z --end-date 2026-03-31T23:59:59.999999999Z --full-refresh --environment default jose-ingestr/assets/orders.py`
Enter
Sleep 30s
Type "clear"
Enter
Type `echo "Step 3) Generate 10 months of sample data (1,000,000 rows) in Postgres"`
Enter
Show
Sleep 2s
Type `bruin query --connection jose-pg --query "select count(*) from public.orders"`
Enter
Sleep 4s
Hide
Type "clear"
Enter
Show

Hide
Type `echo "Step 4) Ingest data from Postgres to DuckDB (full refresh)"`
Enter
Show
Sleep 2s
Type `bruin run --start-date 2025-06-01T00:00:00.000Z --end-date 2026-03-31T23:59:59.999999999Z --full-refresh --environment default jose-ingestr/assets/orders_to_duckdb.asset.yml`
Enter
Sleep 10s
Hide
Type "clear"
Enter
Type `echo "Step 4) Ingest data from Postgres to DuckDB (full refresh)"`
Enter
Show
Sleep 1s
Type `bruin query --connection jose-duckdb --query "select count(*) from main.orders"`
Enter
Sleep 2s
Hide
Type "clear"
Enter
Show

Hide
Type `bruin run --start-date 2026-04-01T00:00:00.000Z --end-date 2026-04-30T23:59:59.999999999Z --environment default jose-ingestr/assets/orders.py`
Enter
Sleep 20s
Type "clear"
Enter
Type `echo "Step 5) Append April 2026 sample data to Postgres (+100,000 rows)"`
Enter
Show
Sleep 2s
Type `bruin query --connection jose-pg --query "select count(*) from public.orders"`
Enter
Sleep 4s
Hide
Type "clear"
Enter
Show

Hide
Type `echo "Step 6) Run April 2026 incremental ingest into DuckDB"`
Enter
Show
Sleep 2s
Type `bruin run --start-date 2026-04-01T00:00:00.000Z --end-date 2026-04-30T23:59:59.999999999Z --environment default jose-ingestr/assets/orders_to_duckdb.asset.yml`
Enter
Sleep 8s
Hide
Type "clear"
Enter
Type `echo "Step 6) Run April 2026 incremental ingest into DuckDB"`
Enter
Show
Sleep 1s
Type `bruin query --connection jose-duckdb --query "select count(*) from main.orders"`
Enter
Sleep 4s
