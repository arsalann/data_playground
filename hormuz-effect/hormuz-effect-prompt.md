# Pipeline Prompt: The Hormuz Effect

Build a new pipeline similar to @stackoverflow-trends for **global energy crisis tracking — oil, gas, and commodity price shocks driven by the 2026 Strait of Hormuz crisis**.

**Context:** utilize Bruin MCP and Bruin CLI, reference Bruin docs. Follow @AGENTS.md strictly — these are the rules for this repo. If you are about to break any rule in AGENTS.md, stop and ask for clarification and permission before proceeding.

### Data source

**FRED API** (free, key required — already configured or register at https://fred.stlouisfed.org/docs/api/api_key.html):
- Base URL: `https://api.stlouisfed.org/fred/series/observations`
- Key series to ingest:
  - `DCOILBRENTEU` — Brent crude oil price (daily, USD/barrel, 1987–present)
  - `DCOILWTICO` — WTI crude oil price (daily, USD/barrel, 1986–present)
  - `GASREGW` — US regular gasoline price (weekly, USD/gallon)
  - `DHHNGSP` — Henry Hub natural gas spot price (daily, USD/MMBtu)
  - `GOLDAMGBD228NLBM` — Gold price London fixing (daily, USD/troy oz)
  - `PWHEAMTUSDM` — Global wheat price (monthly, USD/metric ton)
  - `PCOPPUSDM` — Global copper price (monthly, USD/metric ton)
- Auth: API key passed as `&api_key=KEY` query parameter
- Response format: JSON with `&file_type=json`
- Pagination: use `observation_start` and `observation_end` params for date range

All series can be fetched from the same endpoint, just varying the `series_id` param.

### What to extract

- **Daily prices** for oil (Brent + WTI), natural gas, and gold — going back to 1987 for oil, as far as available for others
- **Weekly US gasoline prices** — going back to 1990
- **Monthly commodity prices** (wheat, copper) — going back to 2000
- All prices in USD, with date as the primary key per series

### Naming

All asset/table names should have prefix **`hormuz_`**
Destination: BigQuery

### Dashboard questions

The goal is a final Streamlit + Altair dashboard that answers:

1. **"How does the 2026 Hormuz crisis compare to past oil shocks?"** — Overlay Brent crude price spikes: 1990 Gulf War, 2008 financial crisis, 2011 Arab Spring, 2022 Russia-Ukraine, 2026 Hormuz. Show magnitude and duration of each spike.
2. **"What's happening to prices RIGHT NOW?"** — Current Brent, WTI, gas, natural gas, gold prices with sparkline trends (last 90 days) and % change badges.
3. **"How are commodities correlated during crises?"** — Scatter/heatmap showing correlation between oil, gas, gold, wheat, copper during crisis periods vs. normal periods.
4. **"What's the historical pain at the pump?"** — US gasoline price over time with shaded bands for major geopolitical events, highlighting that current prices are at/near all-time highs.
5. **"Oil price anatomy — Brent vs WTI spread"** — The Brent-WTI spread over time, which widens during supply disruptions and signals global vs. domestic market stress.

### Build order

Start building the pipeline:

1. Create pipeline structure and a README outlining the pipeline, data sources, and processing method
2. Create the Python extraction asset that fetches all FRED series (single asset that loops through series IDs), with Bruin Python materialization, using Bruin's built-in start/end date variables
3. Test each raw asset individually for a small subset of data (last 30 days)
4. Build the staging SQL transformation assets (deduplicate, pivot to wide format, compute derived metrics like rolling averages, crisis period flags, Brent-WTI spread)
5. Test the entire pipeline end-to-end for a slightly larger window (1 year)
6. Build the Streamlit dashboard with Altair charts answering the 5 dashboard questions above

### Constraints

- Use FRED API only (free, no credit card) — register for an API key at https://fred.stlouisfed.org/docs/api/api_key.html
- Use append-only strategy for raw ingestion and deduplicate in staging
- The FRED API has a rate limit of 120 requests/minute — add a small delay between series fetches
- Store the FRED API key as a Bruin secret (`fred_api_key`)
- All historical data should be backfillable using `BRUIN_START_DATE` / `BRUIN_END_DATE`
