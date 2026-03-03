# stock-market

Tracks daily stock prices and quarterly financial statements for S&P 500 and NASDAQ-listed companies from 2012 to present.

## Goals

1. **Daily price data** — open, high, low, close, volume, and market cap for every trading day since 2012.
2. **Quarterly financials** — EPS, P/E ratio, book value, revenue, net income, retained earnings, short-term debt, long-term debt, operating expenses, and the full balance sheet / income statement / cash flow statement fields available from each source.
3. **Daily refresh** — after the historical backfill, the pipeline runs daily to capture new price data and any newly filed quarterly reports.

## Data Source Comparison

### Paid API Comparison (recommended for full dataset + daily freshness)

| Provider | Plan | $/mo | History | Rate Limit | Bulk Endpoints | Prices | Financials | Best For |
|----------|------|------|---------|------------|----------------|--------|------------|----------|
| **FMP** | Ultimate | $149 | 30+ yrs | 3,000/min | **All tickers in 1 call** | EOD (next morning) | Quarterly + Annual | Data pipelines needing bulk ingest |
| **FMP** | Premium | $59 | 30+ yrs | 750/min | No | EOD | Quarterly + Annual | Per-ticker queries, lighter workloads |
| **Polygon.io** | Advanced + Financials | $228 | 20+ yrs | Unlimited flat files | S3 flat-file downloads | Real-time + EOD | Quarterly + Annual | Tick-level data, real-time feeds |
| **EODHD** | All-in-One | $100 | 30 yrs | 1,000/min, 100K/day | Per-exchange bulk (100 calls) | EOD | Quarterly + Annual | Broad international coverage |
| **Alpha Vantage** | 75 req/min | $50 | 20+ yrs | 75/min | No | EOD | Quarterly + Annual | Light usage, simple integration |

### Free Options (for reference / fallback)

| Source | Type | Prices | Financials | Limitation |
|--------|------|--------|------------|------------|
| **yfinance** | Python library | Daily OHLCV since 1962 | Quarterly (last ~5 yrs) | Unofficial, no SLA, may throttle |
| **BigQuery SEC Dataset** | BigQuery SQL | No | Raw XBRL (10-Q, 10-K) since 2009 | Requires XBRL tag mapping |
| **SEC EDGAR API** | REST API | No | Raw XBRL/JSON | Free, no key, but requires parsing |

## Recommended Approach

### Primary: Financial Modeling Prep (FMP) — Ultimate Plan ($149/mo)

FMP is the best fit for this pipeline because of its **bulk endpoints** — purpose-built for data pipelines:

- **Bulk daily prices**: `GET /stable/batch-end-of-day-prices` returns OHLCV for **every ticker** on a given date in one call. Backfilling 2012–present = ~3,300 API calls (one per trading day), feasible in under 2 hours at 3,000 req/min.
- **Bulk income statements**: `GET /stable/income-statement-bulk?year=2024&period=Q1` returns quarterly income statements for **all companies** in one call. 13 years × 4 quarters = 52 API calls for full history.
- **Bulk balance sheets**: Same pattern — `GET /stable/balance-sheet-statement-bulk`. 52 calls for full history.
- **Bulk cash flow**: `GET /stable/cash-flow-statement-bulk`. 52 calls.
- **Company profiles**: `GET /stable/profile-bulk` for ticker metadata (sector, industry, market cap, exchange).
- **S&P 500 / NASDAQ constituents**: `GET /stable/sp500-constituent` and `GET /stable/nasdaq-constituent` return current index membership.
- **Data freshness**: EOD prices available next morning. Financials updated within 24 hours of SEC filing.
- **Data format**: All endpoints return JSON. Bulk endpoints also support CSV.

Total API calls for full historical backfill: ~3,500 (prices) + ~160 (financials) + a handful for tickers = **under 4,000 calls**. At 3,000 req/min, the entire backfill completes in minutes.

### Daily Refresh Strategy

- **Prices**: One bulk call per trading day fetches all tickers' EOD prices.
- **Financials**: One bulk call per quarter-period checks for newly filed reports. Run daily — the API returns only companies that have filed for that period.
- **Tickers**: Refresh S&P 500 and NASDAQ constituent lists daily to capture index changes.

### Fallback / Supplementary

- **yfinance** — free fallback for ad-hoc analysis, spot-checking, or if FMP is temporarily down.
- **BigQuery SEC Public Dataset** — free cross-reference for audited SEC filing data.
- **SEC EDGAR Company Facts API** — free, no key, structured JSON for validating financials against official filings.

### API Key Management

Store the FMP API key in `.bruin.yml` as a secret or pass via environment variable `FMP_API_KEY`. The key is injected into Python assets at runtime. Never commit the key to version control.

## Ticker Universe

The pipeline ingests data for the current constituents of:

- **S&P 500** (~500 tickers) — large-cap US equities
- **NASDAQ Composite** (~3,000+ tickers) — all NASDAQ-listed equities

Combined deduplicated universe: ~3,200 unique tickers (many S&P 500 members are NASDAQ-listed).

Constituent lists are fetched from FMP's `/stable/sp500-constituent` and `/stable/nasdaq-constituent` endpoints. Survivorship bias note: we track current constituents only — delisted tickers are excluded from backfill. The bulk price endpoint returns data for all tradeable US equities regardless of index membership, so price coverage is comprehensive.

## Assets

### Raw

| Asset | File | Type | Source | Description |
|-------|------|------|--------|-------------|
| `raw.stock_tickers` | `stock_tickers.py` | Python | FMP | S&P 500 + NASDAQ constituent tickers with sector, industry, exchange, market cap |
| `raw.stock_prices_daily` | `stock_prices_daily.py` | Python | FMP Bulk | Daily OHLCV + adjusted close + volume + market cap for all tickers |
| `raw.stock_income_statements` | `stock_income_statements.py` | Python | FMP Bulk | Quarterly income statements: revenue, COGS, gross profit, operating expenses, operating income, net income, EPS, EBITDA, etc. |
| `raw.stock_balance_sheets` | `stock_balance_sheets.py` | Python | FMP Bulk | Quarterly balance sheets: total assets, total liabilities, equity, retained earnings, cash, short/long-term debt, book value, etc. |
| `raw.stock_cash_flows` | `stock_cash_flows.py` | Python | FMP Bulk | Quarterly cash flow statements: operating cash flow, capex, free cash flow, dividends paid, share repurchases, etc. |

### Staging

| Asset | File | Description |
|-------|------|-------------|
| `staging.stock_prices_daily` | `stock_prices_daily.sql` | Deduped daily prices with daily return %, 50/200-day SMAs, 52-week high/low, YTD return, sector/industry enrichment |
| `staging.stock_financials_quarterly` | `stock_financials_quarterly.sql` | Joined income + balance sheet + cash flow with derived ratios: P/E, debt-to-equity, margins, ROE, ROA, revenue growth |

### Reports

| Asset | File | Description |
|-------|------|-------------|
| `streamlit_app.py` | `streamlit_app.py` | Interactive dashboard — price charts, financial comparisons, sector heatmaps |
| `price_performance.sql` | `price_performance.sql` | Price performance summary by ticker and time period |
| `financial_overview.sql` | `financial_overview.sql` | Latest quarterly financials with key ratios |

## Processing Method

### Ingestion (`raw/`)

1. **Ticker list** (`stock_tickers.py`):
   - Calls FMP `/stable/sp500-constituent` and `/stable/nasdaq-constituent` endpoints.
   - Supplements with `/stable/profile-bulk` for sector, industry, market cap, exchange, and company description.
   - Deduplicates tickers that appear in both indices. Stores index membership flags (`is_sp500`, `is_nasdaq`).
   - Strategy: `create+replace` (refreshed on each run to capture index rebalances).

2. **Daily prices** (`stock_prices_daily.py`):
   - Uses FMP bulk endpoint: `GET /stable/batch-end-of-day-prices?date=YYYY-MM-DD`.
   - Returns OHLCV + adjusted close + market cap + change % for **all tickers** in one call.
   - Historical backfill: iterates over each trading day from 2012-01-01 to today (~3,300 calls).
   - Daily runs: fetches only the last trading day (1 call).
   - Strategy: `append` (with dedup in staging on `ticker + date`).

3. **Income statements** (`stock_income_statements.py`):
   - Uses FMP bulk endpoint: `GET /stable/income-statement-bulk?year=YYYY&period=Q1`.
   - Returns quarterly income statements for all companies that filed for that period.
   - Key fields: revenue, cost_of_revenue, gross_profit, operating_expenses, operating_income, income_before_tax, net_income, eps, eps_diluted, ebitda, weighted_avg_shares_diluted.
   - Historical backfill: 13 years × 4 quarters = 52 calls.
   - Strategy: `create+replace`.

4. **Balance sheets** (`stock_balance_sheets.py`):
   - Uses FMP bulk endpoint: `GET /stable/balance-sheet-statement-bulk?year=YYYY&period=Q1`.
   - Key fields: total_assets, total_liabilities, total_stockholders_equity, retained_earnings, total_current_assets, total_current_liabilities, cash_and_equivalents, short_term_debt, long_term_debt, total_debt, net_debt, book_value (equity / shares), goodwill, intangible_assets, inventory.
   - Strategy: `create+replace`.

5. **Cash flow statements** (`stock_cash_flows.py`):
   - Uses FMP bulk endpoint: `GET /stable/cash-flow-statement-bulk?year=YYYY&period=Q1`.
   - Key fields: operating_cash_flow, capital_expenditure, free_cash_flow, dividends_paid, share_repurchases, acquisitions, debt_repayment, net_change_in_cash.
   - Strategy: `create+replace`.

### Transformation (`staging/`)

6. **Daily prices staging** (`stock_prices_daily.sql`):
   - Deduplicates on `(ticker, date)` keeping latest `extracted_at`.
   - Adds: daily return %, 5/20/50/200-day simple moving averages (window functions), 52-week rolling high/low, distance from 52-week high, YTD return, day of week, month, quarter, year.
   - Joins with `raw.stock_tickers` for sector, industry, and index membership enrichment.

7. **Quarterly financials staging** (`stock_financials_quarterly.sql`):
   - Joins income statements + balance sheets + cash flows on `(ticker, fiscal_quarter_end)`.
   - Deduplicates on `(ticker, fiscal_quarter_end)` keeping latest `extracted_at`.
   - Derives: P/E ratio (price / EPS from latest daily close), debt-to-equity, current ratio, gross margin %, operating margin %, net margin %, ROE, ROA, revenue QoQ and YoY growth %, EPS QoQ growth %, free cash flow yield.
   - Joins with `raw.stock_tickers` for sector/industry.

### Dashboards (`reports/`)

8. **Streamlit app** — interactive exploration:
   - Ticker search with price chart (candlestick or line).
   - Financial statement waterfall / trend charts.
   - Sector comparison heatmaps.
   - Screener-style table with sortable financial metrics.

## Volume Estimates

| Table | Rows (est.) | Backfill API Calls | Daily API Calls | Refresh Strategy |
|-------|-------------|-------------------|-----------------|------------------|
| `raw.stock_tickers` | ~3,200 | 3 | 3 | `create+replace` |
| `raw.stock_prices_daily` | ~10M (backfill) + ~3,200/day | ~3,300 (one per trading day) | 1 | `append` |
| `raw.stock_income_statements` | ~170K (13 yrs × 4Q × ~3,200) | 52 | 4 | `create+replace` |
| `raw.stock_balance_sheets` | ~170K | 52 | 4 | `create+replace` |
| `raw.stock_cash_flows` | ~170K | 52 | 4 | `create+replace` |
| `staging.stock_prices_daily` | ~10M+ | — | — | `create+replace` |
| `staging.stock_financials_quarterly` | ~170K | — | — | `create+replace` |

## Running

```bash
# Validate pipeline
bruin validate stock-market/

# Ingest ticker universe
bruin run stock-market/assets/raw/stock_tickers.py

# Historical price backfill (set wide date range)
bruin run --start-date 2012-01-01 --end-date 2026-03-02 stock-market/assets/raw/stock_prices_daily.py

# Ingest all quarterly financials (income, balance sheet, cash flow)
bruin run stock-market/assets/raw/stock_income_statements.py
bruin run stock-market/assets/raw/stock_balance_sheets.py
bruin run stock-market/assets/raw/stock_cash_flows.py

# Build staging tables
bruin run stock-market/assets/staging/stock_prices_daily.sql
bruin run stock-market/assets/staging/stock_financials_quarterly.sql

# Launch dashboard
streamlit run stock-market/assets/reports/streamlit_app.py
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `FMP_API_KEY` | Financial Modeling Prep API key (required). Set in `.bruin.yml` secrets or shell env. |
| `BRUIN_START_DATE` | Start of ingestion window (YYYY-MM-DD). Used by price backfill. |
| `BRUIN_END_DATE` | End of ingestion window (YYYY-MM-DD). Used by price backfill. |

## Future Enhancements

- Add historical index constituent lists to address survivorship bias.
- Cross-validate financials against BigQuery SEC public dataset / SEC EDGAR.
- Build sector/industry aggregation staging tables (sector-level averages, rankings).
- Add dividend and stock split tracking (FMP provides these endpoints).
- Add analyst estimates and earnings surprises (FMP `/stable/earnings-surprises-bulk`).
- Add options data for implied volatility analysis.
- Add technical indicators staging table (RSI, MACD, Bollinger Bands — computed in SQL from daily prices).
