# stock-market

Tracks daily stock prices and quarterly financial statements for S&P 500 companies, going back to 2000.

**BigQuery destination:** `bruin-demo-data.stock_market`

## Data Sources

| Source | Type | What it provides | Limitations |
|--------|------|------------------|-------------|
| **yfinance** | Python library (free) | Daily OHLCV prices since 1962; quarterly income statements, balance sheets, cash flows (~last 5 quarters) | Unofficial API, no SLA, may throttle |
| **FMP** (Premium) | REST API ($59/mo) | Daily OHLCV prices back to 2000+; quarterly financials up to 120 quarters (~30 years) per ticker | 750 req/min, per-ticker queries |
| **Wikipedia** | Web scrape | Current S&P 500 constituent list with sector, industry, CIK | Current members only (survivorship bias) |

## Ticker Universe

S&P 500 constituents (~503 tickers), scraped from Wikipedia. Survivorship bias applies -- only current members are tracked.

## Pipeline Structure

```
stock-market/
├── pipeline.yml
├── README.md
└── assets/
    ├── raw/                              # Ingestion layer (Python)
    │   ├── requirements.txt
    │   ├── tickers.py                    # S&P 500 tickers from Wikipedia
    │   ├── prices_daily.py               # Daily OHLCV from yfinance
    │   ├── income_statements.py          # Quarterly income statements from yfinance
    │   ├── balance_sheets.py             # Quarterly balance sheets from yfinance
    │   ├── cash_flows.py                 # Quarterly cash flows from yfinance
    │   ├── fmp_prices_daily.py           # Daily OHLCV from FMP
    │   ├── fmp_income_statements.py      # Quarterly income statements from FMP
    │   ├── fmp_balance_sheets.py         # Quarterly balance sheets from FMP
    │   └── fmp_cash_flows.py             # Quarterly cash flows from FMP
    └── reports/                          # Transformation + reporting layer (SQL)
        ├── prices_daily.sql              # Deduplicated prices with technicals
        ├── prices_daily_2.sql            # Prices combining yfinance + FMP
        ├── financials_quarterly.sql      # Joined financials with derived ratios
        └── financials_quarterly_2.sql    # Financials combining yfinance + FMP
```

## Assets

### Raw Layer (`stock_market_raw.*`)

All raw assets use `strategy: append`. Deduplication happens in the reports layer.

| Asset Name | File | Source | Description |
|------------|------|--------|-------------|
| `stock_market_raw.tickers` | `tickers.py` | Wikipedia | S&P 500 tickers, company name, GICS sector/sub-industry, CIK, date added |
| `stock_market_raw.prices_daily` | `prices_daily.py` | yfinance | Daily OHLCV + adjusted close for all S&P 500 tickers |
| `stock_market_raw.income_statements` | `income_statements.py` | yfinance | Quarterly income statements (~last 5 quarters per ticker) |
| `stock_market_raw.balance_sheets` | `balance_sheets.py` | yfinance | Quarterly balance sheets (~last 5 quarters per ticker) |
| `stock_market_raw.cash_flows` | `cash_flows.py` | yfinance | Quarterly cash flow statements (~last 5 quarters per ticker) |
| `stock_market_raw.fmp_prices_daily` | `fmp_prices_daily.py` | FMP | Daily OHLCV prices, split-adjusted, back to 2000+ |
| `stock_market_raw.fmp_income_statements` | `fmp_income_statements.py` | FMP | Quarterly income statements, up to 120 quarters per ticker |
| `stock_market_raw.fmp_balance_sheets` | `fmp_balance_sheets.py` | FMP | Quarterly balance sheets, up to 120 quarters per ticker |
| `stock_market_raw.fmp_cash_flows` | `fmp_cash_flows.py` | FMP | Quarterly cash flow statements, up to 120 quarters per ticker |

### Reports Layer (`stock_market.*`)

All reports assets use `strategy: create+replace`, rebuilding from raw on each run.

| Asset Name | File | Description |
|------------|------|-------------|
| `stock_market.prices_daily` | `prices_daily.sql` | Deduped daily prices with daily return %, 5/20/50/200-day SMAs, 52-week high/low, sector enrichment. Sources from yfinance only. |
| `stock_market.prices_daily_2` | `prices_daily_2.sql` | Same as above but unions yfinance + FMP price data before deduplication. |
| `stock_market.financials_quarterly` | `financials_quarterly.sql` | Joined income + balance sheet + cash flow with derived ratios (margins, ROE, ROA, debt-to-equity, revenue growth). Sources from yfinance only. |
| `stock_market.financials_quarterly_2` | `financials_quarterly_2.sql` | Same as above but unions yfinance + FMP financial data before deduplication. |

### Derived Metrics (Reports Layer)

**Prices:**
- Daily return %, 5/20/50/200-day simple moving averages
- 52-week rolling high/low, % distance from 52-week high
- Day of week, month, quarter, year

**Financials:**
- Gross margin %, operating margin %, net margin %
- ROE %, ROA % (annualized from quarterly)
- Debt-to-equity ratio, current ratio
- Revenue QoQ and YoY growth %, EPS QoQ growth %
- Book value per share, free cash flow

## Running

```bash
# Validate pipeline
bruin validate stock-market/

# Ingest tickers
bruin run stock-market/assets/raw/tickers.py

# Ingest yfinance prices (use date range)
bruin run --start-date 2020-01-01 --end-date 2020-01-08 stock-market/assets/raw/prices_daily.py

# Ingest FMP prices (deeper history)
bruin run --start-date 2000-01-01 --end-date 2026-03-02 stock-market/assets/raw/fmp_prices_daily.py

# Ingest financials (yfinance -- fetches all available quarters regardless of date range)
bruin run stock-market/assets/raw/income_statements.py
bruin run stock-market/assets/raw/balance_sheets.py
bruin run stock-market/assets/raw/cash_flows.py

# Ingest financials (FMP -- fetches up to 120 quarters per ticker)
bruin run stock-market/assets/raw/fmp_income_statements.py
bruin run stock-market/assets/raw/fmp_balance_sheets.py
bruin run stock-market/assets/raw/fmp_cash_flows.py

# Build report tables
bruin run stock-market/assets/reports/prices_daily.sql
bruin run stock-market/assets/reports/financials_quarterly.sql
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `FMP_API_KEY` | FMP API key, injected via Bruin secrets (configured in `.bruin.yml` as a generic connection) |
| `BRUIN_START_DATE` | Start of ingestion window (YYYY-MM-DD), used by price assets |
| `BRUIN_END_DATE` | End of ingestion window (YYYY-MM-DD), used by price assets |
| `STOCK_TICKER_LIMIT` | Optional, limits the number of tickers processed (for testing) |
