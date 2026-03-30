# hormuz-effect

**"The Double Squeeze"** — How the 2026 Strait of Hormuz oil shock and tariffs are hitting American consumers from both sides. Tracks energy prices, inflation (CPI), consumer sentiment, and recession signals with historical crisis comparisons.

## Data Sources

- **FRED API** (`api.stlouisfed.org`) — 14 free Federal Reserve economic data series:
  - Energy: Brent crude, WTI crude, gasoline, natural gas
  - Commodities: wheat, copper
  - Inflation: CPI All Items, Core CPI, CPI Gasoline, CPI New Vehicles
  - Sentiment: Consumer Sentiment Index, 1-year Inflation Expectations
  - Recession signals: Unemployment Rate, Yield Curve (10Y-2Y)

## Assets

### Raw

| Asset | Type | Description |
|---|---|---|
| `hormuz_fred_prices` | Python | Fetches all 14 FRED series with date-range support |

### Staging

| Asset | Type | Description |
|---|---|---|
| `hormuz_prices_wide` | SQL | Deduplicates, pivots to wide format (one column per indicator) |
| `hormuz_crisis_analysis` | SQL | Adds crisis period flags, YoY inflation rates, rolling averages, Brent-WTI spread |

### Reports

| Asset | Description |
|---|---|
| `streamlit_app` | "The Double Squeeze" dashboard — 6 acts telling the story from oil shock to recession watch |

## Dashboard Narrative

1. **The Oil Shock** — Brent crude through every crisis since 2010 + normalized comparison
2. **The Tariff Tax** — CPI All Items vs Core CPI vs CPI Gasoline (split the blame)
3. **Where You Feel It** — Gas prices + new car sticker shock side by side
4. **The National Mood** — Consumer sentiment cratering + inflation expectations spiking
5. **Recession Watch** — Yield curve + unemployment rate
6. **Brent-WTI Spread** — Global vs domestic market stress signal

## Running

```bash
# Full pipeline
bruin run hormuz-effect/

# Ingest recent data
bruin run --start-date 2026-02-24 --end-date 2026-03-26 hormuz-effect/assets/raw/hormuz_fred_prices.py

# Backfill from 2010
bruin run --start-date 2010-01-01 --end-date 2026-03-26 hormuz-effect/assets/raw/hormuz_fred_prices.py

# Launch dashboard
streamlit run hormuz-effect/assets/reports/streamlit_app.py
```
