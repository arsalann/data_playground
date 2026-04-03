# Polymarket Insights

Analysis of prediction market data from [Polymarket](https://polymarket.com), the world's largest prediction market platform. This pipeline ingests market metadata and historical price data, enriches it with topic classification and derived metrics, and powers a focused Streamlit dashboard that tells the story of Q1 2026 through the lens of real-money prediction markets.

## What This Pipeline Does

Polymarket is the world's largest prediction market where people bet real money on future events. Prices represent implied probabilities (e.g. a price of $0.70 means the crowd thinks there's a 70% chance). This pipeline ingests the top 10,000 markets by volume and their price histories, then surfaces the most dramatic moments where the crowd was wrong or reality shifted overnight.

## Dashboard

The Streamlit dashboard tells four stories, scoped to 2026:

### 1. The Iran Escalation (1% to Certain in Three Days)
$131M was wagered on whether Khamenei would be ousted as Supreme Leader by Feb 28, 2026. On Feb 25, the market priced it at 1.1%. Three days later it resolved YES. Overlaid with the $90M "US Strikes Iran" market that tracked the same escalation arc.

### 2. The Deal That Fell Apart (Government Shutdown)
On Jan 23, with a deal seemingly in hand, the market priced a government shutdown at just 6.5%. The deal collapsed overnight. Eight days later, the government shut down. $157M in volume — the highest-volume resolved event of Q1 2026.

### 3. The Fed Trapped (Rate Cuts Evaporated)
In Dec 2025 the market gave a Jan rate cut 50/50 odds. Then inflation data, oil price spikes from the Iran crisis, and economic uncertainty froze the Fed. By March, the probability of a rate cut collapsed to 0.3%. The Fed was trapped between inflation and recession.

### 4. Q1 2026: Confirmed by the Crowd
A timeline bar chart of every major non-sports event that resolved YES in Q1 2026, sized by volume and color-coded by theme (Iran Crisis, US Politics, Economy, Crypto, Latin America, Geopolitics). Covers government shutdowns, Iran strikes, Khamenei ousted, anti-cartel ops in Mexico, Maduro captured, crude oil hitting $100, Bitcoin crashing to $65K, and more.

## Data Sources

- **Polymarket Gamma API** (`gamma-api.polymarket.com`) — Market metadata: questions, categories, volumes (24h/1w/1m/total), liquidity, current prices, resolution status, price changes, bid/ask spreads. No authentication required. Paginated with 100 items per page. ~260K total markets on the platform; we ingest the top 10K by volume.
- **Polymarket CLOB API** (`clob.polymarket.com`) — Historical price timeseries per outcome token via the `prices-history` endpoint. Returns timestamped price points (0.0-1.0) for each outcome. No authentication required. We fetch full history for the top 50 markets by volume.

## How It Was Built

### Research Phase
Explored the [pmxt](https://github.com/pmxt-dev/pmxt) library (a "CCXT for prediction markets") and the three direct Polymarket API surfaces (Gamma, CLOB, Data API). Chose direct API access over pmxt for simplicity since we only need read-only market metadata and price history.

### Ingestion Challenges
The Gamma API returns ~260K+ markets. Initial ingestion attempted to pull all of them which took 20+ minutes of pagination. Restructured to sort by volume descending (`order=volumeNum`) and cap at 10K markets via `POLYMARKET_MAX_MARKETS` env var. This captures all meaningful markets (anything with significant volume) while keeping ingestion under 4 minutes.

### Data Exploration
Ran 20+ exploratory SQL queries against the ingested data to find the most provocative insights. Initial dashboard was a broad summary (category breakdowns, KPIs, topic deep-dives). Iterated to focus on just the 2-4 most dramatic stories with clear narratives, after feedback that a summary is less compelling than specific stories.

### Visualization Standards
All charts follow the Wong (2011) colorblind-friendly palette from Nature Methods. Every chart pairs color with a secondary visual channel (stroke dash patterns for line charts, direct value labels for bar charts, date prefixes on y-axis labels). 50% reference lines on probability charts. Diamond-shaped annotation markers at key moments. Horizontal legends at top.

## Assets

### Raw Layer

| Asset | Type | Strategy | Description |
|-------|------|----------|-------------|
| `raw.polymarket_markets` | Python | append | Fetches top markets by volume from the Gamma API. Paginated ingestion with retry/backoff logic, sorted by volume descending. |
| `raw.polymarket_price_history` | Python | append | Fetches daily price history for top N markets from the CLOB API `prices-history` endpoint. |

### Staging Layer

| Asset | Type | Strategy | Depends On | Description |
|-------|------|----------|------------|-------------|
| `staging.polymarket_markets_enriched` | SQL | create+replace | `raw.polymarket_markets` | Deduplicates raw data, classifies markets into 14 topic categories via keyword matching, computes implied probability, adds volume rankings. |
| `staging.polymarket_topic_summary` | SQL | create+replace | `staging.polymarket_markets_enriched` | Aggregates by topic: volume, market counts, resolution rates, top markets per topic. |
| `staging.polymarket_biggest_movers` | SQL | create+replace | `staging.polymarket_markets_enriched` | Markets with the largest absolute price movements across 1d/1w/1m, filtered to >$100K volume. |
| `staging.polymarket_headline_events` | SQL | create+replace | `staging.polymarket_markets_enriched` | Curates significant events by volume tier (Mega $50M+, Major $10M+, Notable $1M+) with resolution status. |

### Reports Layer

| Asset | Type | Description |
|-------|------|-------------|
| `streamlit_app.py` | Streamlit + Altair | Four-story dashboard: Iran escalation, government shutdown, Fed trapped, Q1 timeline. |

## Run Commands

```bash
# Validate the pipeline
bruin validate polymarket-insights/

# Run raw assets individually (recommended for first run)
POLYMARKET_MAX_MARKETS=10000 bruin run polymarket-insights/assets/raw/polymarket_markets.py
POLYMARKET_MARKET_LIMIT=50 bruin run polymarket-insights/assets/raw/polymarket_price_history.py

# Run bruin ai enhance to generate/improve metadata
bruin ai enhance polymarket-insights/assets/raw/polymarket_markets.py
bruin ai enhance polymarket-insights/assets/raw/polymarket_price_history.py

# Run staging assets (must run enriched first, then the rest)
bruin run polymarket-insights/assets/staging/polymarket_markets_enriched.sql
bruin run polymarket-insights/assets/staging/polymarket_topic_summary.sql
bruin run polymarket-insights/assets/staging/polymarket_biggest_movers.sql
bruin run polymarket-insights/assets/staging/polymarket_headline_events.sql

# Or run the full pipeline end-to-end
bruin run polymarket-insights/

# Launch the dashboard
streamlit run polymarket-insights/assets/reports/streamlit_app.py
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POLYMARKET_MAX_MARKETS` | 50000 | Max markets to fetch from Gamma API (10000 recommended) |
| `POLYMARKET_MARKET_LIMIT` | 200 | Number of top markets to fetch price history for (50 recommended for first run) |

## Key Findings (Q1 2026)

| Event | Volume | What Happened |
|-------|--------|---------------|
| Government Shutdown | $157M | Deal collapsed Jan 23 (6.5%) to shutdown Jan 31 (99.7%) |
| Khamenei Ousted | $131M | 1.1% on Feb 25, resolved YES on Mar 1 |
| Fed Holds Rates (Jan) | $107M | Rate cut hopes went from 50% to 0.3% |
| US Strikes Iran | $90M | Military strikes confirmed, escalation tracked alongside Khamenei |
| Anti-Cartel Ops in Mexico | $30M | Ground operation confirmed Jan 31 |
| US-Iran Meeting | $23M | Diplomatic meeting Feb 6 created brief hope |
| Crude Oil Hits $100 | $17M | Iran war spiked oil prices |
| US Recession | $12M | Officially confirmed by Feb 28 |
| Maduro in US Custody | $11M | Venezuela operation confirmed Jan 31 |
| Bitcoin Crashes to $65K | $10M | Down from $1M+ in 2025 |

## Known Limitations

- The Gamma API `category` field is empty for ~99% of markets; topic classification is derived from question text via keyword matching, which may miscategorize some markets.
- Price history from the CLOB API uses synthetic candles (O=H=L=C) reconstructed from trade data, not native OHLCV.
- The Gamma API has Cloudflare protection with a 180-second cache TTL; rapid re-runs may return stale data.
- Only the top 50 markets have price history ingested. To get price history for additional markets, increase `POLYMARKET_MARKET_LIMIT`.
- Sorted API queries (`order=volumeNum`) are slower (~2.5s per page) compared to unsorted (~0.5s per page).
