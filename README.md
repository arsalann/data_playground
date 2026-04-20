# data_playground

A collection of data pipelines for exploring public datasets, built with Bruin and warehoused in BigQuery.

## Getting Started

### Prerequisites

1. **Python 3.10+** — install via [python.org](https://www.python.org/downloads/) or `brew install python`
2. **Bruin CLI** — install with:
   ```bash
   curl -LsSf https://getbruin.com/install/cli | sh
   ```
3. **Google Cloud credentials** — place your service account key at `credentials/playground_key.json` (this directory is gitignored)
4. **Bruin config** — create `.bruin.yml` at the repo root with your connection credentials (this file is gitignored — see `AGENTS.md` for the expected structure)
5. **Python dependencies** — install from the repo root:
   ```bash
   pip install -r requirements.txt
   ```

### Secrets

**No secrets, API keys, or credentials are stored in this repository.** All sensitive configuration is managed locally:

- `.bruin.yml` — Bruin connection credentials (gitignored)
- `credentials/` — GCP service account JSON files (gitignored)
- `.streamlit/secrets.toml` — Streamlit app credentials (gitignored)

See `AGENTS.md` for full secrets management guidelines.

### Running a Pipeline

Each subdirectory is a self-contained Bruin pipeline. Here's how to run the `berlin-weather` pipeline as an example:

```bash
# validate the pipeline
bruin validate berlin-weather/

# run the full pipeline (ingest → staging → reports)
bruin run berlin-weather/

# run just the raw ingest asset
bruin run berlin-weather/assets/raw/weather_raw.py

# run just the staging transformation
bruin run berlin-weather/assets/staging/weather_daily.sql
```

To launch the dashboard:

```bash
streamlit run berlin-weather/assets/reports/streamlit_app.py
```

## Pipelines

- **ai-price-wars** — AI model pricing vs quality analysis
- **baby-bust** — Global fertility decline vs economic development (World Bank, 217 countries, 1960-2024)
- **berlin-weather** — Historical weather data for Berlin
- **chess-analytics** — Chess game analytics
- **chess-dot-com** — Chess.com game analytics
- **city-pulse** — Urban form analysis: street network fingerprints, building heights, and city design metrics (GHSL + OSMnx, 10K cities + 20 analyzed)
- **contoso** — Contoso sample data
- **epias-energy** — Turkish energy market data (EPIAS)
- **flightradar24** — Flight tracking data
- **ga_sample** — Google Analytics sample data
- **google-takeout** — Google Takeout data analysis
- **google-trends** — Google search trends analysis
- **hormuz-effect** — Strait of Hormuz oil crisis impact on markets (FRED + S&P 500)
- **nyc-taxi** — NYC taxi trip data
- **polymarket-insights** — Prediction market analysis (Polymarket)
- **stackoverflow-trends** — Stack Overflow activity trends (2008-present)
- **stock-market** — S&P 500 stock market data (FMP API)

## Stack

- **[Bruin](https://github.com/bruin-data/bruin)** — Pipeline orchestration, data quality, and materialization
- **BigQuery** — Data warehouse (+ public datasets)
- **Python / Pandas** — Raw data ingestion from APIs
- **SQL** — Staging transformations and aggregations
- **Streamlit / Altair** — Interactive dashboards

## Data Sources

- BigQuery public datasets (Stack Overflow, Google Trends, Google Analytics)
- Chess.com API
- EPIAS (Turkish energy market)
- Flightradar24
- FMP (Financial Modeling Prep) API — S&P 500 stock data
- FRED API — Federal Reserve economic data (oil prices, CPI, unemployment, yield curve)
- GHSL Urban Centre Database (European Commission JRC) — 10K+ global urban centers with population, GDP, building height, climate
- NYC TLC trip record data
- Open-Meteo API
- OpenStreetMap (via OSMnx Overpass API) — street network graphs for urban form analysis
- Polymarket API — prediction market data
- Stack Exchange API
- World Bank Open Data API — development indicators, demographics, economics (217 countries, 1960-2024)
