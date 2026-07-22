# Bruin Demo Pipelines

This repository is a collection of example [Bruin](https://getbruin.com/) data pipelines. They demonstrate ingestion from public APIs and datasets, transformations in SQL and Python, data-quality checks, and dashboards. Most examples use BigQuery; a few use DuckDB, MotherDuck, or source-specific connections.

Each top-level pipeline directory contains a `pipeline.yml` file and its assets. Read the pipeline-level README before running an example: it documents its data sources, required credentials, and any limitations.

## Quick start

### 1. Install Bruin

```bash
curl -LsSf https://getbruin.com/install/cli | sh
bruin version
```

The official [installation guide](https://getbruin.com/docs/bruin/getting-started/introduction/installation) has platform-specific troubleshooting.

### 2. Configure `.bruin.yml`

Bruin reads connections and secrets from a `.bruin.yml` file at the repository root. The file is gitignored: keep credentials out of version control.

For the BigQuery-based demos, authenticate with Google Application Default Credentials:

```bash
gcloud auth application-default login
```

Then create `.bruin.yml` with a connection name that matches the pipeline's `default_connections` entry. Most BigQuery examples use `bruin-playground-arsalan`:

```yaml
default_environment: default
environments:
  default:
    connections:
      google_cloud_platform:
        - name: bruin-playground-arsalan
          project_id: YOUR_GCP_PROJECT_ID
          location: US
          use_application_default_credentials: true
```

Use the connection name, project, and region appropriate to your environment. To add any new connection interactively, run the Bruin connection wizard; it writes the selected connection to `.bruin.yml`:

```bash
bruin connections add
```

Some pipelines require an additional connection or API secret; follow their own README and the [Bruin connection documentation](https://getbruin.com/docs/bruin/core-concepts/connections.html). Check the configuration before a run:

```bash
bruin connections list
bruin connections test --name bruin-playground-arsalan
```

### 3. Validate and run a pipeline

Install an asset layer's Python dependencies when that pipeline includes a `requirements.txt`, then validate before executing it:

```bash
pip install -r berlin-weather/assets/raw/requirements.txt

# Validate definitions and connection references.
bruin validate berlin-weather/

# Develop with a small, bounded run of one asset.
bruin run --start-date 2024-01-01 --end-date 2024-01-03 \
  berlin-weather/assets/raw/weather_raw.py

# Run a complete pipeline once its prerequisites are configured.
bruin run --start-date 2024-01-01 --end-date 2024-01-07 berlin-weather/
```

Use the same pattern for another example, substituting its directory and asset paths. A dashboard-enabled pipeline can be served locally with Bruin DAC:

```bash
dac validate --dir polymarket-weather/dashboard-dac
dac serve --dir polymarket-weather/dashboard-dac --port 8321
```

Open the dashboard at [http://localhost:8321](http://localhost:8321).

## Pipelines

| Pipeline | Overview |
| --- | --- |
| `ai-economy` | Examines AI adoption, task exposure, and labour-market context using Anthropic and public economic data. |
| `ai-energy-paradox` | Relates AI and data-centre electricity demand to generation, prices, emissions, and EV demand. |
| `ai-price-wars` | Tracks the relationship between AI model pricing, provider offerings, and benchmark quality. |
| `argentina-spain-final` | Analyses the Argentina–Spain football final with match events, expected goals, squads, and head-to-head history. |
| `baby-bust` | Explores long-term fertility decline and its economic and demographic context across countries. |
| `berlin-weather` | Ingests and analyses historical weather observations for Berlin. |
| `bruin-shop` | Demonstrates a multi-source commerce and marketing pipeline using advertising, web analytics, CRM, and ecommerce data. |
| `chess-analytics` | Builds player- and game-level chess performance statistics from Chess.com data. |
| `chess-dot-com` | Analyses Chess.com games, ratings, openings, results, and player activity patterns. |
| `city-pulse` | Compares global cities through urban form, street networks, building heights, and demographic measures. |
| `contoso` | Loads the Contoso sample business dataset into BigQuery for finance, sales, and operational analysis. |
| `contoso-dac` | Presents the Contoso sample business dataset through a Bruin DAC dashboard workflow. |
| `contoso-v2` | Provides a second Contoso sample-data implementation for BigQuery-based analytics. |
| `epias-energy` | Analyses Turkish electricity generation, prices, demand forecasts, weather, and macroeconomic context. |
| `fifa-2026` | Tracks the 2026 FIFA World Cup schedule, teams, venues, travel, heat risk, and prediction markets. |
| `flightradar24` | Summarises flight-tracking data and compares airport-hub activity. |
| `ga_sample` | Provides a minimal Google Analytics sample-data pipeline. |
| `google-takeout` | Analyses a personal Google Takeout search-history export over time. |
| `google-trends` | Studies global and US Google search trends around AI-related terms. |
| `hormuz-effect` | Examines the market and macroeconomic effects of disruption in the Strait of Hormuz. |
| `ingestr-cli-v1` | Demonstrates ingesting order data through the Ingestr CLI. |
| `jose-ingestr` | Demonstrates loading order data into DuckDB through Ingestr assets. |
| `nyc-taxi` | Analyses New York City taxi trips, fares, tips, timing, and zone-level patterns. |
| `pension-crisis` | Assesses population ageing, pension adequacy, and fiscal pressure across OECD countries. |
| `pension-crisis-dac` | Supplies a Bruin DAC dashboard companion for the pension-crisis analysis. |
| `polymarket-insights` | Analyses prediction-market activity, topic trends, price movements, and headline events. |
| `polymarket-weather` | Evaluates Polymarket weather contracts against forecasts and observed station weather. |
| `public-transit` | Provides a starting point for public-transit pipeline development. |
| `public-transit-analysis` | Compares US transit agencies and metro areas on ridership recovery and operating efficiency. |
| `public-transit-hk` | Analyses Hong Kong GTFS and MTR data, including routes, stops, fares, and service patterns. |
| `public-transit-istanbul` | Analyses Istanbul ridership, rail, ferry, traffic, stations, and passenger demographics. |
| `santiago-dac` | Provides a Bruin DAC dashboard example focused on Santiago data. |
| `self-heal-iot` | Demonstrates data-quality and recovery patterns for IoT sensor readings. |
| `self-heal-shop` | Demonstrates self-healing pipeline patterns for orders, products, and revenue. |
| `self-heal-webevents` | Demonstrates self-healing pipeline patterns for web pageview data. |
| `stackoverflow-trends` | Tracks Stack Overflow questions, tags, and activity trends over time. |
| `stock-market` | Loads S&amp;P 500 prices and company financial statements for market analysis. |
| `toronto-crime` | Analyses Toronto crime events by category, neighbourhood, time, and spatial patterns. |
| `tour-de-france` | Tracks Tour de France stage results, general-classification standings, and time gaps. |
| `wikipedia-ai-trends` | Measures the growth and structure of AI-related articles across Wikipedia. |

## More help

Run `bruin --help` or `bruin <command> --help` to see command options. The [Bruin documentation](https://getbruin.com/docs/bruin/) covers assets, connections, scheduling, and deployment.
