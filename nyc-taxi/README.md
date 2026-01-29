# NYC Taxi Analytics Pipeline

An end-to-end data pipeline that ingests, transforms, and visualizes NYC taxi trip data using Bruin, MotherDuck, and Streamlit.

## Overview

This project processes **~433 million NYC yellow taxi trip records** from January 2020 through November 2025, transforming raw data into actionable insights about tipping behavior, payment patterns, and trip trends across New York City.

### Key Features

- **Automated Data Ingestion**: Downloads parquet files from NYC TLC public datasets
- **Two-Stage Cleaning Pipeline**: Raw ingestion → staging transformation with data quality filters
- **Interactive Dashboard**: Streamlit-powered visualizations including heatmaps and geographic maps
- **Cloud-Native**: Uses MotherDuck (serverless DuckDB) for scalable analytics

## Dashboard Insights

The Streamlit dashboard provides:

- **Tip % Heatmaps**: Tipping patterns by day-of-week × month and day-of-week × hour
- **Borough Flow Analysis**: Average tip percentages for trips between NYC boroughs
- **Payment Type Distribution**: Cash vs credit card usage mapped by pickup zone
- **Low-Tip Rate Maps**: Geographic visualization of where credit card users tip poorly
- **Monthly Totals**: Trip counts, fare sums, and tip rate trends over time

## Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | [Bruin](https://github.com/bruin-data/bruin) |
| Data Warehouse | [MotherDuck](https://motherduck.com/) (DuckDB) |
| Visualization | [Streamlit](https://streamlit.io/) + [Altair](https://altair-viz.github.io/) + [PyDeck](https://pydeck.gl/) |
| Data Source | [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |

## Project Structure

```
nyc_cabs/
├── pipeline.yml              # Bruin pipeline configuration
├── requirements.txt          # Python dependencies
├── .bruin.yml               # Connection credentials (gitignored)
└── assets/
    ├── raw/                  # Data ingestion layer
    │   ├── trips_raw.py      # NYC taxi parquet ingestion
    │   ├── taxi_zone_lookup.sql
    │   ├── taxi_zone_geojson.py
    │   └── payment_lookup.*
    ├── staging/              # Transformation layer
    │   └── trips_summary.sql # Cleaning, normalization, enrichment
    └── reports/              # Analytics & visualization
        ├── streamlit_app.py  # Interactive dashboard
        ├── *.sql             # Report queries
        └── footnotes.md      # Data documentation
```

## Data Pipeline

### Stage 1: Raw Ingestion
- Downloads parquet files from NYC TLC HTTP endpoints
- Loads taxi zone and payment type lookup tables
- Appends records with extraction timestamps

### Stage 2: Staging Transformation
- Normalizes column names across yellow/green taxi types
- Filters invalid records (null times, negative amounts, unreasonable durations)
- Enriches with borough/zone names and payment descriptions
- Deduplicates using composite key + latest extraction

### Stage 3: Reports
- Applies additional data quality filters (distance, speed, fare bounds)
- Aggregates by time periods, boroughs, and zones
- Powers the Streamlit dashboard visualizations

## Running the Dashboard

```bash
# Install dependencies
pip install -r requirements.txt

# Run Streamlit app
streamlit run assets/reports/streamlit_app.py
```

The dashboard connects to MotherDuck using credentials from `.streamlit/secrets.toml` or environment variables.

## Data Quality

- **Raw records ingested**: ~433M rows
- **Records after cleaning**: ~365M rows (~84% retained)
- **Date range**: January 2020 – November 2025

See `assets/reports/footnotes.md` for detailed cleaning criteria.

## Live Demo

🚀 [View the live dashboard](https://nyccabs-cv36qnxfcnvomubqhjn2mf.streamlit.app/)
