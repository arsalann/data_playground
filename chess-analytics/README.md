# Chess Analytics Pipeline

An end-to-end data pipeline that ingests, transforms, and visualizes Chess.com player data using **Bruin**, **MotherDuck**, and **Rill Data**.

## Overview

This project tracks Chess.com games and profiles for configurable players, transforming raw API data into actionable insights about playing patterns, win rates, and head-to-head performance.

### Key Features

- **Automated Data Ingestion**: Uses Bruin's Chess.com ingestr connector (no API key required)
- **Two-Stage Transformation**: Raw ingestion → staging enrichment with derived metrics
- **Interactive Dashboards**: Rill Data-powered analytics with semantic metrics layer
- **Cloud-Native**: Uses MotherDuck (serverless DuckDB) for scalable analytics

## Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | [Bruin](https://github.com/bruin-data/bruin) |
| Data Warehouse | [MotherDuck](https://motherduck.com/) (DuckDB) |
| Data Source | [Chess.com Public API](https://www.chess.com/news/view/published-data-api) |
| Visualization | [Rill Data](https://docs.rilldata.com/) |

## Project Structure

```
chess-analytics/
├── pipeline.yml              # Bruin pipeline configuration
├── README.md                 # This file
├── assets/                   # Bruin data pipeline
│   ├── raw/                  # Data ingestion layer
│   │   ├── player_profiles.asset.yml
│   │   └── player_games.asset.yml
│   ├── staging/              # Transformation layer
│   │   ├── games_enriched.sql
│   │   └── player_stats.sql
│   └── reports/              # SQL views for analytics
│       ├── games_by_time_class.sql
│       ├── games_by_hour.sql
│       ├── player_matchups.sql
│       ├── rating_performance.sql
│       ├── result_types.sql
│       └── footnotes.md
└── rill/                     # Rill Data dashboards
    ├── rill.yaml             # Rill project config
    ├── connectors/
    │   └── motherduck.yaml   # MotherDuck connection
    ├── sources/              # Data sources from MotherDuck
    │   ├── player_stats.yaml
    │   └── games_enriched.yaml
    ├── models/               # Rill models
    │   └── games_with_time.sql
    └── dashboards/           # Metrics views
        ├── chess_games.yaml
        ├── player_performance.yaml
        └── time_analysis.yaml
```

## Setup

### Prerequisites

1. [Bruin CLI](https://github.com/bruin-data/bruin) installed
2. [MotherDuck](https://motherduck.com/) account with API token
3. [Rill Data](https://docs.rilldata.com/developers/get-started/install) installed:
   ```bash
   curl https://rill.sh | sh
   ```

### Step 1: Configure Connections

Ensure your `.bruin.yml` (in workspace root) has:

```yaml
environments:
  default:
    connections:
      motherduck:
        - name: "motherduck-prod"
          token: "your_motherduck_token"
          database: "my_db"
      chess:
        - name: "chess-players"
          players:
            - "MagnusCarlsen"
            - "Hikaru"
            - "GothamChess"
            - "DanielNaroditsky"
```

### Step 2: Run the Bruin Pipeline

```bash
cd chess-analytics

# Validate the pipeline
bruin validate . --fast

# Run the full pipeline (ingests all games for tracked players)
bruin run .
```

### Step 3: Launch Rill Dashboard

```bash
# Set MotherDuck token for Rill
export MOTHERDUCK_TOKEN="your_motherduck_token"

# Start Rill developer
cd rill
rill start
```

This opens the Rill dashboard at http://localhost:9009 with:
- **Chess Games Analytics**: Game patterns, outcomes, time-of-day analysis
- **Player Performance**: Win rates, games played, white vs black performance
- **Time Control Analysis**: Bullet vs blitz vs rapid patterns

## Rill Dashboard Features

### Chess Games Analytics
- Total games, wins, draws breakdown
- White win rate vs black win rate
- Filter by time class (bullet/blitz/rapid)
- Time series of games played

### Player Performance
- Player-level statistics
- Win rate comparison
- Games as white vs games as black

### Time Control Analysis
- Performance differences across time controls
- Decisive game percentage by format
- White's first-move advantage by format

## Data Pipeline

### Stage 1: Raw Ingestion (Bruin ingestr)
- Connects to Chess.com public API
- Downloads player profiles and all games
- Stores raw data in MotherDuck

### Stage 2: Staging Transformation (Bruin SQL)
- Parses game fields, calculates derived metrics
- Aggregates player statistics
- Deduplicates and enriches data

### Stage 3: Visualization (Rill Data)
- Connects to MotherDuck tables
- Provides semantic metrics layer
- Auto-generated dashboards with filters

## Adding Players

Edit `.bruin.yml` to add more players:

```yaml
chess:
  - name: "chess-players"
    players:
      - "MagnusCarlsen"
      - "Hikaru"
      - "YourFavoritePlayer"
```

Then re-run: `bruin run .`

## References

- [Rill Data Docs](https://docs.rilldata.com/)
- [Bruin CLI Docs](https://getbruin.com/docs/bruin/)
- [MotherDuck Docs](https://motherduck.com/docs/)
- [Chess.com API](https://www.chess.com/news/view/published-data-api)
