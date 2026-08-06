# Public Chess.com @peterthiel Archive

An analysis of the public Chess.com history for `peterthiel`, with a focus on
rating-adjusted performance, opening and move patterns, observable play habits,
and evidence-led hidden insights. The public archive begins in December 2016,
so it covers about 9.6 years rather than a complete ten-year window.

## Data sources

- [Chess.com Published Data API](https://www.chess.com/news/view/published-data-api) — public profile, archive index, game records, public ratings, PGN text, clocks, and opening URLs.
- [Chess.com opening taxonomy](https://www.chess.com/openings) — opening labels parsed from Chess.com URLs and PGN headers.
- [Chess.com Public API](https://www.chess.com/news/view/published-data-api) — current public titled-player registries for username-level matching only.

## Warehouse layout

Every project table is materialized in BigQuery dataset
`bruin-playground-arsalan.chess_peterthiel`.

- `raw_…` tables ingest source records.
- `stage_…` tables hold analysis-ready transformations.
- `report_…` tables supply the DAC dashboard.

This project uses four raw assets (`raw_peterthiel_games`, `raw_peterthiel_profile`,
`raw_peterthiel_archives`, and `raw_chess_titled_players`), five stage assets,
and eighteen report assets. Asset names, physical table names, and filenames
all follow the same layer prefix.

## Run locally

```bash
# Validate definitions.
bruin validate peterthiel-chess/

# Load public source records.
bruin run --start-date 2016-08-06 --end-date 2026-08-06 \
  peterthiel-chess/assets/raw/raw_peterthiel_games.asset.yml
bruin run peterthiel-chess/assets/raw/raw_peterthiel_profile.asset.yml
bruin run peterthiel-chess/assets/raw/raw_peterthiel_archives.asset.yml
bruin run peterthiel-chess/assets/raw/raw_chess_titled_players.py

# Build transformations and reports.
bruin run peterthiel-chess/assets/staging/stage_games_enriched.sql
bruin run peterthiel-chess/assets/staging/stage_game_moves.sql
bruin run peterthiel-chess/assets/report/

# Validate, execute, and serve the single three-tab DAC dashboard.
dac validate --dir peterthiel-chess/dashboard-dac
dac check --dir peterthiel-chess/dashboard-dac
dac serve --dir peterthiel-chess/dashboard-dac --port 8321 \
  --template peterthiel-chess/dashboard-dac/themes/wong-dark.yml
# http://localhost:8321
```

The dashboard has three tabs: **Performance & habits**, **Moves & clock**, and
**Hidden insights**. It uses the repository's local DAC additions (`hideName`
and `seriesNames`) for uncluttered, readable charts and tooltips.

## Limitations

- This measures public Chess.com account activity, not a verified person's full chess history.
- Chess.com timestamps are analyzed in UTC; the account holder's local time zone is not inferred.
- Opening, move, and clock signals are descriptive PGN-derived metrics, not a chess-engine evaluation of move quality.
- Only games retained in the public archive are in scope.
- Titled-opponent matches use Chess.com's current public registry and a case-insensitive username match; they do not prove historical title status, account ownership, or offline identity.
