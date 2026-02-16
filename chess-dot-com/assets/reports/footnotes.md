## Data Notes

### Source
- **API**: Chess.com Public API via Bruin data pipeline (ingestr)
- **Database**: MotherDuck (DuckDB)
- **Players Tracked**: 16 elite players (Super GMs + popular streamers)

### Players Analyzed

**Super GMs:**
- Hikaru Nakamura, Fabiano Caruana, Ian Nepomniachtchi
- Alireza Firouzja, Anish Giri, Levon Aronian
- Praggnanandhaa, Nihal Sarin, Daniel Naroditsky
- Hans Niemann, Wesley So

**Popular Streamers:**
- GothamChess (Levy Rozman), Alexandra Botez (BotezLive)
- Eric Rosen, Eric Hansen (chessbrah), Anna Cramling

---

## Data Pipeline & Transformations

### Stage 1: Raw Data Ingestion
- **Source**: Chess.com API via `ingestr` library
- **Tables Created**: `raw.player_games`, `raw.player_profiles`
- **Data Format**: JSON fields for player info (white/black objects)
- **Strategy**: Replace (full reload each run)

### Stage 2: Games Enriched (`staging.games_enriched`)
Transforms raw game data into analytics-ready format:

| Transformation | Description |
|----------------|-------------|
| JSON Extraction | Extracts `username`, `rating`, `result` from white/black JSON objects |
| Winner Calculation | Determines winner based on result field |
| Rating Difference | `white_rating - black_rating` (positive = white higher rated) |
| Timestamp Parsing | Converts `end_time` to proper TIMESTAMP type |
| Deduplication | Uses `ROW_NUMBER() OVER (PARTITION BY game_url)` to remove duplicate entries |

### Stage 3: Player Stats (`staging.player_stats`)
Aggregates per-player statistics from enriched game data.

---

## Report Calculations

### Player Overview
**Aggregations per player**:
- `total_games`: COUNT of all games (as white + as black)
- `win_rate`: `100 * wins / total_games`
- `peak_rating`: `MAX(rating)` across all games
- `white_win_rate` / `black_win_rate`: Win rate by piece color
- **Filter**: Minimum 5 games to appear

### Rating Evolution
- Groups games by player, time_class, and date
- `closing_rating`: Last rating of each day (via window function)
- `daily_high` / `daily_low`: Max and min rating each day
- Rating volatility = `daily_high - daily_low`

### Head-to-Head
**Matchup Normalization**: Uses `LEAST/GREATEST` for consistent player ordering
- `player1 = LEAST(white_username, black_username)`
- `player2 = GREATEST(white_username, black_username)`
- **Filter**: Minimum 3 games between players

### Format Kings
- Groups stats by player and `time_class` (bullet/blitz/rapid)
- Win rate calculated per format
- **Filter**: Minimum 3 games per format

### Streaks & Tilts
**Algorithm**: Gaps-and-islands technique for consecutive results
1. Orders all games by player and `end_time`
2. Assigns group IDs: `ROW_NUMBER() - ROW_NUMBER() PARTITION BY result`
3. Counts consecutive results within each group
4. Returns MAX win streak and MAX loss streak per player
5. Also counts "hot runs" (3+ wins) and "tilt runs" (3+ losses)

### Activity Patterns (Night Owl Index)
- Extracts hour (UTC) and day of week from `end_time`
- Counts games and calculates win rate per hour/day bucket

### How They Lose
**Loss Type Breakdown**:
- Resigned: `result = 'resigned'`
- Timed Out: `result = 'timeout'`
- Checkmated: `result = 'checkmated'`
- Abandoned: `result = 'abandoned'`
- Percentages calculated relative to total losses
- **Filter**: Minimum 2 losses to appear

### Biggest Upsets
**Definition**: Lower-rated player beats higher-rated player by 100+ points
- Rating gap = `ABS(white_rating - black_rating)`
- Shows top 25 upsets ordered by gap size

### Streamers vs GMs
- Players categorized as "Streamer" or "Super GM" based on username
- Compares aggregate stats: win rate, avg rating, loss patterns
- **Filter**: Minimum 3 games per player

---

## Key Definitions

| Term | Definition |
|------|------------|
| **Win Streak** | Consecutive games won without a loss or draw |
| **Loss Streak** | Consecutive games lost without a win or draw |
| **Peak Rating** | Highest rating achieved in the dataset period |
| **Upset** | When a lower-rated player beats a higher-rated player |
| **Rating Gap** | Absolute difference in rating between two players |
| **Decisive Game** | Game with a winner (not a draw) |
| **Time Control** | Format like "180+2" meaning 3 minutes + 2 second increment |
| **Closing Rating** | Player's rating at the end of their last game that day |

---

## Data Quality Notes
- Games counted from both white and black perspectives (a game between two tracked players counts once per player)
- Win rates calculated on all games (includes draws in denominator)
- Time zones are UTC (Chess.com server time)
- Player profiles are from Chess.com public API; some accounts may have different display names
- Rating data is per-game snapshot, not official FIDE ratings
