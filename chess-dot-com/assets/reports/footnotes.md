## Data Notes

### Source
- **API**: Chess.com Public API via Bruin data pipeline
- **Period**: January 2019 - February 2026
- **Total Games**: 200,000+
- **Database**: MotherDuck (DuckDB)

### Players Analyzed

**World Champions & Super GMs:**
- Magnus Carlsen, Hikaru Nakamura, Fabiano Caruana
- Alireza Firouzja, Anish Giri, Levon Aronian, Nihal Sarin

**Popular Streamers:**
- GothamChess (Levy Rozman), Alexandra Botez, Eric Rosen

**Top Rated Players (by category):**
- Philippians46, ManuDavid2910, SeanWinshand, ArkadiiKhromaev
- francisbegbie, Castlecard, JolinTsai, GutovAndrey

---

## Data Pipeline & Transformations

### Stage 1: Raw Data Ingestion
- **Source**: Chess.com API via `ingestr` library
- **Tables Created**: `raw.player_games`, `raw.player_profiles`
- **Data Format**: JSON fields for player info (white/black objects)

### Stage 2: Games Enriched (`staging.games_enriched`)
Transforms raw game data into analytics-ready format:

| Transformation | Description |
|----------------|-------------|
| JSON Extraction | Extracts `username`, `rating`, `result` from white/black JSON objects |
| Winner Calculation | `CASE WHEN white_result = 'win' THEN white_username WHEN black_result = 'win' THEN black_username ELSE NULL END` |
| Rating Difference | `white_rating - black_rating` (positive = white higher rated) |
| Timestamp Parsing | Converts `end_time` to proper TIMESTAMP type |
| Deduplication | Uses `ROW_NUMBER() OVER (PARTITION BY game_url)` to remove duplicate entries |

---

## Report Calculations

### Win Streaks
**Algorithm**: Uses a gaps-and-islands technique to find consecutive wins
1. Orders all games by player and `end_time`
2. Assigns group IDs using difference of row numbers: `ROW_NUMBER() - ROW_NUMBER() PARTITION BY won`
3. Counts consecutive wins within each group
4. Returns `MAX(streak_length)` per player

### Checkmate Immunity (How Players Lose)
**Formula**: `checkmate_rate = (times_checkmated / total_games) × 100`

| Metric | Calculation |
|--------|-------------|
| Lost on Time | `SUM(CASE WHEN lost AND result = 'timeout')` |
| Resigned | `SUM(CASE WHEN lost AND result = 'resigned')` |
| Checkmated | `SUM(CASE WHEN lost AND result = 'checkmated')` |

### Top Player Statistics
**Aggregations per player**:
- `total_games`: COUNT of all games (as white + as black)
- `win_rate`: `100 × wins / total_games`
- `peak_rating`: `MAX(rating)` across all games
- `white_win_rate`: Win rate when playing as white
- `black_win_rate`: Win rate when playing as black

### Time of Day Performance
**Calculation**: Win rate by hour (UTC)
1. Extracts hour from `end_time`: `EXTRACT(hour FROM end_time)`
2. Calculates win rate on decisive games only: `SUM(won) / SUM(decisive)`
3. **Filter**: Requires minimum 30 games per hour bucket
4. **Dashboard Filter**: Shows top 5 players with highest standard deviation in hourly win rate

### Elite Head-to-Head
**Matchup Normalization**: Uses `LEAST/GREATEST` to ensure consistent player ordering
- `player1 = LEAST(white_username, black_username)`
- `player2 = GREATEST(white_username, black_username)`
- **Filter**: Requires minimum 5 games between players

### Biggest Upsets
**Definition**: Lower-rated player beats higher-rated player by 500+ points
**Filter Logic**:
```sql
(winner = white_username AND white_rating < black_rating - 500)
OR 
(winner = black_username AND black_rating < white_rating - 500)
```

### Opening Analysis
**ECO Extraction**: Parses opening name from Chess.com ECO URL
```sql
SPLIT_PART(REPLACE(REGEXP_EXTRACT(eco, '/openings/([^?]+)', 1), '-', ' '), ' ', 1) || ' ' ||
SPLIT_PART(..., ' ', 2)
```
- **Filter**: Requires minimum 100 games per opening

### Fastest Checkmates
**Move Count Approximation**: Counts move numbers in PGN
```sql
(LENGTH(pgn) - LENGTH(REPLACE(pgn, '. ', ''))) / 2
```
- **Filter**: Games ending in checkmate with ≤25 moves

---

## Key Definitions

| Term | Definition |
|------|------------|
| **Win Streak** | Consecutive games won without a loss or draw |
| **Checkmate Rate** | Percentage of total games lost by checkmate |
| **Peak Rating** | Highest rating achieved in the dataset period |
| **Upset** | When a lower-rated player beats a higher-rated player |
| **Rating Gap** | Absolute difference in rating between two players |
| **Decisive Game** | Game with a winner (not a draw) |
| **Time Control** | Format like "180+2" meaning 3 minutes + 2 second increment |

---

## Filters Applied

| Report Section | Filter |
|----------------|--------|
| All Reports | Player whitelist (20 specific players) |
| Time of Day | Min 30 games per hour; top 5 by std dev |
| Elite H2H | Min 5 games between players |
| Opening Analysis | Min 100 games per opening |
| Biggest Upsets | Rating gap > 500 points |
| Fastest Checkmates | Move count ≤ 25 |

---

## Data Quality Notes
- Games counted from both white and black perspectives (doubles game count per player)
- Win rates calculated on all games (includes draws in denominator)
- Time zones are UTC (Chess.com server time)
- Some short games may be abandoned/disconnected
- PGN move count is approximate (regex-based)
- Opening names truncated to first 2 words for grouping
