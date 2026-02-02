import os
from pathlib import Path
from typing import Optional

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

try:
    import yaml
except ModuleNotFoundError:
    raise SystemExit("Missing dependency 'pyyaml'. Install with: pip install pyyaml")


st.set_page_config(
    page_title="Chess.com Elite Analysis",
    page_icon="♟️",
    layout="wide"
)

st.title("♟️ Chess.com Elite Analysis")
st.caption("200K+ games | 2019-2026 | World Champions, Super GMs & Popular Streamers")

# Data pipeline summary
st.info("""
**Data Stack**: Chess.com API → Bruin (Python/SQL) → MotherDuck (DuckDB) → Streamlit

**Process**: Game data ingested via `ingestr`, transformed with SQL (JSON parsing, deduplication, 
winner calculation), filtered to 20 elite players, and visualized with Altair charts.
""")


def load_token() -> Optional[str]:
    try:
        if "MOTHERDUCK_TOKEN" in st.secrets:
            return st.secrets["MOTHERDUCK_TOKEN"].strip().strip('"')
    except Exception:
        pass
    env_token = os.getenv("MOTHERDUCK_TOKEN") or os.getenv("BRUIN_CONNECTION_MOTHERDUCK_PROD_TOKEN")
    if env_token:
        return env_token.strip().strip('"')
    try:
        root = Path(__file__).resolve().parents[2]
        config_path = root / ".bruin.yml"
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            token_val = (
                cfg.get("environments", {})
                .get("default", {})
                .get("connections", {})
                .get("motherduck", [{}])[0]
                .get("token")
            )
            return token_val.strip().strip('"') if token_val else None
    except Exception:
        return None
    return None


token = load_token()
if not token:
    st.error("MotherDuck token missing. Add MOTHERDUCK_TOKEN to .streamlit/secrets.toml.")
    st.stop()

os.environ["MOTHERDUCK_TOKEN"] = token


@st.cache_resource
def get_conn(md_token: str):
    return duckdb.connect(f"md:?motherduck_token={md_token}")


base_path = Path(__file__).parent


@st.cache_data(show_spinner=False, ttl=60)  # Cache for 60 seconds only
def run_query(filename: str) -> pd.DataFrame:
    sql = (base_path / filename).read_text()
    con = get_conn(token)
    return con.execute(sql).df()


# Load data
with st.spinner("Loading chess data..."):
    top_stats = run_query("top_players_stats.sql")
    win_streaks = run_query("win_streaks.sql")
    how_lose = run_query("how_players_lose.sql")
    time_of_day = run_query("time_of_day_performance.sql")
    openings = run_query("opening_analysis.sql")
    elite_h2h = run_query("elite_h2h.sql")
    biggest_upsets = run_query("biggest_upsets.sql")

# Player display config
player_colors = {
    "Hikaru": "#FFD700", "MagnusCarlsen": "#4169E1", "Philippians46": "#32CD32", 
    "nihalsarin": "#9932CC", "ManuDavid2910": "#FF69B4", "SeanWinshand": "#00CED1", 
    "FabianoCaruana": "#DC143C", "AnishGiri": "#8A2BE2", "Firouzja2003": "#20B2AA", 
    "alireza2003": "#20B2AA", "LevonAronian": "#FF8C00", "GothamChess": "#228B22",
    "francisbegbie": "#8B4513", "Castlecard": "#00FF7F", "JolinTsai": "#FF1493",
    "GutovAndrey": "#4682B4", "ArkadiiKhromaev": "#FF4500", "IanNepomniachtchi": "#1E90FF",
    "AlexandraBotez": "#E91E63", "EricRosen": "#9ACD32"
}
player_display = {
    "Hikaru": "Hikaru", "MagnusCarlsen": "Magnus", "Philippians46": "Philippians", 
    "nihalsarin": "Nihal", "ManuDavid2910": "ManuDavid", "SeanWinshand": "Sean",
    "FabianoCaruana": "Fabiano", "AnishGiri": "Anish", "Firouzja2003": "Alireza",
    "alireza2003": "Alireza", "LevonAronian": "Levon", "GothamChess": "Levy (Gotham)",
    "francisbegbie": "francisbegbie", "Castlecard": "Castlecard", "JolinTsai": "JolinTsai",
    "GutovAndrey": "GutovAndrey", "ArkadiiKhromaev": "Arkadii", "IanNepomniachtchi": "Nepo",
    "AlexandraBotez": "Alexandra Botez", "EricRosen": "Eric Rosen"
}

# ============================================================================
# SECTION 1: SHOCKING STATS HEADER
# ============================================================================
st.markdown("---")
st.subheader("Key Metrics")

col1, col2, col3, col4 = st.columns(4)

# Find top streaker
top_streak = win_streaks.iloc[0]
with col1:
    st.metric(
        "Longest Win Streak",
        f"{int(top_streak['longest_win_streak'])} games",
        f"by {player_display.get(top_streak['player'], top_streak['player'])}"
    )

# Find lowest checkmate rate (among players with 500+ games)
how_lose_significant = how_lose[how_lose['total_games'] >= 500]
lowest_mate = how_lose_significant[how_lose_significant['checkmate_rate'] == how_lose_significant['checkmate_rate'].min()].iloc[0]
with col2:
    st.metric(
        "Lowest Checkmate Rate",
        f"{lowest_mate['checkmate_rate']:.2f}%",
        f"{player_display.get(lowest_mate['player'], lowest_mate['player'])}"
    )

# Top upset
if len(biggest_upsets) > 0:
    top_upset = biggest_upsets.iloc[0]
    with col3:
        st.metric(
            "Biggest Upset Ever",
            f"{int(top_upset['rating_gap'])} pts",
            f"{top_upset['winner']} beat {player_display.get(top_upset['loser'], top_upset['loser'])}"
        )

# Total games
total_games = top_stats['total_games'].sum()
with col4:
    st.metric(
        "Total Games Analyzed",
        f"{int(total_games):,}",
        "2019-2026"
    )

# ============================================================================
# SECTION 2: WIN STREAKS
# ============================================================================
st.markdown("---")
st.subheader("Longest Win Streaks")

with st.expander("ℹ️ What is a win streak?", expanded=False):
    st.markdown("""
    **Win Streak** = Consecutive games won in a row without a single loss or draw.
    
    Maintaining a long win streak at the elite level is extraordinarily difficult!
    """)

win_streaks_display = win_streaks.copy()
win_streaks_display['display_name'] = win_streaks_display['player'].map(lambda x: player_display.get(x, x))

streak_chart = (
    alt.Chart(win_streaks_display)
    .mark_bar(cornerRadiusTopRight=8, cornerRadiusTopLeft=8)
    .encode(
        x=alt.X('display_name:N', title='Player', sort='-y'),
        y=alt.Y('longest_win_streak:Q', title='Longest Win Streak'),
        color=alt.Color(
            'player:N',
            scale=alt.Scale(domain=list(player_colors.keys()), range=list(player_colors.values())),
            legend=None
        ),
        tooltip=[
            alt.Tooltip('display_name', title='Player'),
            alt.Tooltip('longest_win_streak', title='Win Streak'),
        ]
    )
    .properties(height=350)
)

text = streak_chart.mark_text(align='center', baseline='bottom', dy=-5, fontSize=14, fontWeight='bold').encode(
    text='longest_win_streak:Q'
)

st.altair_chart(streak_chart + text, use_container_width=True)

st.caption(f"Longest streak: {player_display.get(top_streak['player'], top_streak['player'])} with {int(top_streak['longest_win_streak'])} consecutive wins")

# ============================================================================
# SECTION 3: ELITE HEAD-TO-HEAD
# ============================================================================
st.markdown("---")
st.subheader("Head-to-Head Records")

with st.expander("ℹ️ What does this show?", expanded=False):
    st.markdown("""
    Direct matchup records between the world's best players. 
    These are real games played online on Chess.com!
    """)

# Show top rivalries as grouped bar chart
if len(elite_h2h) > 0:
    elite_h2h_display = elite_h2h.head(10).copy()
    elite_h2h_display['player1_display'] = elite_h2h_display['player1'].map(lambda x: player_display.get(x, x))
    elite_h2h_display['player2_display'] = elite_h2h_display['player2'].map(lambda x: player_display.get(x, x))
    elite_h2h_display['matchup'] = elite_h2h_display['player1_display'] + ' vs ' + elite_h2h_display['player2_display']
    
    # Prepare data for grouped bar chart
    h2h_long = []
    for _, row in elite_h2h_display.iterrows():
        h2h_long.append({
            'matchup': row['matchup'], 
            'result': f"{row['player1_display']} wins", 
            'count': int(row['p1_wins']),
            'color_key': 'p1'
        })
        h2h_long.append({
            'matchup': row['matchup'], 
            'result': f"{row['player2_display']} wins", 
            'count': int(row['p2_wins']),
            'color_key': 'p2'
        })
        h2h_long.append({
            'matchup': row['matchup'], 
            'result': 'Draws', 
            'count': int(row['draws']),
            'color_key': 'draw'
        })
    h2h_df = pd.DataFrame(h2h_long)
    
    h2h_chart = (
        alt.Chart(h2h_df)
        .mark_bar(cornerRadius=3)
        .encode(
            y=alt.Y('matchup:N', title=None, sort=elite_h2h_display['matchup'].tolist()),
            x=alt.X('count:Q', title='Games', stack=None),
            color=alt.Color('color_key:N', 
                scale=alt.Scale(domain=['p1', 'p2', 'draw'], range=['#4CAF50', '#F44336', '#9E9E9E']),
                legend=alt.Legend(title="Result", labelExpr="datum.value === 'p1' ? 'Player 1 Wins' : datum.value === 'p2' ? 'Player 2 Wins' : 'Draws'")
            ),
            yOffset='color_key:N',
            tooltip=[
                alt.Tooltip('matchup', title='Matchup'),
                alt.Tooltip('result', title='Result'),
                alt.Tooltip('count', title='Games'),
            ]
        )
        .properties(height=350)
    )
    st.altair_chart(h2h_chart, use_container_width=True)
    
    # Find a notable matchup for insight
    top_matchup = elite_h2h_display.iloc[0]
    st.caption(f"Most played: {top_matchup['player1_display']} vs {top_matchup['player2_display']} — {int(top_matchup['p1_wins'])}-{int(top_matchup['p2_wins'])} ({int(top_matchup['draws'])} draws) over {int(top_matchup['total_games'])} games")
else:
    st.warning("No head-to-head data available.")

# ============================================================================
# SECTION 5: PLAYER STATISTICS OVERVIEW
# ============================================================================
st.markdown("---")
st.subheader("Player Statistics Overview")

top_stats_display = top_stats.copy()
top_stats_display["display_name"] = top_stats_display["player"].map(lambda x: player_display.get(x, x))

# Dynamic axis ranges based on data
x_min = top_stats_display['total_games'].min() * 0.8
x_max = top_stats_display['total_games'].max() * 1.2
y_min = max(0, top_stats_display['win_rate'].min() - 5)
y_max = min(100, top_stats_display['win_rate'].max() + 5)

# Size scale based on peak rating range
rating_min = top_stats_display['peak_rating'].min()
rating_max = top_stats_display['peak_rating'].max()

scatter_chart = (
    alt.Chart(top_stats_display)
    .mark_circle(opacity=0.8)
    .encode(
        x=alt.X("total_games:Q", title="Total Games Played", scale=alt.Scale(type="log", domain=[x_min, x_max])),
        y=alt.Y("win_rate:Q", title="Win Rate (%)", scale=alt.Scale(domain=[y_min, y_max])),
        color=alt.Color(
            "player:N",
            scale=alt.Scale(domain=list(player_colors.keys()), range=list(player_colors.values())),
            legend=None
        ),
        size=alt.Size(
            "peak_rating:Q", 
            scale=alt.Scale(domain=[rating_min, rating_max], range=[100, 800]), 
            legend=alt.Legend(title="Peak Rating")
        ),
        tooltip=[
            alt.Tooltip("display_name", title="Player"),
            alt.Tooltip("win_rate", title="Win Rate %", format=".1f"),
            alt.Tooltip("total_games", title="Games", format=","),
            alt.Tooltip("peak_rating", title="Peak Rating"),
        ],
    )
    .properties(height=400)
)

text_labels = (
    alt.Chart(top_stats_display)
    .mark_text(dy=-15, fontSize=10, fontWeight="bold", color="white")
    .encode(
        x=alt.X("total_games:Q", scale=alt.Scale(type="log")),
        y=alt.Y("win_rate:Q"),
        text="display_name:N",
    )
)

st.altair_chart(scatter_chart + text_labels, use_container_width=True)

# Stats table
stats_table = top_stats[["player", "total_games", "wins", "losses", "draws", "win_rate", "peak_rating"]].copy()
stats_table["player"] = stats_table["player"].map(lambda x: player_display.get(x, x))
stats_table.columns = ["Player", "Games", "Wins", "Losses", "Draws", "Win %", "Peak Rating"]
st.dataframe(stats_table, hide_index=True, use_container_width=True)

# ============================================================================
# SECTION 7: TIME OF DAY PERFORMANCE
# ============================================================================
st.markdown("---")
st.subheader("Win Rate by Hour of Day")
st.caption("Showing top 5 players with the most variation in win rate across hours (highest standard deviation)")

# Filter to top 5 players with most variation (std dev) in win rate across hours
player_stddev = time_of_day.groupby('player')['win_rate'].std().reset_index()
player_stddev.columns = ['player', 'win_rate_std']
top_5_variable = player_stddev.nlargest(5, 'win_rate_std')['player'].tolist()

time_filtered = time_of_day[time_of_day['player'].isin(top_5_variable)]
time_filtered_display = time_filtered.copy()
time_filtered_display['display_name'] = time_filtered_display['player'].map(lambda x: player_display.get(x, x))

if len(time_filtered_display) > 0:
    min_rate = time_filtered_display['win_rate'].min()
    max_rate = time_filtered_display['win_rate'].max()
    y_min = max(0, min_rate - 5)
    y_max = min(100, max_rate + 5)
    
    # Build color scale for only filtered players
    filtered_colors = {p: player_colors.get(p, '#888888') for p in top_5_variable}

    time_chart = (
        alt.Chart(time_filtered_display)
        .mark_line(point=True, strokeWidth=2)
        .encode(
            x=alt.X('hour:O', title='Hour (UTC)', axis=alt.Axis(labelAngle=0)),
            y=alt.Y('win_rate:Q', title='Win Rate (%)', scale=alt.Scale(domain=[y_min, y_max], zero=False)),
            color=alt.Color(
                'display_name:N',
                scale=alt.Scale(
                    domain=[player_display.get(p, p) for p in top_5_variable],
                    range=[filtered_colors[p] for p in top_5_variable]
                ),
                legend=alt.Legend(title="Player", orient="bottom")
            ),
            tooltip=[
                alt.Tooltip('display_name', title='Player'),
                alt.Tooltip('hour', title='Hour (UTC)'),
                alt.Tooltip('win_rate', title='Win Rate %', format='.1f'),
                alt.Tooltip('games', title='Games'),
            ]
        )
        .properties(height=400)
    )

    st.altair_chart(time_chart, use_container_width=True)

# ============================================================================
# SECTION 8: BIGGEST UPSETS (moved to bottom)
# ============================================================================
st.markdown("---")
st.subheader("Biggest Rating Upsets")

with st.expander("ℹ️ What is an upset?", expanded=False):
    st.markdown("""
    **An Upset** occurs when a lower-rated player defeats a higher-rated player.
    
    A 500+ point rating gap means the higher player is expected to win ~85% of the time.
    These upsets defied the odds!
    """)

if len(biggest_upsets) > 0:
    upsets_display = biggest_upsets.head(10).copy()
    upsets_display['loser_display'] = upsets_display['loser'].map(lambda x: player_display.get(x, x))
    upsets_display['label'] = upsets_display['winner'] + ' beat ' + upsets_display['loser_display']
    upsets_display['winner_rating_int'] = upsets_display['winner_rating'].astype(int)
    upsets_display['loser_rating_int'] = upsets_display['loser_rating'].astype(int)
    
    # Lollipop chart showing rating gap - use columns to make it narrower
    col_chart, col_space = st.columns([2, 1])
    
    with col_chart:
        base = alt.Chart(upsets_display).encode(
            y=alt.Y('label:N', title=None, sort=alt.EncodingSortField(field='rating_gap', order='descending'))
        )
        
        # Lines from winner rating to loser rating
        upset_lines = base.mark_rule(strokeWidth=3, color='#FF6B6B').encode(
            x=alt.X('winner_rating_int:Q', title='Rating', scale=alt.Scale(domain=[0, 3500])),
            x2='loser_rating_int:Q',
            tooltip=[
                alt.Tooltip('winner', title='Giant Killer'),
                alt.Tooltip('winner_rating_int', title='Winner Rating'),
                alt.Tooltip('loser_display', title='Victim'),
                alt.Tooltip('loser_rating_int', title='Victim Rating'),
                alt.Tooltip('rating_gap', title='Rating Gap'),
                alt.Tooltip('time_class', title='Format'),
            ]
        )
        
        # Points for winner (lower rating)
        winner_points = base.mark_circle(size=100, color='#4CAF50').encode(
            x='winner_rating_int:Q',
            tooltip=[alt.Tooltip('winner', title='Winner'), alt.Tooltip('winner_rating_int', title='Rating')]
        )
        
        # Points for loser (higher rating)  
        loser_points = base.mark_circle(size=100, color='#F44336').encode(
            x='loser_rating_int:Q',
            tooltip=[alt.Tooltip('loser_display', title='Loser'), alt.Tooltip('loser_rating_int', title='Rating')]
        )
        
        upset_chart = (upset_lines + winner_points + loser_points).properties(height=350)
        st.altair_chart(upset_chart, use_container_width=True)
        
        st.caption("🟢 Green = Winner (lower rated) | 🔴 Red = Loser (higher rated)")
    
    st.caption(f"Largest upset: {int(biggest_upsets.iloc[0]['winner_rating'])}-rated player defeated {player_display.get(biggest_upsets.iloc[0]['loser'], biggest_upsets.iloc[0]['loser'])} ({int(biggest_upsets.iloc[0]['loser_rating'])}) — {int(biggest_upsets.iloc[0]['rating_gap'])} point difference")

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================
st.markdown("---")
st.subheader("Summary")

st.markdown("""
### Notable Findings

**Win Streaks**
- francisbegbie: 257 consecutive wins (longest in dataset)
- Hikaru: 101-game streak across 44K+ total games

**Checkmate Rates**
- Super GMs (Magnus, Fabiano): 1-2% — typically resign before checkmate
- Streamers (GothamChess, Botez): 4-5%
- Casual players: 7-10%+

**Head-to-Head**
- Magnus vs Fabiano online: Significant advantage to Magnus
- Streamers vs Super GMs: Large skill gap evident in matchup records

**Upsets**
- Rating gaps of 500+ points occasionally overcome
- Lower-rated players have beaten titled players in bullet/blitz formats
""")

st.markdown("---")

# Load and display footnotes
with st.expander("📋 Methodology & Data Notes", expanded=False):
    try:
        with open("footnotes.md", "r") as f:
            footnotes_content = f.read()
        st.markdown(footnotes_content)
    except FileNotFoundError:
        st.markdown("""
        **Data Source**: Chess.com API via Bruin data pipeline
        
        **Period**: January 2019 - February 2026
        
        **Processing**: Games enriched from raw JSON, deduplicated by game URL
        
        **Calculations**: Win streaks use gaps-and-islands algorithm; checkmate rate = checkmated / total games
        """)

st.caption("Data: Chess.com API via Bruin | Period: 2019-2026 | Database: MotherDuck | 200K+ games analyzed")
