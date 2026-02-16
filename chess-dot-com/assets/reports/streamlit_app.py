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


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Chess.com Elite Analysis",
    page_icon="♟️",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Player metadata: colors & display names (dynamic - adapts to data)
# ---------------------------------------------------------------------------
PLAYER_COLORS = {
    # Tracked players (using usernames as they appear in game data)
    "Hikaru": "#FFD700",
    "MagnusCarlsen": "#4169E1",
    "lachesisQ": "#1E90FF",
    "Firouzja2003": "#20B2AA",
    "IMRosen": "#9ACD32",
    "nihalsarin": "#9932CC",
    "HansOnTwitch": "#FF4500",
    "annacramling": "#FF69B4",
    "GMWSO": "#228B22",
    "AlexandraBotez": "#E91E63",
    "FabianoCaruana": "#DC143C",
    "AnishGiri": "#8A2BE2",
    "LevonAronian": "#FF8C00",
    "rpragchess": "#00CED1",
    "DanielNaroditsky": "#4682B4",
    "GothamChess": "#32CD32",
    "chessbrah": "#00FF7F",
    "AnnaCramling": "#FF69B4",
    # Common opponents
    "DenLaz": "#C0C0C0",
    "Sina-Movahed": "#A9A9A9",
    "amintabatabaei": "#808080",
    "Twitch_ElhamBlitz05": "#696969",
    "Sajid0987654321": "#778899",
}

PLAYER_DISPLAY = {
    "Hikaru": "Hikaru Nakamura",
    "MagnusCarlsen": "Magnus Carlsen",
    "lachesisQ": "Ian Nepomniachtchi",
    "Firouzja2003": "Alireza Firouzja",
    "IMRosen": "Eric Rosen",
    "nihalsarin": "Nihal Sarin",
    "HansOnTwitch": "Hans Niemann",
    "annacramling": "Anna Cramling",
    "GMWSO": "Wesley So",
    "AlexandraBotez": "Alexandra Botez",
    "FabianoCaruana": "Fabiano Caruana",
    "AnishGiri": "Anish Giri",
    "LevonAronian": "Levon Aronian",
    "rpragchess": "Praggnanandhaa",
    "DanielNaroditsky": "Daniel Naroditsky",
    "GothamChess": "Levy (GothamChess)",
    "chessbrah": "Eric Hansen",
    "AnnaCramling": "Anna Cramling",
    # Common opponents
    "DenLaz": "Denis Lazavik",
    "Sina-Movahed": "Sina Movahed",
    "amintabatabaei": "Amin Tabatabaei",
}

STREAMERS = {"gothamchess", "alexandrabotez", "imrosen", "chessbrah", "annacramling"}


def display_name(username: str) -> str:
    return PLAYER_DISPLAY.get(username, username)


def player_color(username: str) -> str:
    return PLAYER_COLORS.get(username, "#888888")


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------
def load_token() -> Optional[str]:
    try:
        if "MOTHERDUCK_TOKEN" in st.secrets:
            return st.secrets["MOTHERDUCK_TOKEN"].strip().strip('"')
    except Exception:
        pass
    env_token = os.getenv("MOTHERDUCK_TOKEN") or os.getenv(
        "BRUIN_CONNECTION_MOTHERDUCK_PROD_TOKEN"
    )
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
    st.error(
        "MotherDuck token missing. Add MOTHERDUCK_TOKEN to .streamlit/secrets.toml."
    )
    st.stop()

os.environ["MOTHERDUCK_TOKEN"] = token


@st.cache_resource
def get_conn(md_token: str):
    return duckdb.connect(f"md:?motherduck_token={md_token}")


base_path = Path(__file__).parent


@st.cache_data(show_spinner=False, ttl=120)
def run_query(filename: str) -> pd.DataFrame:
    sql = (base_path / filename).read_text()
    con = get_conn(token)
    return con.execute(sql).df()


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
with st.spinner("Loading chess data..."):
    overview = run_query("player_overview.sql")
    rating_evo = run_query("rating_evolution.sql")
    h2h = run_query("head_to_head.sql")
    format_kings = run_query("format_kings.sql")
    streaks = run_query("streaks_and_tilts.sql")
    activity = run_query("activity_patterns.sql")
    how_lose = run_query("how_they_lose.sql")
    upsets = run_query("biggest_upsets.sql")
    svgm = run_query("streamer_vs_gm.sql")

# Add display names everywhere
for df in [overview, streaks, how_lose, svgm]:
    if "player" in df.columns:
        df["display_name"] = df["player"].map(display_name)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("♟️ Chess.com Elite Player Analysis")

# Date range from data
if not overview.empty:
    first = overview["first_game"].min()
    last = overview["last_game"].max()
    total_g = int(overview["total_games"].sum())
    n_players = len(overview)
    first_str = str(first)[:10] if first else "?"
    last_str = str(last)[:10] if last else "?"
    st.caption(
        f"{total_g:,} games analyzed | {n_players} active players | {first_str} to {last_str}"
    )

st.info(
    "**Data Stack**: Chess.com API -> Bruin (ingestr) -> MotherDuck (DuckDB) -> Streamlit\n\n"
    "Games are ingested from Chess.com, transformed with SQL (JSON parsing, deduplication, "
    "winner calculation), and visualized with Altair charts."
)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
(
    tab_overview,
    tab_rating,
    tab_h2h,
    tab_format,
    tab_streaks,
    tab_activity,
    tab_lose,
    tab_upsets,
    tab_svgm,
) = st.tabs(
    [
        "Overview",
        "Rating Wars",
        "Head-to-Head",
        "Format Kings",
        "Streaks & Tilts",
        "Night Owls",
        "How They Lose",
        "Biggest Upsets",
        "Streamers vs GMs",
    ]
)

# ====================== TAB 1: OVERVIEW ======================
with tab_overview:
    st.subheader("Player Overview")

    if not overview.empty:
        # Key metrics row
        top = overview.iloc[0]
        best_wr = overview.loc[overview["win_rate"].idxmax()]
        highest_rated = overview.loc[overview["peak_rating"].idxmax()]

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Most Active", display_name(top["player"]), f"{int(top['total_games'])} games")
        with c2:
            st.metric(
                "Highest Win Rate",
                f"{best_wr['win_rate']:.1f}%",
                display_name(best_wr["player"]),
            )
        with c3:
            st.metric(
                "Peak Rating",
                f"{int(highest_rated['peak_rating'])}",
                display_name(highest_rated["player"]),
            )
        with c4:
            st.metric("Total Games", f"{total_g:,}")

        st.markdown("---")

        # Scatter: games vs win rate, sized by peak rating
        ov = overview.copy()
        ov["display_name"] = ov["player"].map(display_name)

        scatter = (
            alt.Chart(ov)
            .mark_circle(opacity=0.85)
            .encode(
                x=alt.X("total_games:Q", title="Total Games", scale=alt.Scale(type="log")),
                y=alt.Y("win_rate:Q", title="Win Rate (%)", scale=alt.Scale(zero=False)),
                color=alt.Color(
                    "player:N",
                    scale=alt.Scale(
                        domain=ov["player"].tolist(),
                        range=[player_color(p) for p in ov["player"]],
                    ),
                    legend=None,
                ),
                size=alt.Size(
                    "peak_rating:Q",
                    scale=alt.Scale(range=[150, 900]),
                    legend=alt.Legend(title="Peak Rating"),
                ),
                tooltip=[
                    alt.Tooltip("display_name", title="Player"),
                    alt.Tooltip("total_games", title="Games"),
                    alt.Tooltip("win_rate", title="Win %", format=".1f"),
                    alt.Tooltip("peak_rating", title="Peak Rating"),
                    alt.Tooltip("avg_rating", title="Avg Rating"),
                ],
            )
            .properties(height=400)
        )
        labels = (
            alt.Chart(ov)
            .mark_text(dy=-18, fontSize=11, fontWeight="bold", color="white")
            .encode(
                x=alt.X("total_games:Q", scale=alt.Scale(type="log")),
                y="win_rate:Q",
                text="display_name:N",
            )
        )
        st.altair_chart(scatter + labels, use_container_width=True)

        # Stats table
        tbl = overview[
            ["player", "total_games", "wins", "losses", "draws", "win_rate", "peak_rating", "avg_rating"]
        ].copy()
        tbl["player"] = tbl["player"].map(display_name)
        tbl.columns = [
            "Player",
            "Games",
            "Wins",
            "Losses",
            "Draws",
            "Win %",
            "Peak",
            "Avg Rating",
        ]
        st.dataframe(tbl, hide_index=True, use_container_width=True)

# ====================== TAB 2: RATING EVOLUTION ======================
with tab_rating:
    st.subheader("Rating Evolution")
    st.caption("Daily closing rating for each player by format")

    if not rating_evo.empty:
        re = rating_evo.copy()
        re["display_name"] = re["player"].map(display_name)

        # Format filter
        formats_avail = sorted(re["time_class"].unique())
        fmt_sel = st.selectbox("Format", formats_avail, index=0, key="rating_fmt")
        re_filtered = re[re["time_class"] == fmt_sel]

        if not re_filtered.empty:
            players_in_data = re_filtered["player"].unique().tolist()
            line = (
                alt.Chart(re_filtered)
                .mark_line(point=True, strokeWidth=2.5)
                .encode(
                    x=alt.X("game_date:T", title="Date"),
                    y=alt.Y("closing_rating:Q", title="Rating", scale=alt.Scale(zero=False)),
                    color=alt.Color(
                        "display_name:N",
                        scale=alt.Scale(
                            domain=[display_name(p) for p in players_in_data],
                            range=[player_color(p) for p in players_in_data],
                        ),
                        legend=alt.Legend(title="Player", orient="bottom"),
                    ),
                    tooltip=[
                        alt.Tooltip("display_name", title="Player"),
                        alt.Tooltip("game_date:T", title="Date"),
                        alt.Tooltip("closing_rating", title="Rating"),
                        alt.Tooltip("daily_high", title="High"),
                        alt.Tooltip("daily_low", title="Low"),
                        alt.Tooltip("games_played", title="Games"),
                    ],
                )
                .properties(height=420)
            )
            st.altair_chart(line, use_container_width=True)

            # Rating volatility
            vol = (
                re_filtered.groupby("player")
                .agg(
                    high=("daily_high", "max"),
                    low=("daily_low", "min"),
                    avg=("closing_rating", "mean"),
                )
                .reset_index()
            )
            vol["swing"] = vol["high"] - vol["low"]
            vol["display_name"] = vol["player"].map(display_name)
            vol = vol.sort_values("swing", ascending=False)

            st.markdown("**Rating Volatility** (highest to lowest swing)")
            vol_chart = (
                alt.Chart(vol)
                .mark_bar(cornerRadiusTopRight=6, cornerRadiusTopLeft=6)
                .encode(
                    x=alt.X("display_name:N", title="Player", sort="-y"),
                    y=alt.Y("swing:Q", title="Rating Swing (High - Low)"),
                    color=alt.Color(
                        "player:N",
                        scale=alt.Scale(
                            domain=vol["player"].tolist(),
                            range=[player_color(p) for p in vol["player"]],
                        ),
                        legend=None,
                    ),
                    tooltip=[
                        alt.Tooltip("display_name", title="Player"),
                        alt.Tooltip("high", title="Peak"),
                        alt.Tooltip("low", title="Low"),
                        alt.Tooltip("swing", title="Swing"),
                    ],
                )
                .properties(height=300)
            )
            st.altair_chart(vol_chart, use_container_width=True)

# ====================== TAB 3: HEAD-TO-HEAD ======================
with tab_h2h:
    st.subheader("Head-to-Head Records")
    st.caption("Matchup records between players (minimum 3 games)")

    if not h2h.empty:
        h = h2h.head(15).copy()
        h["p1_display"] = h["player1"].map(display_name)
        h["p2_display"] = h["player2"].map(display_name)
        h["matchup"] = h["p1_display"] + " vs " + h["p2_display"]

        h2h_long = []
        for _, row in h.iterrows():
            h2h_long.append(
                {
                    "matchup": row["matchup"],
                    "result": f"{row['p1_display']} wins",
                    "count": int(row["p1_wins"]),
                    "color_key": "p1",
                }
            )
            h2h_long.append(
                {
                    "matchup": row["matchup"],
                    "result": f"{row['p2_display']} wins",
                    "count": int(row["p2_wins"]),
                    "color_key": "p2",
                }
            )
            h2h_long.append(
                {
                    "matchup": row["matchup"],
                    "result": "Draws",
                    "count": int(row["draws"]),
                    "color_key": "draw",
                }
            )
        h2h_df = pd.DataFrame(h2h_long)

        h2h_chart = (
            alt.Chart(h2h_df)
            .mark_bar(cornerRadius=3)
            .encode(
                y=alt.Y(
                    "matchup:N",
                    title=None,
                    sort=h["matchup"].tolist(),
                ),
                x=alt.X("count:Q", title="Games", stack=None),
                color=alt.Color(
                    "color_key:N",
                    scale=alt.Scale(
                        domain=["p1", "p2", "draw"],
                        range=["#4CAF50", "#F44336", "#9E9E9E"],
                    ),
                    legend=alt.Legend(
                        title="Result",
                        labelExpr="datum.value === 'p1' ? 'Player 1 Wins' : datum.value === 'p2' ? 'Player 2 Wins' : 'Draws'",
                    ),
                ),
                yOffset="color_key:N",
                tooltip=[
                    alt.Tooltip("matchup", title="Matchup"),
                    alt.Tooltip("result", title="Result"),
                    alt.Tooltip("count", title="Games"),
                ],
            )
            .properties(height=max(250, len(h) * 45))
        )
        st.altair_chart(h2h_chart, use_container_width=True)

        # Notable matchup callout
        top_m = h.iloc[0]
        st.caption(
            f"Most played: {top_m['p1_display']} vs {top_m['p2_display']} -- "
            f"{int(top_m['p1_wins'])}-{int(top_m['p2_wins'])} "
            f"({int(top_m['draws'])} draws) over {int(top_m['total_games'])} games"
        )

# ====================== TAB 4: FORMAT KINGS ======================
with tab_format:
    st.subheader("Format Kings")
    st.caption("Who dominates bullet vs blitz vs rapid?")

    if not format_kings.empty:
        fk = format_kings.copy()
        fk["display_name"] = fk["player"].map(display_name)

        grouped = (
            alt.Chart(fk)
            .mark_bar(cornerRadiusTopRight=6, cornerRadiusTopLeft=6)
            .encode(
                x=alt.X("display_name:N", title="Player"),
                y=alt.Y("win_rate:Q", title="Win Rate (%)"),
                color=alt.Color(
                    "time_class:N",
                    scale=alt.Scale(
                        domain=["bullet", "blitz", "rapid"],
                        range=["#FF6B6B", "#4ECDC4", "#45B7D1"],
                    ),
                    legend=alt.Legend(title="Format"),
                ),
                xOffset="time_class:N",
                tooltip=[
                    alt.Tooltip("display_name", title="Player"),
                    alt.Tooltip("time_class", title="Format"),
                    alt.Tooltip("win_rate", title="Win %", format=".1f"),
                    alt.Tooltip("games", title="Games"),
                    alt.Tooltip("peak_rating", title="Peak Rating"),
                ],
            )
            .properties(height=400)
        )
        st.altair_chart(grouped, use_container_width=True)

        # Games distribution table
        st.markdown("**Games by Format**")
        fmt_tbl = fk[["player", "time_class", "games", "win_rate", "peak_rating"]].copy()
        fmt_tbl["player"] = fmt_tbl["player"].map(display_name)
        fmt_tbl.columns = ["Player", "Format", "Games", "Win %", "Peak Rating"]
        st.dataframe(fmt_tbl, hide_index=True, use_container_width=True)

# ====================== TAB 5: STREAKS & TILTS ======================
with tab_streaks:
    st.subheader("Streaks & Tilts")
    st.caption("Longest consecutive wins and losses -- who's clutch, who tilts?")

    if not streaks.empty:
        sk = streaks.copy()
        sk["display_name"] = sk["player"].map(display_name)

        # Paired bar chart
        streak_long = []
        for _, row in sk.iterrows():
            streak_long.append(
                {
                    "display_name": row["display_name"],
                    "player": row["player"],
                    "type": "Win Streak",
                    "length": int(row["longest_win_streak"]),
                }
            )
            streak_long.append(
                {
                    "display_name": row["display_name"],
                    "player": row["player"],
                    "type": "Loss Streak",
                    "length": int(row["longest_loss_streak"]),
                }
            )
        streak_df = pd.DataFrame(streak_long)

        streak_chart = (
            alt.Chart(streak_df)
            .mark_bar(cornerRadiusTopRight=8, cornerRadiusTopLeft=8)
            .encode(
                x=alt.X("display_name:N", title="Player", sort="-y"),
                y=alt.Y("length:Q", title="Consecutive Games"),
                color=alt.Color(
                    "type:N",
                    scale=alt.Scale(
                        domain=["Win Streak", "Loss Streak"],
                        range=["#4CAF50", "#F44336"],
                    ),
                    legend=alt.Legend(title="Type"),
                ),
                xOffset="type:N",
                tooltip=[
                    alt.Tooltip("display_name", title="Player"),
                    alt.Tooltip("type", title="Type"),
                    alt.Tooltip("length", title="Games"),
                ],
            )
            .properties(height=350)
        )
        st.altair_chart(streak_chart, use_container_width=True)

        # Hot streak / tilt streak counts
        st.markdown("**Momentum Summary**")
        mom_tbl = sk[["player", "longest_win_streak", "longest_loss_streak", "hot_streaks_3plus", "tilt_streaks_3plus"]].copy()
        mom_tbl["player"] = mom_tbl["player"].map(display_name)
        mom_tbl.columns = ["Player", "Best Win Streak", "Worst Loss Streak", "Hot Runs (3+)", "Tilt Runs (3+)"]
        st.dataframe(mom_tbl, hide_index=True, use_container_width=True)

# ====================== TAB 6: ACTIVITY / NIGHT OWLS ======================
with tab_activity:
    st.subheader("The Night Owl Index")
    st.caption("When do players play chess? Activity by hour (UTC)")

    if not activity.empty:
        act = activity.copy()
        act["display_name"] = act["player"].map(display_name)

        # Aggregate by hour across all days
        hourly = (
            act.groupby(["player", "display_name", "hour_utc"])
            .agg(games=("games", "sum"), wins=("wins", "sum"))
            .reset_index()
        )
        hourly["win_rate"] = (100.0 * hourly["wins"] / hourly["games"]).round(2)

        players_avail = sorted(hourly["display_name"].unique())
        selected = st.multiselect(
            "Select players",
            players_avail,
            default=players_avail[:3],
            key="activity_players",
        )
        hourly_filtered = hourly[hourly["display_name"].isin(selected)]

        if not hourly_filtered.empty:
            # Games by hour
            hour_games = (
                alt.Chart(hourly_filtered)
                .mark_bar(opacity=0.7)
                .encode(
                    x=alt.X("hour_utc:O", title="Hour (UTC)", axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("games:Q", title="Games Played"),
                    color=alt.Color(
                        "display_name:N",
                        legend=alt.Legend(title="Player", orient="bottom"),
                    ),
                    xOffset="display_name:N",
                    tooltip=[
                        alt.Tooltip("display_name", title="Player"),
                        alt.Tooltip("hour_utc", title="Hour"),
                        alt.Tooltip("games", title="Games"),
                        alt.Tooltip("win_rate", title="Win %", format=".1f"),
                    ],
                )
                .properties(height=350)
            )
            st.altair_chart(hour_games, use_container_width=True)

            # Win rate by hour line chart
            st.markdown("**Win Rate by Hour**")
            hour_wr = (
                alt.Chart(hourly_filtered)
                .mark_line(point=True, strokeWidth=2)
                .encode(
                    x=alt.X("hour_utc:O", title="Hour (UTC)", axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("win_rate:Q", title="Win Rate (%)", scale=alt.Scale(zero=False)),
                    color=alt.Color(
                        "display_name:N",
                        legend=alt.Legend(title="Player", orient="bottom"),
                    ),
                    tooltip=[
                        alt.Tooltip("display_name", title="Player"),
                        alt.Tooltip("hour_utc", title="Hour"),
                        alt.Tooltip("win_rate", title="Win %", format=".1f"),
                        alt.Tooltip("games", title="Games"),
                    ],
                )
                .properties(height=350)
            )
            st.altair_chart(hour_wr, use_container_width=True)

# ====================== TAB 7: HOW THEY LOSE ======================
with tab_lose:
    st.subheader("How They Lose")
    st.caption("Super GMs resign -- streamers get checkmated. The data proves it.")

    if not how_lose.empty:
        hl = how_lose.copy()
        hl["display_name"] = hl["player"].map(display_name)

        # Stacked bar of loss types
        loss_long = []
        for _, row in hl.iterrows():
            loss_long.append(
                {
                    "display_name": row["display_name"],
                    "player": row["player"],
                    "type": "Resigned",
                    "count": int(row["resigned"]),
                }
            )
            loss_long.append(
                {
                    "display_name": row["display_name"],
                    "player": row["player"],
                    "type": "Timed Out",
                    "count": int(row["lost_on_time"]),
                }
            )
            loss_long.append(
                {
                    "display_name": row["display_name"],
                    "player": row["player"],
                    "type": "Checkmated",
                    "count": int(row["got_checkmated"]),
                }
            )
        loss_df = pd.DataFrame(loss_long)

        loss_chart = (
            alt.Chart(loss_df)
            .mark_bar(cornerRadius=3)
            .encode(
                y=alt.Y("display_name:N", title=None, sort=hl["display_name"].tolist()),
                x=alt.X("count:Q", title="Games Lost", stack="zero"),
                color=alt.Color(
                    "type:N",
                    scale=alt.Scale(
                        domain=["Resigned", "Timed Out", "Checkmated"],
                        range=["#FF9800", "#2196F3", "#F44336"],
                    ),
                    legend=alt.Legend(title="Loss Type"),
                ),
                order=alt.Order("type:N"),
                tooltip=[
                    alt.Tooltip("display_name", title="Player"),
                    alt.Tooltip("type", title="How"),
                    alt.Tooltip("count", title="Games"),
                ],
            )
            .properties(height=max(200, len(hl) * 50))
        )
        st.altair_chart(loss_chart, use_container_width=True)

        # Percentage table
        pct_tbl = hl[["player", "total_losses", "resign_pct", "timeout_pct", "checkmate_pct"]].copy()
        pct_tbl["player"] = pct_tbl["player"].map(display_name)
        pct_tbl.columns = ["Player", "Total Losses", "Resign %", "Timeout %", "Checkmate %"]
        st.dataframe(pct_tbl, hide_index=True, use_container_width=True)

# ====================== TAB 8: BIGGEST UPSETS ======================
with tab_upsets:
    st.subheader("Biggest Rating Upsets")
    st.caption("When lower-rated players beat the elite -- defying the odds")

    if not upsets.empty:
        up = upsets.head(15).copy()
        up["winner_display"] = up["winner"].map(display_name)
        up["loser_display"] = up["loser"].map(display_name)
        up["label"] = up["winner_display"] + " beat " + up["loser_display"]
        up["winner_rating_int"] = up["winner_rating"].astype(int)
        up["loser_rating_int"] = up["loser_rating"].astype(int)

        base = alt.Chart(up).encode(
            y=alt.Y(
                "label:N",
                title=None,
                sort=alt.EncodingSortField(field="rating_gap", order="descending"),
                axis=alt.Axis(labelLimit=300),
            )
        )

        rules = base.mark_rule(strokeWidth=3, color="#FF6B6B").encode(
            x=alt.X("winner_rating_int:Q", title="Rating", scale=alt.Scale(zero=False)),
            x2="loser_rating_int:Q",
            tooltip=[
                alt.Tooltip("winner_display", title="Winner"),
                alt.Tooltip("winner_rating_int", title="Winner Rating"),
                alt.Tooltip("loser_display", title="Loser"),
                alt.Tooltip("loser_rating_int", title="Loser Rating"),
                alt.Tooltip("rating_gap", title="Gap"),
                alt.Tooltip("time_class", title="Format"),
            ],
        )

        winner_pts = base.mark_circle(size=100, color="#4CAF50").encode(
            x="winner_rating_int:Q",
            tooltip=[
                alt.Tooltip("winner_display", title="Winner"),
                alt.Tooltip("winner_rating_int", title="Rating"),
            ],
        )

        loser_pts = base.mark_circle(size=100, color="#F44336").encode(
            x="loser_rating_int:Q",
            tooltip=[
                alt.Tooltip("loser_display", title="Loser"),
                alt.Tooltip("loser_rating_int", title="Rating"),
            ],
        )

        upset_chart = (rules + winner_pts + loser_pts).properties(height=max(250, len(up) * 30))
        st.altair_chart(upset_chart, use_container_width=True)
        st.caption("Green = Winner (lower rated) | Red = Loser (higher rated)")

        if len(upsets) > 0:
            top_u = upsets.iloc[0]
            st.info(
                f"**Biggest upset**: {display_name(top_u['winner'])} ({int(top_u['winner_rating'])}) "
                f"beat {display_name(top_u['loser'])} ({int(top_u['loser_rating'])}) -- "
                f"{int(top_u['rating_gap'])} point gap in {top_u['time_class']}"
            )

# ====================== TAB 9: STREAMERS VS GMS ======================
with tab_svgm:
    st.subheader("Streamers vs Super GMs")
    st.caption("How do content creators stack up against the world's best?")

    if not svgm.empty:
        sv = svgm.copy()
        sv["display_name"] = sv["player"].map(display_name)

        # Category summary
        cat_summary = (
            sv.groupby("category")
            .agg(
                avg_win_rate=("win_rate", "mean"),
                avg_rating=("avg_rating", "mean"),
                avg_checkmate_loss=("checkmate_loss_rate", "mean"),
                avg_resign_loss=("resign_loss_rate", "mean"),
                total_games=("total_games", "sum"),
                players=("player", "count"),
            )
            .reset_index()
        )

        c1, c2 = st.columns(2)
        for _, row in cat_summary.iterrows():
            col = c1 if row["category"] == "Super GM" else c2
            with col:
                st.markdown(f"### {row['category']}s")
                st.metric("Avg Win Rate", f"{row['avg_win_rate']:.1f}%")
                st.metric("Avg Rating", f"{int(row['avg_rating']):,}")
                st.metric("Checkmate Loss Rate", f"{row['avg_checkmate_loss']:.2f}%")
                st.metric("Games Analyzed", f"{int(row['total_games']):,}")

        st.markdown("---")

        # Individual comparison chart
        compare_chart = (
            alt.Chart(sv)
            .mark_circle(opacity=0.85, size=300)
            .encode(
                x=alt.X("avg_rating:Q", title="Average Rating", scale=alt.Scale(zero=False)),
                y=alt.Y("win_rate:Q", title="Win Rate (%)", scale=alt.Scale(zero=False)),
                color=alt.Color(
                    "category:N",
                    scale=alt.Scale(
                        domain=["Super GM", "Streamer"],
                        range=["#FFD700", "#FF69B4"],
                    ),
                    legend=alt.Legend(title="Category"),
                ),
                tooltip=[
                    alt.Tooltip("display_name", title="Player"),
                    alt.Tooltip("category", title="Category"),
                    alt.Tooltip("win_rate", title="Win %", format=".1f"),
                    alt.Tooltip("avg_rating", title="Avg Rating"),
                    alt.Tooltip("total_games", title="Games"),
                ],
            )
            .properties(height=400)
        )
        labels_sv = (
            alt.Chart(sv)
            .mark_text(dy=-18, fontSize=11, fontWeight="bold", color="white")
            .encode(
                x="avg_rating:Q",
                y="win_rate:Q",
                text="display_name:N",
            )
        )
        st.altair_chart(compare_chart + labels_sv, use_container_width=True)

        # Full comparison table
        detail_tbl = sv[
            ["player", "category", "total_games", "win_rate", "avg_rating", "peak_rating", "checkmate_loss_rate", "resign_loss_rate"]
        ].copy()
        detail_tbl["player"] = detail_tbl["player"].map(display_name)
        detail_tbl.columns = [
            "Player", "Category", "Games", "Win %", "Avg Rating", "Peak", "Checkmate Loss %", "Resign Loss %"
        ]
        st.dataframe(detail_tbl, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")

with st.expander("Methodology & Data Notes", expanded=False):
    try:
        footnotes_path = base_path / "footnotes.md"
        if footnotes_path.exists():
            st.markdown(footnotes_path.read_text())
        else:
            st.markdown(
                "**Data Source**: Chess.com API via Bruin\n\n"
                "**Database**: MotherDuck (DuckDB)\n\n"
                "**Processing**: Games enriched from raw JSON, deduplicated by game URL"
            )
    except Exception:
        pass

st.caption(
    "Data: Chess.com API via Bruin | Database: MotherDuck | "
    "Built with Streamlit + Altair"
)
