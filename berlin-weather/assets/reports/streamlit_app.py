from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(
    page_title="Berlin Winter Check",
    layout="wide",
)

# ──────────────────────────────────────────────────────────────────────
# Connection
# ──────────────────────────────────────────────────────────────────────

PROJECT_ID = "bruin-playground-arsalan"

base_path = Path(__file__).parent


@st.cache_resource
def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


def run_raw(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


def run_query(filename: str) -> pd.DataFrame:
    sql = (base_path / filename).read_text()
    return get_client().query(sql).to_dataframe()


# ──────────────────────────────────────────────────────────────────────
# Load & prep
# ──────────────────────────────────────────────────────────────────────

daily = run_raw("SELECT * FROM `bruin-playground-arsalan.staging.weather_daily` ORDER BY date")
streaks = run_query("weather_streaks.sql")

daily["date"] = pd.to_datetime(daily["date"])

# Assign each Dec/Jan/Feb day to a winter. Dec 2025 + Jan-Feb 2026 = "2025/26"
winter = daily[daily["month"].isin([12, 1, 2])].copy()
winter["winter_year"] = winter.apply(
    lambda r: r["year"] if r["month"] == 12 else r["year"] - 1, axis=1
)
winter["winter_label"] = winter["winter_year"].apply(
    lambda y: f"{y}/{(y + 1) % 100:02d}"
)
winter = winter[winter["winter_label"] != "2008/09"]

CURRENT_WINTER = winter["winter_label"].max()
HIGHLIGHT = "#D55E00"
DEFAULT = "#56B4E9"

# ──────────────────────────────────────────────────────────────────────
# Header
# ──────────────────────────────────────────────────────────────────────

st.title("Is Berlin's 2025/26 Winter Really That Bad?")
st.caption(
    "Daily weather data for Berlin (2010-2026) from Open-Meteo  ·  "
    "Built with Bruin + BigQuery + Streamlit"
)

latest_date = daily["date"].max().strftime("%b %d, %Y")
st.info(
    f"The current winter (2025/26) is **incomplete** - data runs through **{latest_date}**. "
    "Percentages are based on days observed so far."
)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════
# 1. % of winter days below 1 °C
# ══════════════════════════════════════════════════════════════════════

st.subheader("Freezing Winters: % of Days Below 1 °C")
st.caption("Percentage of winter days (Dec-Feb) where the mean temperature was below 1 °C.")

freezing = (
    winter.groupby("winter_label")
    .apply(lambda g: pd.Series({
        "total_days": len(g),
        "freezing_days": (g["temp_mean_c"] < 1).sum(),
    }))
    .reset_index()
)
freezing["pct"] = (freezing["freezing_days"] / freezing["total_days"] * 100).round(1)
freezing["is_current"] = freezing["winter_label"] == CURRENT_WINTER

freezing_chart = (
    alt.Chart(freezing)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("winter_label:N", title="Winter", sort=list(freezing["winter_label"])),
        y=alt.Y("pct:Q", title="% of Days Below 1 °C"),
        color=alt.condition(
            alt.datum.is_current,
            alt.value(HIGHLIGHT),
            alt.value(DEFAULT),
        ),
        tooltip=[
            alt.Tooltip("winter_label:N", title="Winter"),
            alt.Tooltip("pct:Q", title="Below 1 °C (%)", format=".1f"),
            alt.Tooltip("freezing_days:Q", title="Freezing Days"),
            alt.Tooltip("total_days:Q", title="Total Days"),
        ],
    )
    .properties(height=340)
)

hist_avg = freezing.loc[~freezing["is_current"], "pct"].mean()
avg_rule = (
    alt.Chart(pd.DataFrame({"avg": [hist_avg]}))
    .mark_rule(color="#999999", strokeDash=[6, 3], strokeWidth=2)
    .encode(y="avg:Q")
)
avg_text = (
    alt.Chart(pd.DataFrame({"avg": [hist_avg], "label": [f"Historical avg: {hist_avg:.1f}%"]}))
    .mark_text(align="left", dx=5, dy=-8, color="#999999", fontSize=12)
    .encode(y="avg:Q", text="label:N")
)

st.altair_chart(freezing_chart + avg_rule + avg_text, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════
# 2. Overcast vs sunny during winter
# ══════════════════════════════════════════════════════════════════════

st.subheader("Grey Skies: Gloomy vs Sunny Winter Days")
st.caption(
    "Gloomy = less than 1 hour of sunshine that day. "
    "Sunny = at least 1 hour of sunshine."
)

sky = (
    winter.groupby("winter_label")
    .apply(lambda g: pd.Series({
        "total_days": len(g),
        "sunny_days": (g["sunshine_hours"] >= 1).sum(),
        "gloomy_days": (g["sunshine_hours"] < 1).sum(),
    }))
    .reset_index()
)
sky["sunny_pct"] = (sky["sunny_days"] / sky["total_days"] * 100).round(1)
sky["gloomy_pct"] = (sky["gloomy_days"] / sky["total_days"] * 100).round(1)

sky_melt = sky.melt(
    id_vars=["winter_label", "total_days"],
    value_vars=["sunny_pct", "gloomy_pct"],
    var_name="type",
    value_name="pct",
)
sky_melt["type"] = sky_melt["type"].map({"sunny_pct": "Some Sun", "gloomy_pct": "Gloomy"})

sky_chart = (
    alt.Chart(sky_melt)
    .mark_bar()
    .encode(
        x=alt.X("winter_label:N", title="Winter", sort=list(sky["winter_label"])),
        y=alt.Y("pct:Q", title="% of Winter Days", stack="normalize",
                 axis=alt.Axis(format="%")),
        color=alt.Color(
            "type:N",
            title="Sky",
            scale=alt.Scale(
                domain=["Some Sun", "Gloomy"],
                range=["#E69F00", "#999999"],
            ),
        ),
        tooltip=[
            alt.Tooltip("winter_label:N", title="Winter"),
            alt.Tooltip("type:N", title="Condition"),
            alt.Tooltip("pct:Q", title="%", format=".1f"),
            alt.Tooltip("total_days:Q", title="Total Days"),
        ],
    )
    .properties(height=340)
)
st.altair_chart(sky_chart, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════
# 3. January harshness index
# ══════════════════════════════════════════════════════════════════════

st.subheader("January Harshness Index")
st.caption(
    "% of January days that were **snowy or had a mean temperature below -5 °C**. "
    "Higher = harsher January."
)

jan = daily[daily["month"] == 1].copy()
jan_harsh = (
    jan.groupby("year")
    .apply(lambda g: pd.Series({
        "total_days": len(g),
        "harsh_days": ((g["has_snow"]) | (g["temp_mean_c"] < -5)).sum(),
    }))
    .reset_index()
)
jan_harsh["harsh_pct"] = (jan_harsh["harsh_days"] / jan_harsh["total_days"] * 100).round(1)
jan_harsh["is_current"] = jan_harsh["year"] == 2026

harsh_chart = (
    alt.Chart(jan_harsh)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("year:O", title="January"),
        y=alt.Y("harsh_pct:Q", title="Harshness Index (%)"),
        color=alt.condition(
            alt.datum.is_current,
            alt.value(HIGHLIGHT),
            alt.value("#CC79A7"),
        ),
        tooltip=[
            alt.Tooltip("year:O", title="January"),
            alt.Tooltip("harsh_pct:Q", title="Harshness %", format=".1f"),
            alt.Tooltip("harsh_days:Q", title="Harsh Days"),
            alt.Tooltip("total_days:Q", title="Total Days"),
        ],
    )
    .properties(height=340)
)

hist_avg_h = jan_harsh.loc[~jan_harsh["is_current"], "harsh_pct"].mean()
avg_rule_h = (
    alt.Chart(pd.DataFrame({"avg": [hist_avg_h]}))
    .mark_rule(color="#999999", strokeDash=[6, 3], strokeWidth=2)
    .encode(y="avg:Q")
)
avg_text_h = (
    alt.Chart(pd.DataFrame({"avg": [hist_avg_h], "label": [f"Historical avg: {hist_avg_h:.1f}%"]}))
    .mark_text(align="left", dx=5, dy=-8, color="#999999", fontSize=12)
    .encode(y="avg:Q", text="label:N")
)

st.altair_chart(harsh_chart + avg_rule_h + avg_text_h, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════
# 4. Longest consecutive cloudy (not-clear) streaks
# ══════════════════════════════════════════════════════════════════════

st.subheader("Longest Consecutive Gloomy Streaks")
st.caption(
    "Longest runs of consecutive days with less than 1 hour of sunshine. "
    "These are the truly grey stretches."
)

streaks["streak_start"] = pd.to_datetime(streaks["streak_start"])
streaks["streak_end"] = pd.to_datetime(streaks["streak_end"])
streaks["label"] = (
    streaks["streak_start"].dt.strftime("%b %d, %Y")
    + " - "
    + streaks["streak_end"].dt.strftime("%b %d, %Y")
)
streaks["in_current_winter"] = streaks["streak_start"].apply(
    lambda d: (d.month == 12 and d.year == 2025)
              or (d.month in [1, 2] and d.year == 2026)
              or (d.month == 12 and d.year == 2025)
) | streaks["streak_end"].apply(
    lambda d: (d.month == 12 and d.year == 2025)
              or (d.month in [1, 2] and d.year == 2026)
)

streak_chart = (
    alt.Chart(streaks.head(10))
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("streak_length:Q", title="Consecutive Gloomy Days"),
        y=alt.Y("label:N", title=None, sort="-x"),
        color=alt.condition(
            alt.datum.in_current_winter,
            alt.value(HIGHLIGHT),
            alt.value("#999999"),
        ),
        tooltip=[
            alt.Tooltip("label:N", title="Period"),
            alt.Tooltip("streak_length:Q", title="Days"),
        ],
    )
    .properties(height=340)
)
st.altair_chart(streak_chart, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════
# 5. Berlin forgot what winter felt like
# ══════════════════════════════════════════════════════════════════════

st.markdown("---")
st.subheader("Berlin Forgot What Winter Felt Like")
st.caption(
    "Average winter temperature (Dec-Feb) and number of bitter-cold days "
    "(mean temp below -5 °C). After 12 mild years, 2025/26 brought winter back."
)

winter_summary = (
    winter.groupby("winter_label")
    .agg(
        winter_year=("winter_year", "first"),
        avg_temp=("temp_mean_c", "mean"),
        bitter_cold=("temp_mean_c", lambda s: (s < -5).sum()),
        total_days=("temp_mean_c", "count"),
    )
    .reset_index()
)
winter_summary["avg_temp"] = winter_summary["avg_temp"].round(2)
winter_summary["is_current"] = winter_summary["winter_label"] == CURRENT_WINTER
winter_summary["era"] = winter_summary["winter_year"].apply(
    lambda y: "2025/26" if y == 2025
    else ("Mild era (2013-24)" if 2013 <= y <= 2024 else "Early (2009-12)")
)

temp_bars = (
    alt.Chart(winter_summary)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("winter_label:N", title="Winter",
                sort=list(winter_summary["winter_label"])),
        y=alt.Y("avg_temp:Q", title="Average Temperature (°C)"),
        color=alt.Color(
            "era:N",
            title="Era",
            scale=alt.Scale(
                domain=["Early (2009-12)", "Mild era (2013-24)", "2025/26"],
                range=["#56B4E9", "#E69F00", HIGHLIGHT],
            ),
        ),
        tooltip=[
            alt.Tooltip("winter_label:N", title="Winter"),
            alt.Tooltip("avg_temp:Q", title="Avg Temp °C", format=".1f"),
            alt.Tooltip("bitter_cold:Q", title="Bitter Cold Days"),
            alt.Tooltip("total_days:Q", title="Total Days"),
        ],
    )
    .properties(height=380)
)

zero_line = (
    alt.Chart(pd.DataFrame({"y": [0]}))
    .mark_rule(color="#333333", strokeWidth=1)
    .encode(y="y:Q")
)

bitter_dots = (
    alt.Chart(winter_summary)
    .mark_circle(size=80, opacity=0.9)
    .encode(
        x=alt.X("winter_label:N", sort=list(winter_summary["winter_label"])),
        y=alt.Y("bitter_cold:Q", title="Bitter Cold Days (< -5 °C)"),
        color=alt.Color("era:N", title="Era", scale=alt.Scale(
            domain=["Early (2009-12)", "Mild era (2013-24)", "2025/26"],
            range=["#56B4E9", "#E69F00", HIGHLIGHT],
        )),
        tooltip=[
            alt.Tooltip("winter_label:N", title="Winter"),
            alt.Tooltip("bitter_cold:Q", title="Bitter Cold Days"),
        ],
    )
)

bitter_line = (
    alt.Chart(winter_summary)
    .mark_line(strokeWidth=1.5, opacity=0.6, color="#999999")
    .encode(
        x=alt.X("winter_label:N", sort=list(winter_summary["winter_label"])),
        y=alt.Y("bitter_cold:Q"),
    )
)

temp_col, bitter_col = st.columns(2)
with temp_col:
    st.markdown("**Avg Winter Temperature**")
    st.altair_chart(temp_bars + zero_line, use_container_width=True)
with bitter_col:
    st.markdown("**Bitter Cold Days (< -5 °C)**")
    st.altair_chart(bitter_line + bitter_dots, use_container_width=True)

mild_era_avg = winter_summary.loc[
    winter_summary["era"] == "Mild era (2013-24)", "avg_temp"
].mean()
mild_era_bitter = winter_summary.loc[
    winter_summary["era"] == "Mild era (2013-24)", "bitter_cold"
].mean()
current_temp = winter_summary.loc[
    winter_summary["is_current"], "avg_temp"
].values[0]
current_bitter = int(winter_summary.loc[
    winter_summary["is_current"], "bitter_cold"
].values[0])

st.markdown(
    f"> From 2013 to 2024, Berlin averaged **{mild_era_avg:.1f} °C** in winter "
    f"with just **{mild_era_bitter:.0f} bitter-cold days** per year. "
    f"2019/20 hit **4.6 °C** with **zero** days below -5 °C — winter barely existed. "
    f"Then 2025/26 arrived: **{current_temp:.1f} °C** average, "
    f"**{current_bitter} bitter-cold days**. "
    "The coldest winter in 15 years."
)

# ──────────────────────────────────────────────────────────────────────
# Verdict
# ──────────────────────────────────────────────────────────────────────

st.markdown("---")

current_freezing = freezing.loc[freezing["is_current"], "pct"].values
current_harsh = jan_harsh.loc[jan_harsh["is_current"], "harsh_pct"].values
current_gloomy = sky.loc[sky["winter_label"] == CURRENT_WINTER, "gloomy_pct"].values

if len(current_freezing) and len(current_harsh) and len(current_gloomy):
    freeze_val = current_freezing[0]
    harsh_val = current_harsh[0]
    gloomy_val = current_gloomy[0]

    prev_freeze = freezing.loc[~freezing["is_current"], "pct"]
    freeze_worse = (prev_freeze >= freeze_val).sum()
    freeze_total = len(prev_freeze)

    prev_harsh = jan_harsh.loc[~jan_harsh["is_current"], "harsh_pct"]
    harsh_worse = (prev_harsh >= harsh_val).sum()
    harsh_total = len(prev_harsh)

    gloomy_avg = sky.loc[sky["winter_label"] != CURRENT_WINTER, "gloomy_pct"].mean()

    def ordinal(n):
        s = {1: "st", 2: "nd", 3: "rd"}.get(n % 10 * (n % 100 not in (11, 12, 13)), "th")
        return f"{n}{s}"

    st.subheader("The Verdict")
    st.markdown(
        f"- **Freezing days**: {freeze_val:.1f}% of winter days below 1 °C — "
        f"the **{ordinal(freeze_worse + 1)} coldest** out of {freeze_total + 1} winters "
        f"(historical avg {hist_avg:.1f}%)\n"
        f"- **Grey skies**: {gloomy_val:.1f}% of days were gloomy — "
        f"historical avg is {gloomy_avg:.1f}%\n"
        f"- **January harshness**: {harsh_val:.1f}% — "
        f"the **{ordinal(harsh_worse + 1)} harshest** out of {harsh_total + 1} Januaries "
        f"(historical avg {hist_avg_h:.1f}%)"
    )

    # Deep-dive: January 2021 vs January 2026
    jan_detail = daily[daily["month"] == 1].copy()
    jan_2021 = jan_detail[jan_detail["year"] == 2021]
    jan_2026 = jan_detail[jan_detail["year"] == 2026]

    if len(jan_2021) and len(jan_2026):
        def jan_stats(df):
            return {
                "snow_days": df["has_snow"].sum(),
                "snow_cm": df["snowfall_cm"].sum(),
                "bitter_cold": (df["temp_mean_c"] < -5).sum(),
                "avg_temp": df["temp_mean_c"].mean(),
                "coldest": df["temp_min_c"].min(),
                "gloomy": (df["sunshine_hours"] < 1).sum(),
                "below_1c": (df["temp_mean_c"] < 1).sum(),
                "harsh_pct": (
                    (df["has_snow"] | (df["temp_mean_c"] < -5)).sum()
                    / len(df) * 100
                ),
            }

        s21 = jan_stats(jan_2021)
        s26 = jan_stats(jan_2026)

        # Winter 2020/21 monthly context
        w2021_dec = daily[(daily["year"] == 2020) & (daily["month"] == 12)]
        dec_avg = w2021_dec["temp_mean_c"].mean() if len(w2021_dec) else 0

        st.markdown("---")
        st.subheader("A Tale of Two Januaries: 2021 vs 2026")
        st.markdown(
            "Winter 2020/21 had **only ~43% freezing days** overall — yet its January scored "
            f"a **{s21['harsh_pct']:.0f}% harshness index**, the 2nd harshest on record. "
            "How? The answer reveals two very different flavours of winter misery."
        )

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("##### January 2021: The White Winter")
            st.markdown(
                f"- **{int(s21['snow_days'])} snow days** out of 31 "
                f"({s21['snow_cm']:.1f} cm total)\n"
                f"- Only **{int(s21['bitter_cold'])} day** below -5 °C\n"
                f"- Average temp: **{s21['avg_temp']:.1f} °C** (mild!)\n"
                f"- {int(s21['gloomy'])} gloomy days (< 1 h sunshine)\n\n"
                "It snowed almost every day, but temperatures hovered around freezing. "
                f"December 2020 was even milder (avg {dec_avg:.1f} °C), "
                "which dragged the full-winter freezing % down to ~43%."
            )
        with col_b:
            st.markdown("##### January 2026: The Bitter Cold")
            st.markdown(
                f"- **{int(s26['snow_days'])} snow days** "
                f"({s26['snow_cm']:.1f} cm total)\n"
                f"- **{int(s26['bitter_cold'])} days** below -5 °C\n"
                f"- Average temp: **{s26['avg_temp']:.1f} °C** (frigid)\n"
                f"- Coldest low: **{s26['coldest']:.1f} °C**\n"
                f"- {int(s26['gloomy'])} gloomy days (< 1 h sunshine)\n\n"
                "Fewer snow days, but the cold was far more intense — "
                f"8x as many bitter-cold days and nearly 3 °C colder on average."
            )

        diff_snow = int(s21["snow_days"] - s26["snow_days"])
        diff_bitter = int(s26["bitter_cold"] - s21["bitter_cold"])

        st.markdown(
            f"> **Bottom line**: January 2021 had **{diff_snow} more snow days** "
            f"but January 2026 had **{diff_bitter} more bitter-cold days**. "
            "2021 was a relentlessly *snowy* month; 2026 is a genuinely *cold* one. "
            "Different misery, same Berlin winter."
        )

st.markdown("---")
st.caption(
    "Data: Open-Meteo Historical Weather API (CC BY 4.0)  ·  "
    "Pipeline: Bruin  ·  Database: BigQuery  ·  Visualization: Streamlit + Altair"
)
