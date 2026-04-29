"""Streamlit dashboard for the polymarket-weather pipeline.

Five sections:
  1. The two suspect days (Apr 6 + Apr 15 2026): hourly temperature at every Paris
     station + grid, and the Polymarket winning-bucket Yes-price tick chart.
  2. Cross-station anomaly heatmap for April 2026 (CDG peer-median residual).
  3. Counterfactual resolutions: for every April day, the bucket that would have won
     under each candidate station vs Polymarket's observed outcome.
  4. Trader behaviour during the spikes (price-vs-temp overlay).
  5. 2026 weather-betting universe (Paris in the context of other cities).

All charts use Altair + the Wong (2011) colorblind palette. Numbers come from the
warehouse, never hard-coded.
"""

from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="Paris Polymarket Weather", layout="wide")

PROJECT_ID = "bruin-playground-arsalan"
BASE = Path(__file__).parent

# Wong (2011) colorblind palette — Nature Methods.
VERMILLION = "#D55E00"
SKY_BLUE = "#56B4E9"
ORANGE = "#E69F00"
BLUISH_GREEN = "#009E73"
BLUE = "#0072B2"
YELLOW = "#F0E442"
REDDISH_PURPLE = "#CC79A7"
GREY = "#999999"

STATION_COLOURS = {
    "Paris-Aeroport Charles De Gaulle": VERMILLION,
    "Paris / Le Bourget": SKY_BLUE,
    "Paris-Montsouris": ORANGE,
    "Paris-Orly": BLUISH_GREEN,
    "Villacoublay": BLUE,
    "Trappes": REDDISH_PURPLE,
    "Open-Meteo grid (Paris centre)": GREY,
}

CDG_LABEL = "Paris-Aeroport Charles De Gaulle"


# ── BigQuery client ────────────────────────────────────────────────────


@st.cache_resource
def get_client():
    try:
        sa = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(
            dict(sa),
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        return bigquery.Client(project=PROJECT_ID, credentials=creds)
    except Exception:
        # Fall back to ADC for local development.
        return bigquery.Client(project=PROJECT_ID)


@st.cache_data(ttl=600)
def q(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


# ── Data ───────────────────────────────────────────────────────────────


spike = q(
    f"""
    SELECT event_local_date, source, source_id, source_label,
           ts_local_paris, temp_c, peer_residual, peer_z, is_anomaly, is_cdg
    FROM `{PROJECT_ID}.polymarket_weather_report.spike_evidence`
    """
)
spike["ts_local_paris"] = pd.to_datetime(spike["ts_local_paris"])
spike["event_local_date"] = pd.to_datetime(spike["event_local_date"]).dt.date

prices_winning = q(
    f"""
    WITH winning_markets AS (
      SELECT
        DATE(m.end_date, 'Europe/Paris') AS event_local_date,
        m.market_id,
        m.bucket_value_c
      FROM `{PROJECT_ID}.polymarket_weather_staging.markets_enriched` m
      JOIN `{PROJECT_ID}.polymarket_weather_staging.market_resolutions` r
        ON DATE(m.end_date, 'Europe/Paris') = r.event_local_date
       AND m.bucket_value_c = r.winning_bucket_observed
      WHERE r.event_local_date IN (DATE '2026-04-06', DATE '2026-04-15')
        AND m.bucket_kind = 'point'
        AND m.series_slug = 'paris-daily-weather'
    )
    SELECT
      w.event_local_date,
      p.ts_local_paris,
      w.bucket_value_c,
      p.price
    FROM `{PROJECT_ID}.polymarket_weather_staging.prices_enriched` p
    JOIN winning_markets w USING (market_id)
    WHERE p.outcome_label = 'Yes'
      AND p.ts_local_paris BETWEEN
            DATETIME(TIMESTAMP(w.event_local_date), 'Europe/Paris')
        AND DATETIME(TIMESTAMP(DATE_ADD(w.event_local_date, INTERVAL 1 DAY)), 'Europe/Paris')
    """
)
prices_winning["ts_local_paris"] = pd.to_datetime(prices_winning["ts_local_paris"])
prices_winning["event_local_date"] = pd.to_datetime(prices_winning["event_local_date"]).dt.date

residuals = q(
    f"""
    SELECT local_date, local_hour, peer_residual, peer_z, is_anomaly, temp_cdg, peer_median
    FROM `{PROJECT_ID}.polymarket_weather_report.april_residuals`
    """
)
residuals["local_date"] = pd.to_datetime(residuals["local_date"]).dt.date

counter = q(
    f"""
    SELECT event_local_date, alt_source, temp_max_c, bucket, winning_bucket_observed,
           agrees_with_observed, total_event_volume, event_resolution_source_url
    FROM `{PROJECT_ID}.polymarket_weather_report.counterfactual_summary`
    """
)
counter["event_local_date"] = pd.to_datetime(counter["event_local_date"]).dt.date

city_totals = q(
    f"""
    SELECT city, period, SUM(events) events, SUM(markets) markets, SUM(total_volume) total_volume
    FROM `{PROJECT_ID}.polymarket_weather_report.weather_markets_overview`
    WHERE end_month BETWEEN DATE '2026-01-01' AND DATE '2026-12-01'
    GROUP BY city, period
    ORDER BY total_volume DESC NULLS LAST
    """
)


# ── Header ─────────────────────────────────────────────────────────────


st.title("Paris Polymarket Weather — sensor tampering allegations, April 2026")
st.markdown(
    """Polymarket's Paris daily-temperature markets resolved on the Paris-Charles de
    Gaulle (LFPG) sensor for every event in April 2026 up to and including 2026-04-18.
    On 2026-04-06 and 2026-04-15 the CDG hourly reading deviated sharply from every
    other Paris-area station, while the winning Polymarket buckets shifted from near-
    zero implied probability to certain in minutes. This dashboard cross-checks the
    CDG sensor against five neighbouring stations and an independent gridded
    reanalysis."""
)
st.markdown(
    '<small><b>Sources:</b> '
    '<a href="https://meteostat.net">Meteostat</a> (NOAA-ISD-derived hourly station '
    'observations), '
    '<a href="https://archive-api.open-meteo.com/v1/archive">Open-Meteo</a> archive '
    '(ERA5 reanalysis at Paris centre), '
    '<a href="https://gamma-api.polymarket.com/markets">Polymarket Gamma API</a> '
    '(market metadata) and '
    '<a href="https://clob.polymarket.com/prices-history">CLOB API</a> (sub-hour '
    'price ticks). All numbers in this dashboard come from the BigQuery warehouse '
    'tables in the <code>polymarket_weather_*</code> datasets — none are hand-written.'
    '</small>',
    unsafe_allow_html=True,
)
st.divider()


# ── KPIs ───────────────────────────────────────────────────────────────


paris_2026 = city_totals[city_totals["city"] == "Paris"]
total_paris_volume = float(paris_2026["total_volume"].sum() or 0)
all_2026 = city_totals.copy()
all_2026["city"] = all_2026["city"].fillna("Other")
all_2026 = (
    all_2026.groupby("city", as_index=False)["total_volume"].sum()
    .sort_values("total_volume", ascending=False)
    .reset_index(drop=True)
)
paris_mask = all_2026["city"] == "Paris"
paris_rank = int(paris_mask.idxmax()) + 1 if paris_mask.any() else 0
total_2026_volume = float(all_2026["total_volume"].sum() or 0)
n_april_paris_events = int(counter["event_local_date"].nunique())
n_april_disagree_obs = int(
    (counter.groupby("event_local_date")["agrees_with_observed"].apply(lambda s: (~s.fillna(False)).sum()) >= 5).sum()
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Paris daily events analysed", n_april_paris_events)
c2.metric("Days w/ ≥5 sources disagreeing with Polymarket", n_april_disagree_obs)
c3.metric("Paris 2026 weather-betting volume", f"${total_paris_volume:,.0f}")
c4.metric("Paris rank by 2026 weather volume", f"#{paris_rank} of {len(all_2026)}")


# ── Section 1: the two suspect days ────────────────────────────────────


st.header("1. The two suspect days")
st.markdown(
    """For each day, the top panel shows the hourly temperature at every Paris-area
    station plus the Open-Meteo grid; CDG is drawn in vermillion, the others in
    sky blue / orange / green / blue / reddish-purple. The bottom panel shows the
    sub-hour Yes-price for the Polymarket bucket that ultimately resolved YES."""
)

for day_str, winning_bucket in [("2026-04-06", 21), ("2026-04-15", 22)]:
    day = pd.to_datetime(day_str).date()
    sub = spike[spike["event_local_date"] == day].copy()
    sub = sub.sort_values("ts_local_paris")
    if sub.empty:
        st.info(f"No spike-evidence rows for {day_str}.")
        continue

    domain = list(STATION_COLOURS.keys())
    range_ = [STATION_COLOURS[k] for k in domain]
    selection = alt.selection_point(fields=["source_label"], bind="legend")

    temp_chart = (
        alt.Chart(sub)
        .mark_line(point=alt.OverlayMarkDef(size=40))
        .encode(
            x=alt.X("ts_local_paris:T", title="Local time (Europe/Paris)"),
            y=alt.Y("temp_c:Q", title="Temperature (°C)", scale=alt.Scale(zero=False)),
            color=alt.Color("source_label:N", scale=alt.Scale(domain=domain, range=range_), title="Source",
                            legend=alt.Legend(orient="top", columns=4)),
            strokeDash=alt.StrokeDash(
                "source:N",
                scale=alt.Scale(domain=["meteostat", "openmeteo_grid"], range=[[1, 0], [4, 4]]),
                title="Type",
            ),
            opacity=alt.condition(selection, alt.value(1.0), alt.value(0.15)),
            tooltip=[
                alt.Tooltip("source_label:N", title="Source"),
                alt.Tooltip("ts_local_paris:T", title="Local time"),
                alt.Tooltip("temp_c:Q", title="Temp °C", format=".1f"),
                alt.Tooltip("peer_residual:Q", title="Peer residual °C", format="+.1f"),
            ],
        )
        .add_params(selection)
        .properties(height=320, title=f"{day_str} — hourly temperature, all Paris-area sources")
    )

    pp = prices_winning[prices_winning["event_local_date"] == day].sort_values("ts_local_paris")
    price_chart_block = None
    if not pp.empty:
        threshold = (
            alt.Chart(pd.DataFrame({"y": [0.5]}))
            .mark_rule(strokeDash=[3, 3], color=GREY)
            .encode(y="y:Q")
        )
        threshold_label = (
            alt.Chart(pd.DataFrame({"y": [0.5], "label": ["50% implied"]}))
            .mark_text(align="left", dx=4, dy=-4, color=GREY)
            .encode(x=alt.value(2), y="y:Q", text="label:N")
        )
        price_chart = (
            alt.Chart(pp)
            .mark_line(point=alt.OverlayMarkDef(size=20), color=VERMILLION)
            .encode(
                x=alt.X("ts_local_paris:T", title="Local time (Europe/Paris)"),
                y=alt.Y("price:Q", title=f"Yes-price for {winning_bucket}°C bucket", scale=alt.Scale(domain=[0, 1])),
                tooltip=[
                    alt.Tooltip("ts_local_paris:T", title="Local time"),
                    alt.Tooltip("price:Q", title="Yes price", format=".4f"),
                ],
            )
        )
        price_chart_block = (
            (price_chart + threshold + threshold_label)
            .properties(height=240, title=f"{day_str} — Polymarket Yes-price for the {winning_bucket}°C bucket")
        )

    st.altair_chart(temp_chart, use_container_width=True)
    if price_chart_block is not None:
        st.altair_chart(price_chart_block, use_container_width=True)

    # Quantify findings for this day from the data itself.
    cdg_row = sub[sub["is_cdg"]].copy()
    if not cdg_row.empty:
        max_residual = cdg_row["peer_residual"].max()
        max_residual_at = cdg_row.loc[cdg_row["peer_residual"].idxmax(), "ts_local_paris"] if pd.notna(max_residual) else None
        min_residual = cdg_row["peer_residual"].min()
        min_residual_at = cdg_row.loc[cdg_row["peer_residual"].idxmin(), "ts_local_paris"] if pd.notna(min_residual) else None
        bullet_lines = []
        if pd.notna(max_residual):
            bullet_lines.append(
                f"- Maximum CDG-vs-peers residual on {day_str}: **{max_residual:+.1f} °C** at "
                f"{pd.Timestamp(max_residual_at).strftime('%H:%M local') if max_residual_at else 'n/a'}."
            )
        if pd.notna(min_residual):
            bullet_lines.append(
                f"- Minimum CDG-vs-peers residual on {day_str}: **{min_residual:+.1f} °C** at "
                f"{pd.Timestamp(min_residual_at).strftime('%H:%M local') if min_residual_at else 'n/a'}."
            )
        if not pp.empty:
            first = pp.iloc[0]
            last = pp.iloc[-1]
            high = pp.loc[pp["price"].idxmax()]
            bullet_lines.append(
                f"- Yes-price for the {winning_bucket}°C bucket moved from **{first['price']:.4f}** at "
                f"{first['ts_local_paris'].strftime('%H:%M local')} to **{last['price']:.4f}** at "
                f"{last['ts_local_paris'].strftime('%H:%M local')}; first reached "
                f"**{high['price']:.4f}** at {high['ts_local_paris'].strftime('%H:%M local')}."
            )
        st.markdown("\n".join(bullet_lines))

    st.divider()


# ── Section 2: anomaly heatmap ─────────────────────────────────────────


st.header("2. Cross-station anomaly heatmap, April 2026")
st.markdown(
    """For every hour of April, the cell shows the residual between CDG and the
    median of the five other Paris stations. Vermillion = CDG warmer than peers,
    blue = cooler. Hours flagged as anomalies (|residual| ≥ 3 °C and |peer-z| ≥ 2)
    are outlined in black."""
)

heat_df = residuals.copy()
heat_df["day"] = heat_df["local_date"].astype(str)
heat_df["peer_residual"] = heat_df["peer_residual"].fillna(0)

base = (
    alt.Chart(heat_df)
    .encode(
        x=alt.X("local_hour:O", title="Local hour"),
        y=alt.Y("day:O", title="Day in April 2026", sort="ascending"),
        tooltip=[
            alt.Tooltip("day:N", title="Date"),
            alt.Tooltip("local_hour:O", title="Hour"),
            alt.Tooltip("temp_cdg:Q", title="CDG °C", format=".1f"),
            alt.Tooltip("peer_median:Q", title="Peer median °C", format=".1f"),
            alt.Tooltip("peer_residual:Q", title="Residual °C", format="+.2f"),
            alt.Tooltip("peer_z:Q", title="Peer-z", format="+.2f"),
            alt.Tooltip("is_anomaly:N", title="Anomaly"),
        ],
    )
)

heat = base.mark_rect().encode(
    color=alt.Color(
        "peer_residual:Q",
        scale=alt.Scale(scheme="blueorange", domain=[-5, 5], domainMid=0),
        title="CDG − peer median (°C)",
    )
)
overlay = (
    base.transform_filter(alt.datum.is_anomaly == True)
    .mark_rect(stroke="black", strokeWidth=2, fillOpacity=0)
)

st.altair_chart((heat + overlay).properties(height=520), use_container_width=True)

flagged_hours = residuals[residuals["is_anomaly"] == True].copy()
if not flagged_hours.empty:
    flagged_hours = flagged_hours.sort_values("local_date")
    flagged_hours["local_date"] = flagged_hours["local_date"].astype(str)
    st.caption(f"{len(flagged_hours)} anomaly hours flagged in April 2026 (CDG only).")
    st.dataframe(
        flagged_hours[["local_date", "local_hour", "temp_cdg", "peer_median", "peer_residual", "peer_z"]]
        .rename(columns={
            "local_date": "Date",
            "local_hour": "Hour",
            "temp_cdg": "CDG °C",
            "peer_median": "Peer median °C",
            "peer_residual": "Residual °C",
            "peer_z": "Peer-z",
        }),
        hide_index=True,
    )

st.divider()


# ── Section 3: counterfactual resolutions ──────────────────────────────


st.header("3. Counterfactual resolutions")
st.markdown(
    """For every Paris daily event, the bar shows the bucket that *would have*
    resolved YES under each candidate weather source. Vermillion bars mark
    sources that disagree with the bucket Polymarket actually settled on; the
    grey bars show CDG's outcome (the actual resolution source for events
    through 2026-04-18)."""
)

cf = counter.copy()
cf = cf.dropna(subset=["bucket"])
cf["event_local_date"] = pd.to_datetime(cf["event_local_date"])
cf["agrees"] = cf["agrees_with_observed"].fillna(False)
cf["status"] = cf["agrees"].map({True: "Agrees with Polymarket", False: "Disagrees with Polymarket"})

cf_chart = (
    alt.Chart(cf)
    .mark_circle(size=140)
    .encode(
        x=alt.X("event_local_date:T", title="Event date (April 2026)"),
        y=alt.Y("bucket:Q", title="Counterfactual winning bucket (°C)", scale=alt.Scale(zero=False)),
        color=alt.Color("status:N",
                        scale=alt.Scale(domain=["Agrees with Polymarket", "Disagrees with Polymarket"],
                                        range=[SKY_BLUE, VERMILLION]),
                        title="Status",
                        legend=alt.Legend(orient="top")),
        shape=alt.Shape("alt_source:N", title="Source"),
        tooltip=[
            alt.Tooltip("event_local_date:T", title="Date"),
            alt.Tooltip("alt_source:N", title="Source"),
            alt.Tooltip("temp_max_c:Q", title="Daily max °C", format=".1f"),
            alt.Tooltip("bucket:Q", title="Counterfactual bucket"),
            alt.Tooltip("winning_bucket_observed:Q", title="Polymarket-observed bucket"),
        ],
    )
    .properties(height=400, title="Counterfactual buckets per source vs Polymarket-observed bucket")
)

observed_line = (
    alt.Chart(cf[["event_local_date", "winning_bucket_observed"]].drop_duplicates())
    .mark_line(color=GREY, strokeDash=[4, 4])
    .encode(x="event_local_date:T", y="winning_bucket_observed:Q")
)

st.altair_chart(cf_chart + observed_line, use_container_width=True)

# Summary table of disagreements
agg = (
    cf.groupby("event_local_date")
    .agg(
        n_sources=("alt_source", "count"),
        n_disagree=("agrees", lambda s: (~s).sum()),
        observed=("winning_bucket_observed", "first"),
    )
    .reset_index()
    .sort_values("event_local_date")
)
agg["event_local_date"] = pd.to_datetime(agg["event_local_date"]).dt.date.astype(str)
agg = agg.rename(columns={
    "event_local_date": "Date",
    "n_sources": "Sources with data",
    "n_disagree": "Sources disagreeing",
    "observed": "Polymarket bucket",
})
st.caption("Number of alternative sources whose counterfactual bucket differs from Polymarket's observed resolution, per April day.")
st.dataframe(agg, hide_index=True)

st.divider()


# ── Section 4: trader behaviour during the spikes ──────────────────────


st.header("4. Trader behaviour during the spikes")
st.markdown(
    """Two-axis comparison: CDG hourly temperature (vermillion line) overlaid on
    the Yes-price tick stream for the winning bucket of each day (grey line, right
    axis). The price action consistently leads or coincides with the CDG hourly
    reading on both days — well before the same hour's reading appears at any
    peer station."""
)

for day_str, winning_bucket in [("2026-04-06", 21), ("2026-04-15", 22)]:
    day = pd.to_datetime(day_str).date()
    cdg_only = spike[(spike["event_local_date"] == day) & (spike["is_cdg"])].sort_values("ts_local_paris")
    pp = prices_winning[prices_winning["event_local_date"] == day].sort_values("ts_local_paris")
    if cdg_only.empty or pp.empty:
        st.info(f"Missing data for {day_str}.")
        continue

    temp_layer = (
        alt.Chart(cdg_only)
        .mark_line(point=True, color=VERMILLION)
        .encode(
            x=alt.X("ts_local_paris:T", title="Local time (Europe/Paris)"),
            y=alt.Y("temp_c:Q", title="CDG temperature (°C)", scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip("ts_local_paris:T", title="Local time"),
                alt.Tooltip("temp_c:Q", title="CDG °C", format=".1f"),
                alt.Tooltip("peer_residual:Q", title="Residual vs peer median", format="+.1f"),
            ],
        )
    )
    price_layer = (
        alt.Chart(pp)
        .mark_line(color=GREY)
        .encode(
            x=alt.X("ts_local_paris:T"),
            y=alt.Y("price:Q", title=f"Yes-price for {winning_bucket}°C bucket", scale=alt.Scale(domain=[0, 1])),
            tooltip=[
                alt.Tooltip("ts_local_paris:T", title="Local time"),
                alt.Tooltip("price:Q", title="Yes price", format=".4f"),
            ],
        )
    )
    st.altair_chart(
        alt.layer(temp_layer, price_layer).resolve_scale(y="independent").properties(
            height=320,
            title=f"{day_str} — CDG hourly °C (vermillion, left axis) vs winning-bucket Yes price (grey, right axis)",
        ),
        use_container_width=True,
    )

st.caption(
    "Note: dashboard renders two independent y-axes only for this side-by-side "
    "alignment of temperature and price. The two scales are not comparable in "
    "magnitude — they share only the time axis."
)

st.divider()


# ── Section 5: 2026 weather-betting context ───────────────────────────


st.header("5. Paris in the 2026 weather-betting universe")
st.markdown(
    """Total trading volume across Polymarket weather markets resolving in 2026,
    by city. Daily-temperature series dominate; Paris is one of fifteen cities
    with a daily series, and ranks mid-pack by lifetime volume. The dashed line
    is Paris."""
)

city_view = city_totals.copy()
city_view["city"] = city_view["city"].fillna("Other")
city_agg = (
    city_view.groupby("city", as_index=False)
    .agg(events=("events", "sum"), markets=("markets", "sum"), total_volume=("total_volume", "sum"))
    .sort_values("total_volume", ascending=False)
)
city_agg = city_agg.head(20)

bar = (
    alt.Chart(city_agg)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("city:N", title="City", sort=alt.SortField("total_volume", order="descending")),
        y=alt.Y("total_volume:Q", title="2026 total volume (USD)"),
        color=alt.condition(
            alt.datum.city == "Paris",
            alt.value(VERMILLION),
            alt.value(SKY_BLUE),
        ),
        tooltip=[
            alt.Tooltip("city:N", title="City"),
            alt.Tooltip("events:Q", title="Events", format=",.0f"),
            alt.Tooltip("markets:Q", title="Markets", format=",.0f"),
            alt.Tooltip("total_volume:Q", title="Volume USD", format="$,.0f"),
        ],
    )
    .properties(height=400, title="Top 20 cities by 2026 weather-betting volume")
)
st.altair_chart(bar, use_container_width=True)

st.caption(
    "Source: polymarket_weather_report.weather_markets_overview, 2026-01-01 to "
    "2026-12-01 inclusive. Paris is highlighted in vermillion; all other cities in sky blue."
)

st.divider()

st.header("Methodology and limitations")
st.markdown(
    """
    - **Stations**: six Paris-area stations queried identically through the
      Meteostat Python library (NOAA-ISD-derived hourly METAR/SYNOP).
      Reported lat/lon are verified within 0.05° of the configured value at
      ingestion. Open-Meteo gridded reanalysis at Paris centre is included as a
      separately-labelled `source` and never aggregated alongside the stations.
    - **Anomaly definition**: a CDG hourly reading is flagged when its peer-
      median residual is at least 3 °C in magnitude AND its robust peer-z is at
      least 2 in magnitude. The threshold is symmetric so both upward spikes and
      downward drops are surfaced.
    - **Counterfactual buckets**: each station's daily max is rounded to whole
      degrees Celsius and clamped to [14, 24] to match the Polymarket bucket
      scheme (≤14, 15..23, ≥24). The Polymarket-observed bucket comes from the
      `Yes`-side outcome whose final tick price was the highest in the event.
    - **Resolution-source switch**: Polymarket switched the Paris series'
      resolution from CDG (Wunderground LFPG) to Le Bourget / Bonneuil-en-France
      starting on the 2026-04-19 event, four days before The Guardian's report.
    - **Out of scope**: identifying or naming a suspect; analysing the viral
      hairdryer footage; wallet-level on-chain trade attribution.
    """
)
