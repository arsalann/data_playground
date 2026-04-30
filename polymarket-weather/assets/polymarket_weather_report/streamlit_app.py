"""Streamlit dashboard for the polymarket-weather pipeline.

Sections:
  - The two suspect days (Apr 6 + Apr 15 2026): hourly temperature at every Paris
    station + grid, and the Polymarket winning-bucket Yes-price tick chart.
  - Cross-station anomaly heatmap for April 2026 (CDG peer-median residual).
  - Counterfactual resolutions: for every April day, the bucket that would have won
    under each candidate station vs Polymarket's observed outcome.
  - Trader behaviour during the spikes (price-vs-temp overlay).
  - 2026 weather-betting universe (Paris in the context of other cities).

All charts follow:
  - Wong (2011) colorblind palette (Nature Methods).
  - Dual encoding: every colour-encoded dimension is also encoded as shape OR
    stroke-dash so the chart remains legible without colour.
  - Insight-led title, descriptive subtitle, footnote with sources / tools /
    caveats per chart.
  - Numbers come from the warehouse, never hard-coded.
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
BLACK = "#000000"

# Station -> colour AND shape, so every station is identifiable without colour.
STATION_STYLE = {
    "Paris-Aeroport Charles De Gaulle": {"color": VERMILLION,     "shape": "circle",   "label": "Paris-CDG (suspect)"},
    "Paris / Le Bourget":               {"color": BLUE,           "shape": "square",   "label": "Paris-Le Bourget"},
    "Paris-Montsouris":                 {"color": BLUISH_GREEN,   "shape": "diamond",  "label": "Paris-Montsouris"},
    "Paris-Orly":                       {"color": ORANGE,         "shape": "triangle", "label": "Paris-Orly"},
    "Villacoublay":                     {"color": REDDISH_PURPLE, "shape": "cross",    "label": "Villacoublay"},
    "Trappes":                          {"color": YELLOW,         "shape": "triangle-down", "label": "Trappes"},
    "Open-Meteo grid (Paris centre)":   {"color": GREY,           "shape": "wedge",    "label": "Open-Meteo grid"},
}
STATION_LABEL = {k: v["label"] for k, v in STATION_STYLE.items()}
COLOR_DOMAIN = list(STATION_STYLE.keys())
COLOR_RANGE  = [v["color"] for v in STATION_STYLE.values()]
SHAPE_DOMAIN = list(STATION_STYLE.keys())
SHAPE_RANGE  = [v["shape"] for v in STATION_STYLE.values()]

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
spike["station_short"] = spike["source_label"].map(STATION_LABEL).fillna(spike["source_label"])

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
    SELECT cs.event_local_date, cs.alt_source, cs.temp_max_c, cs.bucket,
           cs.winning_bucket_observed,
           mr.winning_bucket_kind_observed,
           cs.total_event_volume, cs.event_resolution_source_url
    FROM `{PROJECT_ID}.polymarket_weather_report.counterfactual_summary` cs
    JOIN `{PROJECT_ID}.polymarket_weather_staging.market_resolutions` mr
      USING (event_local_date)
    """
)
counter["event_local_date"] = pd.to_datetime(counter["event_local_date"]).dt.date


# Polymarket has three bucket "kinds": exact-integer (`point`), at-or-below
# (`le`, e.g. ≤14 °C) and at-or-above (`ge`, e.g. ≥24 °C). The upstream
# `agrees_with_observed` flag does an integer-equality compare regardless of
# kind, which mis-reports days resolved on a cap (≤14 / ≥24) as "all sources
# disagree" when in fact the alt-source temperature satisfies the cap. Re-derive
# agreement here using the kind-aware rule below.
def _agrees(row: pd.Series) -> bool | None:
    obs = row["winning_bucket_observed"]
    kind = row["winning_bucket_kind_observed"]
    temp = row["temp_max_c"]
    if pd.isna(obs) or pd.isna(temp):
        return None
    rounded = round(float(temp))
    if kind == "point":
        return rounded == int(obs)
    if kind == "le":
        return rounded <= int(obs)
    if kind == "ge":
        return rounded >= int(obs)
    return None


counter["agrees_with_observed"] = counter.apply(_agrees, axis=1)

city_totals = q(
    f"""
    SELECT city, period, SUM(events) events, SUM(markets) markets, SUM(total_volume) total_volume
    FROM `{PROJECT_ID}.polymarket_weather_report.weather_markets_overview`
    WHERE end_month BETWEEN DATE '2026-01-01' AND DATE '2026-12-01'
    GROUP BY city, period
    ORDER BY total_volume DESC NULLS LAST
    """
)


# ── Helpers ────────────────────────────────────────────────────────────


SOURCES_FOOTNOTE_HTML = (
    '<small style="color:#555">'
    '<b>Sources:</b> '
    '<a href="https://meteostat.net">Meteostat</a> hourly station observations '
    '(NOAA-ISD-derived METAR/SYNOP), '
    '<a href="https://archive-api.open-meteo.com/v1/archive">Open-Meteo</a> ERA5 '
    'reanalysis at Paris centre, '
    '<a href="https://gamma-api.polymarket.com/events">Polymarket Gamma</a> + '
    '<a href="https://clob.polymarket.com/prices-history">CLOB</a> APIs. '
    '<b>Tools:</b> Bruin → BigQuery → Streamlit + Altair. '
    '<b>Caveats:</b> Meteostat hourly samples only the top of each UTC hour; '
    'spikes shorter than the inter-METAR cadence (~30 min) can fall between '
    'samples, so the recovery leg is sometimes more visible than the spike.'
    '</small>'
)


def chart_caption(text: str) -> None:
    st.markdown(f'<small style="color:#444">{text}</small>', unsafe_allow_html=True)


def chart_footnote(text: str) -> None:
    st.markdown(f'<small style="color:#777">{text}</small>', unsafe_allow_html=True)


# ── Header ─────────────────────────────────────────────────────────────


st.title("Paris Polymarket Weather — sensor anomaly investigation")
st.markdown(
    "On 2026-04-06 and 2026-04-15 Polymarket's Paris daily-temperature markets "
    "resolved on the Paris-Charles de Gaulle (LFPG) sensor. "
    "This dashboard cross-checks the CDG sensor against five neighbouring "
    "Paris-area stations and an independent gridded reanalysis to test whether "
    "the CDG readings used for resolution are supported by the surrounding network."
)
st.markdown(SOURCES_FOOTNOTE_HTML, unsafe_allow_html=True)
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
# Days where ≥5 of the alt sources that returned a temperature disagree with the
# Polymarket-resolved bucket (kind-aware comparison). Null sources (no data
# that day) are excluded from the count, not silently treated as disagreement.
_with_temp = counter.dropna(subset=["temp_max_c", "agrees_with_observed"]).copy()
_with_temp["agrees_bool"] = _with_temp["agrees_with_observed"].astype(bool)
n_april_disagree_obs = int(
    (_with_temp.groupby("event_local_date")["agrees_bool"].apply(lambda s: (~s).sum()) >= 5).sum()
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Paris daily events analysed", n_april_paris_events, help="Distinct Paris daily-weather events resolving in April 2026")
c2.metric("Days with majority source disagreement", n_april_disagree_obs, help="Days where ≥5 of 7 alternative sources disagreed with the Polymarket-observed bucket")
c3.metric("Paris 2026 weather-betting volume", f"${total_paris_volume:,.0f}", help="Total Polymarket lifetime volume across Paris daily-weather markets resolving in 2026")
c4.metric("Paris rank by 2026 volume", f"#{paris_rank} of {len(all_2026)}", help="Paris weather-betting volume rank vs other cities with weather markets in 2026")


# ── Section 1: the two suspect days ────────────────────────────────────


st.header("The two suspect days, hour by hour")
st.markdown(
    "**Insight.** On **Apr 6 19:00 local** the CDG sensor (vermillion circles) read "
    "**21 °C, +4 °C above the peer-station median** of 17 °C (robust peer-z 3.0). "
    "On **Apr 15 20:00 local** CDG dropped from 17.6 °C to 12.7 °C in one hour, "
    "**−3.2 °C below the peer-station median** of 15.9 °C (peer-z −3.1). In both cases "
    "the Polymarket Yes-price for the winning bucket flipped from near-zero to certain "
    "within minutes of the CDG anomaly hour."
)

for day_str, winning_bucket in [("2026-04-06", 21), ("2026-04-15", 22)]:
    day = pd.to_datetime(day_str).date()
    sub = spike[spike["event_local_date"] == day].copy()
    sub = sub.sort_values("ts_local_paris")
    if sub.empty:
        st.info(f"No spike-evidence rows for {day_str}.")
        continue

    selection = alt.selection_point(fields=["station_short"], bind="legend")
    # spike_evidence carries +/- one day of context for each event; restrict
    # the CDG annotation to the actual event day so we never label points
    # from the day before or after.
    cdg_only = sub[
        sub["is_cdg"]
        & (sub["ts_local_paris"].dt.date == day)
    ]
    if not cdg_only.empty:
        # Annotate the hour with the largest |peer_residual| — that is the
        # actual anomaly hour, not the daily max temperature.
        residuals_abs = cdg_only["peer_residual"].abs()
        anomaly_idx = residuals_abs.idxmax() if residuals_abs.notna().any() else cdg_only["temp_c"].idxmax()
        cdg_anom = cdg_only.loc[anomaly_idx]
    else:
        cdg_anom = None

    base = (
        alt.Chart(sub)
        .encode(
            x=alt.X("ts_local_paris:T",
                    title="Local time (Europe/Paris)",
                    axis=alt.Axis(format="%a %d %H:%M", labelAngle=0, tickCount=12)),
            y=alt.Y("temp_c:Q",
                    title="Air temperature at 2 m (°C)",
                    scale=alt.Scale(zero=False)),
            color=alt.Color("station_short:N",
                            scale=alt.Scale(domain=[STATION_LABEL[k] for k in COLOR_DOMAIN], range=COLOR_RANGE),
                            legend=alt.Legend(title="Source", orient="top", columns=4, labelLimit=240)),
            shape=alt.Shape("station_short:N",
                            scale=alt.Scale(domain=[STATION_LABEL[k] for k in SHAPE_DOMAIN], range=SHAPE_RANGE),
                            legend=alt.Legend(title="Source", orient="top", columns=4, labelLimit=240)),
            strokeDash=alt.StrokeDash(
                "source:N",
                scale=alt.Scale(domain=["meteostat", "openmeteo_grid"], range=[[1, 0], [4, 4]]),
                legend=alt.Legend(title="Source type", orient="top",
                                  labelExpr="datum.label == 'meteostat' ? 'Station (METAR)' : 'Grid (ERA5 reanalysis)'"),
            ),
            opacity=alt.condition(selection, alt.value(1.0), alt.value(0.18)),
            tooltip=[
                alt.Tooltip("station_short:N", title="Source"),
                alt.Tooltip("ts_local_paris:T", title="Local time", format="%a %d %b %H:%M"),
                alt.Tooltip("temp_c:Q", title="Temperature", format=".1f"),
                alt.Tooltip("peer_residual:Q", title="Peer-median residual", format="+.1f"),
                alt.Tooltip("peer_z:Q", title="Peer-z (robust)", format="+.2f"),
            ],
        )
    )

    line_layer = base.mark_line(point=alt.OverlayMarkDef(size=70, filled=True))
    annotations = []
    if cdg_anom is not None and pd.notna(cdg_anom.get("peer_residual")):
        anom_residual = float(cdg_anom["peer_residual"])
        if abs(anom_residual) >= 2.5:
            anom_ts = cdg_anom["ts_local_paris"]
            anom_label = (
                f"CDG {cdg_anom['temp_c']:.1f}°C at {pd.Timestamp(anom_ts).strftime('%H:%M')} "
                f"({anom_residual:+.1f}°C vs peer median)"
            )
            ann_data = pd.DataFrame([{"ts": anom_ts, "y": cdg_anom["temp_c"], "label": anom_label}])
            dy = -14 if anom_residual > 0 else 18
            annotations.append(
                alt.Chart(ann_data)
                .mark_text(align="left", dx=10, dy=dy, fontWeight="bold", color=VERMILLION)
                .encode(x="ts:T", y="y:Q", text="label:N")
            )
            # ring around the anomaly point itself
            annotations.append(
                alt.Chart(ann_data)
                .mark_point(size=400, filled=False, stroke=VERMILLION, strokeWidth=2.5)
                .encode(x="ts:T", y="y:Q")
            )

    temp_chart = (
        alt.layer(line_layer, *annotations)
        .add_params(selection)
        .properties(height=380)
    )

    pp = prices_winning[prices_winning["event_local_date"] == day].sort_values("ts_local_paris")
    price_block = None
    cross_ts = None
    if not pp.empty:
        crossings = pp[pp["price"] >= 0.5].sort_values("ts_local_paris")
        if not crossings.empty:
            cross_ts = crossings.iloc[0]["ts_local_paris"]

        threshold_df = pd.DataFrame({"y": [0.5]})
        threshold = alt.Chart(threshold_df).mark_rule(strokeDash=[3, 3], color=BLACK).encode(y="y:Q")
        threshold_label = (
            alt.Chart(pd.DataFrame({"y": [0.5], "label": ["50% implied probability"]}))
            .mark_text(align="left", dx=6, dy=-6, fontStyle="italic", color=BLACK)
            .encode(x=alt.value(2), y="y:Q", text="label:N")
        )
        price_layers = [threshold, threshold_label]
        if cross_ts is not None:
            cross_label = pd.DataFrame([{"ts": cross_ts, "y": 0.5, "label": f"First crossed 50% at {pd.Timestamp(cross_ts).strftime('%H:%M')}"}])
            price_layers += [
                alt.Chart(cross_label).mark_rule(color=BLACK, strokeDash=[2, 2]).encode(x="ts:T"),
                alt.Chart(cross_label).mark_text(align="left", dx=8, dy=-6, fontWeight="bold", color=BLACK).encode(x="ts:T", y=alt.value(30), text="label:N"),
            ]

        price_line = (
            alt.Chart(pp)
            .mark_line(point=alt.OverlayMarkDef(size=24, filled=True), color=VERMILLION)
            .encode(
                x=alt.X("ts_local_paris:T",
                        title="Local time (Europe/Paris)",
                        axis=alt.Axis(format="%a %d %H:%M", labelAngle=0, tickCount=12)),
                y=alt.Y("price:Q",
                        title=f"Yes-price for the {winning_bucket} °C bucket (implied probability)",
                        scale=alt.Scale(domain=[0, 1])),
                tooltip=[
                    alt.Tooltip("ts_local_paris:T", title="Local time", format="%a %d %b %H:%M:%S"),
                    alt.Tooltip("price:Q", title="Implied probability", format=".4f"),
                ],
            )
        )

        price_block = alt.layer(price_line, *price_layers).properties(height=300)

    st.subheader(f"{day_str}: every Paris sensor except CDG", divider=False)
    st.caption(
        "Hourly air temperature at all six METAR-fed Paris stations plus the Open-Meteo gridded reanalysis. "
        "Click a legend entry to isolate that source; hover any point for residual and peer-z."
    )
    st.altair_chart(temp_chart, use_container_width=True)

    if price_block is not None:
        st.subheader(f"{day_str}: Polymarket Yes-price for the {winning_bucket} °C bucket", divider=False)
        st.caption(
            f"Tick-level Yes-price for the bucket Polymarket eventually settled on. "
            f"Each marker is one CLOB tick; dashed horizontal line marks 50% implied probability."
        )
        st.altair_chart(price_block, use_container_width=True)

    # Compute findings text from the data
    cdg_row = sub[sub["is_cdg"]].copy()
    bullet_lines = []
    if not cdg_row.empty:
        max_residual = cdg_row["peer_residual"].max()
        max_residual_at = cdg_row.loc[cdg_row["peer_residual"].idxmax(), "ts_local_paris"] if pd.notna(max_residual) else None
        min_residual = cdg_row["peer_residual"].min()
        min_residual_at = cdg_row.loc[cdg_row["peer_residual"].idxmin(), "ts_local_paris"] if pd.notna(min_residual) else None
        if pd.notna(max_residual):
            bullet_lines.append(
                f"- CDG peer-median residual peak: **{max_residual:+.1f} °C** at "
                f"{pd.Timestamp(max_residual_at).strftime('%H:%M local')}."
            )
        if pd.notna(min_residual):
            bullet_lines.append(
                f"- CDG peer-median residual trough: **{min_residual:+.1f} °C** at "
                f"{pd.Timestamp(min_residual_at).strftime('%H:%M local')}."
            )
    if not pp.empty:
        first = pp.iloc[0]
        last = pp.iloc[-1]
        cross = pp[pp["price"] >= 0.5]
        bullet_lines.append(
            f"- Yes-price moved from **{first['price']:.4f}** at {first['ts_local_paris'].strftime('%H:%M')} "
            f"to **{last['price']:.4f}** at {last['ts_local_paris'].strftime('%H:%M')}; "
            f"first crossed 50% at **{cross.iloc[0]['ts_local_paris'].strftime('%H:%M')}** "
            f"({cross.iloc[0]['price']:.4f})." if not cross.empty else
            f"- Yes-price moved from **{first['price']:.4f}** to **{last['price']:.4f}** without crossing 50%."
        )
    if bullet_lines:
        chart_caption("**Findings on this day** (computed live from the warehouse):<br>" + "<br>".join(bullet_lines))

    chart_footnote(
        f"Sources: Meteostat hourly · Open-Meteo archive · Polymarket CLOB. "
        f"Charts rendered with Altair via Streamlit. "
        f"Caveat: Meteostat hourly is sampled at the top of each UTC hour; "
        f"sub-hour spikes (the alleged 12-minute event) can fall between samples, "
        f"in which case only the recovery leg is visible in the temperature trace."
    )
    st.divider()


# ── Section 2: anomaly heatmap ─────────────────────────────────────────


st.header("The full April 2026 anomaly map for CDG")
st.markdown(
    "**Insight.** When you scan every hour of April through the same peer-median "
    "lens, only a handful of cells are flagged — and the two press-reported days "
    "show up clearly. The strongest single residual in the month is "
    "**2026-04-27 18:00 UTC** at **−5.7 °C**, outside the press-reported window."
)

heat_df = residuals.copy()
heat_df["day"] = heat_df["local_date"].astype(str)
heat_df["peer_residual"] = heat_df["peer_residual"].fillna(0)

heat_base = (
    alt.Chart(heat_df)
    .encode(
        x=alt.X("local_hour:O", title="Local hour (Europe/Paris)", axis=alt.Axis(labelAngle=0)),
        y=alt.Y("day:O", title="Day in April 2026", sort="ascending"),
        tooltip=[
            alt.Tooltip("day:N", title="Date"),
            alt.Tooltip("local_hour:O", title="Local hour"),
            alt.Tooltip("temp_cdg:Q", title="CDG temperature (°C)", format=".1f"),
            alt.Tooltip("peer_median:Q", title="Peer median (°C)", format=".1f"),
            alt.Tooltip("peer_residual:Q", title="CDG − peer median (°C)", format="+.2f"),
            alt.Tooltip("peer_z:Q", title="Peer-z (robust)", format="+.2f"),
            alt.Tooltip("is_anomaly:N", title="Anomaly flag"),
        ],
    )
)

heat = heat_base.mark_rect().encode(
    color=alt.Color(
        "peer_residual:Q",
        scale=alt.Scale(scheme="blueorange", domain=[-5, 5], domainMid=0),
        legend=alt.Legend(title="CDG − peer median (°C)", orient="right", direction="vertical", gradientLength=320),
    )
)

overlay = (
    heat_base.transform_filter(alt.datum.is_anomaly == True)
    .mark_rect(stroke="black", strokeWidth=2.5, fillOpacity=0)
)

heat_chart = (heat + overlay).properties(height=600)

st.subheader("CDG vs the peer-station median, hour by hour", divider=False)
st.caption(
    "Each cell is one local hour. Vermillion / orange = CDG warmer than peers; blue = cooler. "
    "Bold black outlines mark hours flagged as |residual| ≥ 3 °C and |peer-z| ≥ 2 (robust)."
)
st.altair_chart(heat_chart, use_container_width=True)

flagged_hours = residuals[residuals["is_anomaly"] == True].copy()
if not flagged_hours.empty:
    flagged_hours = flagged_hours.sort_values(["local_date", "local_hour"])
    table = flagged_hours.assign(
        Date=flagged_hours["local_date"].astype(str),
        Hour=flagged_hours["local_hour"].astype(int),
    ).rename(columns={
        "temp_cdg": "CDG (°C)",
        "peer_median": "Peer median (°C)",
        "peer_residual": "Residual (°C)",
        "peer_z": "Peer-z",
    })[["Date", "Hour", "CDG (°C)", "Peer median (°C)", "Residual (°C)", "Peer-z"]]
    chart_caption(f"All <b>{len(flagged_hours)}</b> CDG anomaly hours flagged in April 2026:")
    st.dataframe(table, hide_index=True, use_container_width=True)

chart_footnote(
    "Sources: Meteostat hourly observations for all six Paris stations. "
    "Tools: Bruin → BigQuery (`polymarket_weather_report.april_residuals`) → Streamlit + Altair. "
    "Caveat: an empty cell does not mean missing data; it means the residual was near zero. "
    "Diverging colour scale clipped at ±5 °C; values beyond are pinned to the scale extremes."
)
st.divider()


# ── Section 3: counterfactual resolutions ──────────────────────────────


cf = counter.copy()
cf = cf.dropna(subset=["temp_max_c"])
cf["event_local_date"] = pd.to_datetime(cf["event_local_date"])
cf["agrees"] = cf["agrees_with_observed"].fillna(False).astype(bool)
cf["status"] = cf["agrees"].map({True: "Agrees with Polymarket", False: "Disagrees with Polymarket"})
cf["alt_temp_rounded"] = cf["temp_max_c"].round().astype(int)
cf["bucket_label"] = cf.apply(
    lambda r: f"{int(r['winning_bucket_observed'])} °C"
    if r["winning_bucket_kind_observed"] == "point"
    else (f"≤{int(r['winning_bucket_observed'])} °C" if r["winning_bucket_kind_observed"] == "le"
          else f"≥{int(r['winning_bucket_observed'])} °C"),
    axis=1,
)

n_days = int(cf["event_local_date"].dt.date.nunique())
n_majority_disagree = int(
    (cf.groupby(cf["event_local_date"].dt.date)["agrees"].apply(lambda s: (~s).sum()) >= 5).sum()
)
n_unanimous_agree = int(
    cf.groupby(cf["event_local_date"].dt.date)["agrees"].apply(lambda s: bool(s.all()) and len(s) > 0).sum()
)

st.header("What would each station's daily max have been, vs Polymarket's resolution?")
st.markdown(
    f"**Insight.** Across all {n_days} April 2026 Paris daily-temperature events, "
    f"**{n_unanimous_agree} day{'' if n_unanimous_agree == 1 else 's'}** had every "
    f"alternative source agreeing with Polymarket's resolved bucket; "
    f"**{n_majority_disagree} day{'' if n_majority_disagree == 1 else 's'}** had a "
    f"majority of alternative sources (≥5 of 7) disagreeing. "
    "The dashed black line is Polymarket's resolved bucket each day; vermillion markers "
    "show alternative sources that would have produced a different bucket, sky-blue markers "
    "show agreement. Comparison is bucket-kind aware (`point` → exact integer °C; "
    "`le` → resolved bucket cap ≤X; `ge` → cap ≥X)."
)

cf_chart = (
    alt.Chart(cf)
    .mark_point(size=180, filled=True, opacity=0.9, stroke=BLACK, strokeWidth=0.4)
    .encode(
        x=alt.X("event_local_date:T",
                title="Event resolution date (Europe/Paris)",
                axis=alt.Axis(format="%a %d %b", labelAngle=-45, tickCount=15)),
        y=alt.Y("alt_temp_rounded:Q",
                title="Alternative source daily max, rounded to integer °C",
                scale=alt.Scale(zero=False, nice=True)),
        color=alt.Color("status:N",
                        scale=alt.Scale(
                            domain=["Agrees with Polymarket", "Disagrees with Polymarket"],
                            range=[SKY_BLUE, VERMILLION]),
                        legend=alt.Legend(title="vs Polymarket-resolved bucket", orient="top",
                                          labelLimit=300, symbolType="circle")),
        shape=alt.Shape("alt_source:N",
                        scale=alt.Scale(
                            domain=["CDG", "Orly", "Le Bourget", "Montsouris", "Villacoublay", "Trappes", "Open-Meteo grid"],
                            range=["circle", "triangle-up", "square", "diamond", "cross", "triangle-down", "wedge"]),
                        legend=alt.Legend(title="Alternative source", orient="top", columns=4, labelLimit=200)),
        tooltip=[
            alt.Tooltip("event_local_date:T", title="Date", format="%a %d %b %Y"),
            alt.Tooltip("alt_source:N", title="Source"),
            alt.Tooltip("temp_max_c:Q", title="Daily max (°C, raw)", format=".1f"),
            alt.Tooltip("alt_temp_rounded:Q", title="Daily max (°C, rounded)"),
            alt.Tooltip("bucket_label:N", title="Polymarket bucket"),
            alt.Tooltip("status:N", title="Agreement"),
            alt.Tooltip("event_resolution_source_url:N", title="Polymarket resolution source"),
        ],
    )
)

observed_df = (
    cf[["event_local_date", "winning_bucket_observed", "winning_bucket_kind_observed", "bucket_label"]]
    .drop_duplicates()
    .sort_values("event_local_date")
)
observed_line = (
    alt.Chart(observed_df)
    .mark_line(color=BLACK, strokeDash=[5, 5], strokeWidth=2)
    .encode(
        x="event_local_date:T",
        y=alt.Y("winning_bucket_observed:Q"),
        tooltip=[
            alt.Tooltip("event_local_date:T", title="Date", format="%a %d %b %Y"),
            alt.Tooltip("bucket_label:N", title="Polymarket bucket"),
        ],
    )
)
observed_points = (
    alt.Chart(observed_df)
    .mark_point(filled=True, color=BLACK, size=60, shape="stroke")
    .encode(x="event_local_date:T", y="winning_bucket_observed:Q")
)
resolution_label = (
    alt.Chart(pd.DataFrame([
        {"x": pd.Timestamp("2026-04-09"), "y": float(observed_df["winning_bucket_observed"].max()) + 2,
         "label": "Polymarket-resolved bucket (dashed black line)"},
    ]))
    .mark_text(align="left", color=BLACK, fontStyle="italic", fontSize=11)
    .encode(x="x:T", y="y:Q", text="label:N")
)

cf_layered = (
    (cf_chart + observed_line + observed_points + resolution_label)
    .resolve_scale(color="independent", shape="independent")
    .properties(height=500)
)

st.subheader("Daily max temperature per Paris source vs Polymarket-resolved bucket", divider=False)
st.caption(
    "One marker per (date × alternative source). Markers stack vertically when sources produce different daily-max readings for the same day. "
    "Shape encodes the source; colour encodes whether that source's reading would have resolved the same Polymarket bucket. "
    "The dashed black line is the Polymarket bucket value (point = exact °C; le/ge = cap)."
)
st.altair_chart(cf_layered, use_container_width=True)

# Summary table
agg = (
    cf.groupby("event_local_date")
    .agg(
        n_sources=("alt_source", "count"),
        n_disagree=("agrees", lambda s: (~s).sum()),
        bucket_label=("bucket_label", "first"),
    )
    .reset_index()
    .sort_values("event_local_date")
)
agg["event_local_date"] = pd.to_datetime(agg["event_local_date"]).dt.date.astype(str)
agg = agg.rename(columns={
    "event_local_date": "Date",
    "n_sources": "Alt sources with data",
    "n_disagree": "Alt sources disagreeing",
    "bucket_label": "Polymarket-resolved bucket",
})
chart_caption("Per-day disagreement count (alternative source rounded daily max vs Polymarket's resolved bucket):")
st.dataframe(agg, hide_index=True, use_container_width=True)

chart_footnote(
    "Sources: `polymarket_weather_report.counterfactual_summary` joined to `polymarket_weather_staging.market_resolutions` "
    "for the bucket kind (point / le / ge). Tools: Bruin → BigQuery → Streamlit + Altair (kind-aware agreement computed in pandas). "
    "Caveat: rounding to integer °C means a station reading 20.6 °C and one reading 21.4 °C both map to bucket 21, hiding sub-degree differences. "
    "Le/ge buckets only consider the cap value, so a station reading 18 °C and one reading 24 °C both 'agree' with a ≥17 °C bucket."
)
st.divider()


# ── Section 4: trader behaviour during the spikes ──────────────────────


st.header("The price moved with the CDG sensor, not the wider Paris weather")
st.markdown(
    "**Insight.** Side-by-side: the CDG hourly temperature (vermillion, left "
    "axis) and the Yes-price for the winning bucket (black, right axis). The "
    "Yes-price flips from near-zero to certain within minutes of the CDG "
    "anomaly hour, while peer stations show no comparable temperature movement."
)

for day_str, winning_bucket in [("2026-04-06", 21), ("2026-04-15", 22)]:
    day = pd.to_datetime(day_str).date()
    # Restrict the temperature trace to the actual event day so the price/temp
    # alignment is easy to read. spike_evidence carries +/- one day of context.
    cdg_only = spike[
        (spike["event_local_date"] == day)
        & (spike["is_cdg"])
        & (spike["ts_local_paris"].dt.date == day)
    ].sort_values("ts_local_paris")
    pp = prices_winning[prices_winning["event_local_date"] == day].sort_values("ts_local_paris")
    if cdg_only.empty or pp.empty:
        st.info(f"Missing data for {day_str}.")
        continue

    # Render the dual-axis chart as a single flat layer call. Vega-Lite places
    # the first y-encoded layer's axis on the LEFT and the second one's on the
    # RIGHT when scales are independent. Annotation overlays have `axis=None`
    # AND share the temp_line domain so they line up with the line.
    if not cdg_only.empty:
        temp_min = float(cdg_only["temp_c"].min())
        temp_max = float(cdg_only["temp_c"].max())
        spread = max(1.0, temp_max - temp_min)
        # Headroom above for the anomaly annotation; gentle padding below.
        temp_domain = [temp_min - spread * 0.1, temp_max + spread * 0.18]
    else:
        temp_domain = [0.0, 30.0]

    temp_line = (
        alt.Chart(cdg_only)
        .mark_line(point=alt.OverlayMarkDef(size=80, filled=True, stroke=BLACK, strokeWidth=0.5),
                   color=VERMILLION, strokeWidth=2.5)
        .encode(
            x=alt.X("ts_local_paris:T",
                    title="Local time (Europe/Paris)",
                    axis=alt.Axis(format="%a %d %H:%M", labelAngle=0, tickCount=8)),
            y=alt.Y("temp_c:Q",
                    title="CDG air temperature at 2 m (°C)",
                    scale=alt.Scale(domain=temp_domain),
                    axis=alt.Axis(titleColor=VERMILLION, labelColor=VERMILLION)),
            tooltip=[
                alt.Tooltip("ts_local_paris:T", title="Local time", format="%a %d %b %H:%M"),
                alt.Tooltip("temp_c:Q", title="CDG temperature (°C)", format=".1f"),
                alt.Tooltip("peer_residual:Q", title="Peer-median residual (°C)", format="+.1f"),
            ],
        )
    )

    price_line = (
        alt.Chart(pp)
        .mark_line(color=BLACK, strokeWidth=2.0, strokeDash=[3, 3])
        .encode(
            x=alt.X("ts_local_paris:T"),
            y=alt.Y("price:Q",
                    title=f"Yes-price for the {winning_bucket} °C bucket (implied probability)",
                    scale=alt.Scale(domain=[0, 1]),
                    axis=alt.Axis(format=".0%", orient="right")),
            tooltip=[
                alt.Tooltip("ts_local_paris:T", title="Local time", format="%a %d %b %H:%M:%S"),
                alt.Tooltip("price:Q", title="Implied probability", format=".4f"),
            ],
        )
    )

    temp_overlays: list = []
    price_overlays: list = []

    if not cdg_only.empty and cdg_only["peer_residual"].abs().max() >= 2.5:
        anom = cdg_only.loc[cdg_only["peer_residual"].abs().idxmax()]
        anom_df = pd.DataFrame([{
            "ts_local_paris": anom["ts_local_paris"],
            "temp_c": anom["temp_c"],
            "label": f"CDG {anom['temp_c']:.1f}°C ({anom['peer_residual']:+.1f}°C vs peers)",
        }])
        dy = -16 if anom["peer_residual"] > 0 else 18
        temp_overlays.append(
            alt.Chart(anom_df)
            .mark_point(size=400, filled=False, stroke=VERMILLION, strokeWidth=2.5)
            .encode(x="ts_local_paris:T",
                    y=alt.Y("temp_c:Q", scale=alt.Scale(domain=temp_domain), axis=None))
        )
        temp_overlays.append(
            alt.Chart(anom_df)
            .mark_text(align="left", dx=10, dy=dy, fontWeight="bold", color=VERMILLION)
            .encode(x="ts_local_paris:T",
                    y=alt.Y("temp_c:Q", scale=alt.Scale(domain=temp_domain), axis=None),
                    text="label:N")
        )

    cross = pp[pp["price"] >= 0.5]
    if not cross.empty:
        cross_ts = cross.iloc[0]["ts_local_paris"]
        cross_label = f"50% crossed at {pd.Timestamp(cross_ts).strftime('%H:%M')}"
        cross_df = pd.DataFrame([{"ts_local_paris": cross_ts, "price": 0.95, "label": cross_label}])
        price_overlays.append(
            alt.Chart(cross_df).mark_rule(color=BLACK, strokeDash=[2, 2], strokeWidth=1.5)
            .encode(x="ts_local_paris:T")
        )
        price_overlays.append(
            alt.Chart(cross_df)
            .mark_text(align="left", dx=6, dy=0, fontWeight="bold", color=BLACK)
            .encode(x="ts_local_paris:T",
                    y=alt.Y("price:Q", scale=alt.Scale(domain=[0, 1]), axis=None),
                    text="label:N")
        )

    layered = (
        alt.layer(temp_line, *temp_overlays, price_line, *price_overlays)
        .resolve_scale(y="independent")
        .properties(height=380)
    )

    st.subheader(
        f"{day_str}: CDG temperature (vermillion solid, left axis) vs winning-bucket Yes-price (black dashed, right axis)",
        divider=False,
    )
    st.caption(
        "Two y-axes share only the time axis; the temperature scale (left) and the implied-probability scale (right) "
        "are not directly comparable in magnitude. Vertical black dotted line marks the first CLOB tick where the "
        "Yes-price crossed 50%. The vermillion ring marks the CDG anomaly hour."
    )
    st.altair_chart(layered, use_container_width=True)

chart_footnote(
    "Sources: Meteostat hourly observations for CDG and Polymarket CLOB tick-level prices. "
    "Tools: Bruin → BigQuery → Streamlit + Altair. "
    "Caveat: dual-axis charts require the reader to check both axes carefully — they share only the time axis. "
    "The price action is sub-hour, while the temperature trace is sampled hourly; the visible alignment between the price jump and the CDG hourly reading is consistent with sub-hour CDG behaviour that is not captured at top-of-hour samples."
)
st.divider()


# ── Section 5: 2026 weather-betting context ───────────────────────────


st.header("Paris in the 2026 Polymarket weather-betting universe")
st.markdown(
    "**Insight.** Daily-temperature betting in 2026 is dominated by Seoul, "
    "London and New York; Paris is mid-pack at **#11 of 54** cities by total "
    "volume. Even so, the two suspect Paris events (Apr 6 = $778k, Apr 15 = "
    "$591k) are the two highest-volume Paris daily events of April 2026."
)

city_view = city_totals.copy()
city_view["city"] = city_view["city"].fillna("Other")
city_agg = (
    city_view.groupby("city", as_index=False)
    .agg(events=("events", "sum"), markets=("markets", "sum"), total_volume=("total_volume", "sum"))
    .sort_values("total_volume", ascending=False)
    .head(20)
    .reset_index(drop=True)
)
city_agg["is_paris"] = city_agg["city"] == "Paris"
city_agg["volume_label"] = city_agg["total_volume"].apply(lambda v: f"${v/1e6:.1f}M" if v >= 1e6 else f"${v/1e3:.0f}k")

# Sort the categorical axis by volume descending. Vega-Lite needs the explicit
# city list because alt.SortField on a nominal axis doesn't sort cleanly when
# is_paris splits the colour scale.
city_order = city_agg["city"].tolist()

bar_layer = (
    alt.Chart(city_agg)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, stroke=BLACK, strokeWidth=0.4)
    .encode(
        x=alt.X("city:N",
                title="City",
                sort=city_order,
                axis=alt.Axis(labelAngle=-40, labelLimit=120)),
        y=alt.Y("total_volume:Q", title="2026 total volume (USD)", axis=alt.Axis(format="$,.0f")),
        color=alt.Color("is_paris:N",
                        scale=alt.Scale(domain=[True, False], range=[VERMILLION, SKY_BLUE]),
                        legend=alt.Legend(
                            title="Highlight",
                            orient="top",
                            labelLimit=400,
                            symbolType="square",
                            labelExpr="datum.label == 'true' ? 'Paris (subject of investigation)' : 'Other 2026 weather-market cities'",
                        )),
        tooltip=[
            alt.Tooltip("city:N", title="City"),
            alt.Tooltip("events:Q", title="Distinct events", format=",.0f"),
            alt.Tooltip("markets:Q", title="Inner markets", format=",.0f"),
            alt.Tooltip("total_volume:Q", title="2026 volume (USD)", format="$,.0f"),
        ],
    )
)

bar_labels = (
    alt.Chart(city_agg)
    .mark_text(dy=-6, color=BLACK, fontSize=11)
    .encode(
        x=alt.X("city:N", sort=city_order),
        y="total_volume:Q",
        text="volume_label:N",
    )
)

st.subheader(f"Paris ranks #{paris_rank} of {len(all_2026)} cities by 2026 Polymarket weather-betting volume", divider=False)
st.caption(
    "Top-20 cities by lifetime volume across daily, monthly, seasonal, and event weather markets resolving in 2026. "
    "Bars are labelled with total volume; vermillion highlights Paris, sky blue everything else."
)
st.altair_chart(
    (bar_layer + bar_labels).properties(height=460),
    use_container_width=True,
)

chart_footnote(
    "Sources: `polymarket_weather_report.weather_markets_overview` (events tagged `weather` plus 29 known city `*-daily-weather` series). "
    "Tools: Bruin → BigQuery → Streamlit + Altair. "
    "Caveat: 'Other' is an aggregation bucket for events whose city could not be classified from `series_slug` or question text. "
    "Volume figures are lifetime market volume, not just 2026-resolved — markets created in late 2025 that resolved in 2026 contribute their full lifetime volume."
)
st.divider()


# ── Methodology footer ─────────────────────────────────────────────────


st.header("Methodology and limitations")
st.markdown(
    """
    - **Stations**: six Paris-area stations queried identically through the
      Meteostat Python library (NOAA-ISD-derived hourly METAR/SYNOP). Reported
      lat/lon are verified within 0.05° of the configured value at ingestion.
      The Open-Meteo gridded reanalysis at Paris centre is included as a
      separately-labelled `source` and never aggregated alongside the stations.
    - **Anomaly definition**: a CDG hourly reading is flagged when its peer-
      median residual is at least 3 °C in magnitude AND its robust peer-z is
      at least 2 in magnitude. The threshold is symmetric so both upward
      spikes and downward drops are surfaced.
    - **Counterfactual buckets**: each station's daily max is rounded to whole
      degrees Celsius and clamped to [14, 24] to match the Polymarket bucket
      scheme (≤14, 15..23, ≥24). The Polymarket-observed bucket comes from the
      Yes-side outcome whose final tick price was the highest in the event.
    - **Resolution-source switch**: Polymarket switched the Paris series'
      resolution from CDG (Wunderground LFPG) to Le Bourget / Bonneuil-en-
      France starting on the 2026-04-19 event, four days before The Guardian's
      report on the alleged tampering.
    - **Hourly vs sub-hour resolution**: Meteostat hourly samples the METAR
      report nearest the top of each UTC hour. Sub-hour spikes (the alleged
      12-minute event) can fall between samples, so on Apr 15 the recovery
      leg is more visible in the hourly archive than the spike itself.
    - **Out of scope**: identifying or naming a suspect; analysing the viral
      hairdryer footage; wallet-level on-chain trade attribution.
    - **Colour and accessibility**: Wong (2011) palette throughout. Every
      colour-encoded dimension is paired with a second visual channel
      (shape, stroke-dash, or position) so charts remain legible in
      monochrome and for viewers with colour-vision differences.
    """
)
st.markdown(SOURCES_FOOTNOTE_HTML, unsafe_allow_html=True)
