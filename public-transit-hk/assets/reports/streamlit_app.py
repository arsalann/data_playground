from pathlib import Path

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="Hong Kong Public Transport Network", layout="wide")

PROJECT_ID = "bruin-playground-arsalan"

# ---------------------------------------------------------------------------
# IBM Carbon Design System — categorical palette for color-blind accessibility
# https://carbondesignsystem.com/data-visualization/color-palettes/
# ---------------------------------------------------------------------------
IBM_CYAN_50 = "#1192e8"
IBM_PURPLE_70 = "#6929c4"
IBM_TEAL_50 = "#009d9a"
IBM_MAGENTA_70 = "#9f1853"
IBM_RED_50 = "#fa4d56"
IBM_GREEN_60 = "#198038"
IBM_BLUE_80 = "#002d9c"
IBM_ORANGE_70 = "#8a3800"
IBM_PURPLE_50 = "#a56eff"
IBM_GRAY_50 = "#8d8d8d"

# Transport mode palette (4 categories)
MODE_COLORS = {
    "Bus": IBM_CYAN_50,
    "Tram": IBM_PURPLE_70,
    "Ferry": IBM_TEAL_50,
    "Funicular": IBM_MAGENTA_70,
}
MODE_DOMAIN = list(MODE_COLORS.keys())
MODE_RANGE = list(MODE_COLORS.values())


@st.cache_resource
def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)


@st.cache_data(ttl=86400)
def run_sql(sql: str) -> pd.DataFrame:
    return get_client().query(sql).to_dataframe()


def clean_stop_name(name: str) -> str:
    """Extract the cleanest stop name from GTFS concatenated format."""
    if not name:
        return name
    parts = str(name).split("|")
    for p in parts:
        if "[KMB]" in p:
            return p.split("]")[-1].strip()
    first = parts[0]
    if "]" in first:
        return first.split("]")[-1].strip()
    return first.strip()


# ---- Load data ----
kpi_data = run_sql("""
    SELECT
        (SELECT COUNT(DISTINCT stop_id) FROM staging.hk_transit_stops) AS total_stops,
        (SELECT COUNT(DISTINCT route_id) FROM staging.hk_transit_routes) AS total_routes,
        (SELECT COUNT(*) FROM staging.hk_transit_stop_times) AS total_departures,
        (SELECT COUNT(DISTINCT trip_id) FROM staging.hk_transit_trips) AS total_trips,
        (SELECT COUNT(DISTINCT station_id) FROM staging.hk_transit_mtr_stations) AS mtr_stations
""")

peak_hours = run_sql("""
    SELECT departure_hour, route_type_name, departure_count, distinct_routes, distinct_stops
    FROM marts.hk_transit_mart_peak_hour_analysis
    ORDER BY departure_hour
""")

busiest_stops = run_sql("""
    SELECT stop_name, total_departures, distinct_routes, bus_departures, tram_departures,
           ferry_departures, busy_rank
    FROM marts.hk_transit_mart_busiest_stops
    ORDER BY total_departures DESC
    LIMIT 20
""")

transfer_hubs = run_sql("""
    SELECT stop_name, stop_lat, stop_lon, distinct_route_count, total_departures, hub_rank
    FROM marts.hk_transit_mart_transfer_hubs
    ORDER BY distinct_route_count DESC
    LIMIT 20
""")

weekday_weekend = run_sql("""
    SELECT w.route_type_name,
           SUM(w.weekday_departures) AS weekday_departures,
           SUM(w.weekend_departures) AS weekend_departures,
           COUNT(DISTINCT w.route_id) AS route_count
    FROM marts.hk_transit_mart_weekday_vs_weekend w
    GROUP BY w.route_type_name
    ORDER BY weekday_departures DESC
""")

longest_routes = run_sql("""
    SELECT route_short_name, route_long_name, route_type_name, total_stops,
           max_stops_per_trip, total_trips
    FROM marts.hk_transit_mart_longest_routes
    ORDER BY total_stops DESC
    LIMIT 20
""")

stop_locations = run_sql("""
    SELECT s.stop_id, s.stop_name, s.stop_lat, s.stop_lon,
           COALESCE(b.total_departures, 0) AS total_departures,
           COALESCE(b.distinct_routes, 0) AS distinct_routes
    FROM staging.hk_transit_stops s
    LEFT JOIN marts.hk_transit_mart_busiest_stops b ON s.stop_id = b.stop_id
    WHERE s.stop_lat IS NOT NULL AND s.stop_lon IS NOT NULL
      AND s.stop_lat BETWEEN 22.1 AND 22.6
      AND s.stop_lon BETWEEN 113.8 AND 114.5
""")

mtr_stations = run_sql("""
    SELECT station_name_en, station_name_tc, station_type, lines_served,
           line_count, is_interchange
    FROM marts.hk_transit_mart_mtr_stations
    ORDER BY line_count DESC
""")

first_last = run_sql("""
    SELECT route_short_name, route_type_name, first_departure, last_departure,
           service_span_minutes, total_departures
    FROM marts.hk_transit_mart_first_last_service
    WHERE service_span_minutes IS NOT NULL
    ORDER BY service_span_minutes DESC
    LIMIT 20
""")


# =====================================================================
# DASHBOARD
# =====================================================================

st.title("Hong Kong Public Transport Network")

# ---- KPIs ----
k = kpi_data.iloc[0]
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Stops", f"{int(k.total_stops):,}")
c2.metric("Routes", f"{int(k.total_routes):,}")
c3.metric("Scheduled departures", f"{int(k.total_departures):,}")
c4.metric("Trips", f"{int(k.total_trips):,}")
c5.metric("MTR stations", f"{int(k.mtr_stations):,}")

st.divider()

# =====================================================================
# 1. Peak hour analysis
# =====================================================================
st.subheader("Scheduled departures by hour of day")

peak_total = (
    peak_hours.groupby("departure_hour")
    .agg({"departure_count": "sum"})
    .reset_index()
)
peak_total["period"] = peak_total["departure_hour"].apply(
    lambda h: "Peak (07-09, 17-19)" if h in (7, 8, 9, 17, 18, 19) else "Off-peak"
)

am_peak = int(peak_total[peak_total.departure_hour.isin([7, 8, 9])].departure_count.sum())
pm_peak = int(peak_total[peak_total.departure_hour.isin([17, 18, 19])].departure_count.sum())
total_dep = int(peak_total.departure_count.sum())

st.markdown(
    f"The six peak hours (07:00-09:00 and 17:00-19:00) account for "
    f"**{am_peak + pm_peak:,}** of **{total_dep:,}** total scheduled departures "
    f"(**{(am_peak + pm_peak) / total_dep:.0%}**). "
    f"The AM rush alone produces {am_peak:,} departures, "
    f"making 07:00-08:00 the single busiest hour across all modes."
)

bar = (
    alt.Chart(peak_total)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("departure_hour:O", title="Hour of day (0-23)", axis=alt.Axis(labelAngle=0)),
        y=alt.Y("departure_count:Q", title="Scheduled departures", scale=alt.Scale(zero=True)),
        color=alt.Color(
            "period:N",
            title="Period",
            scale=alt.Scale(
                domain=["Peak (07-09, 17-19)", "Off-peak"],
                range=[IBM_RED_50, IBM_CYAN_50],
            ),
            legend=alt.Legend(orient="top-right"),
        ),
        tooltip=[
            alt.Tooltip("departure_hour:O", title="Hour"),
            alt.Tooltip("departure_count:Q", title="Departures", format=","),
            alt.Tooltip("period:N", title="Period"),
        ],
    )
    .properties(height=380)
)
st.altair_chart(bar, use_container_width=True)

st.caption(
    "**Source:** GTFS stop_times from data.gov.hk. "
    "**Calculation:** Each row in stop_times represents one vehicle visiting one stop; "
    "the departure hour is extracted from the departure_time field (modulo 24 for post-midnight trips). "
    "Counts are summed across all modes (bus, tram, ferry). "
    "**Caveats:** GTFS represents scheduled, not actual, service. "
    "Post-midnight trips (e.g. 25:00) are mapped to hour 1."
)

st.divider()

# =====================================================================
# 2. Busiest stops
# =====================================================================
st.subheader("Top 20 stops by scheduled departure count")

busiest_stops["clean_name"] = busiest_stops["stop_name"].apply(clean_stop_name)

top_stop = busiest_stops.iloc[0]
st.markdown(
    f"**{clean_stop_name(top_stop.stop_name)}** leads with "
    f"**{int(top_stop.total_departures):,}** scheduled departures, "
    f"though it is served by only {int(top_stop.distinct_routes)} routes. "
    f"High departure counts with few routes indicate high-frequency terminus operations. "
    f"Stops with many routes but fewer total departures (visible in the transfer hubs chart below) "
    f"serve a different structural role as interchange nodes."
)

busiest_bar = (
    alt.Chart(busiest_stops)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("total_departures:Q", title="Scheduled departures"),
        y=alt.Y("clean_name:N", title=None, sort="-x"),
        color=alt.value(IBM_CYAN_50),
        tooltip=[
            alt.Tooltip("clean_name:N", title="Stop"),
            alt.Tooltip("total_departures:Q", title="Departures", format=","),
            alt.Tooltip("distinct_routes:Q", title="Routes served", format=","),
            alt.Tooltip("bus_departures:Q", title="Bus departures", format=","),
            alt.Tooltip("tram_departures:Q", title="Tram departures", format=","),
            alt.Tooltip("ferry_departures:Q", title="Ferry departures", format=","),
        ],
    )
    .properties(height=480)
)
st.altair_chart(busiest_bar, use_container_width=True)

st.caption(
    "**Source:** GTFS stop_times aggregated by stop_id, joined with stops for names. "
    "**Calculation:** Total departures = count of all stop_time records for a given stop across all trips. "
    "Includes bus, tram, and ferry departures. "
    "**Caveats:** Stop names in the GTFS feed concatenate multiple operator names separated by '|'. "
    "The dashboard extracts the primary name (KMB preferred) for display. "
    "Some physically adjacent stops may have separate stop_ids per operator."
)

st.divider()

# =====================================================================
# 3. Transfer hubs
# =====================================================================
st.subheader("Top 20 stops by number of distinct routes served")

transfer_hubs["clean_name"] = transfer_hubs["stop_name"].apply(clean_stop_name)

top_hub = transfer_hubs.iloc[0]
st.markdown(
    f"**{clean_stop_name(top_hub.stop_name)}** connects **{int(top_hub.distinct_route_count)}** "
    f"distinct routes, making it the most connected transit node in the GTFS network. "
    f"Cross-harbour tunnel bus-bus interchanges dominate this ranking because "
    f"Hong Kong's geography funnels traffic through a small number of harbour crossing points."
)

hub_bar = (
    alt.Chart(transfer_hubs)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("distinct_route_count:Q", title="Distinct routes"),
        y=alt.Y("clean_name:N", title=None, sort="-x"),
        color=alt.value(IBM_TEAL_50),
        tooltip=[
            alt.Tooltip("clean_name:N", title="Stop"),
            alt.Tooltip("distinct_route_count:Q", title="Routes", format=","),
            alt.Tooltip("total_departures:Q", title="Departures", format=","),
        ],
    )
    .properties(height=480)
)
st.altair_chart(hub_bar, use_container_width=True)

st.caption(
    "**Source:** GTFS stop_times joined with trips to resolve route_id per stop. "
    "**Calculation:** Distinct routes = COUNT(DISTINCT route_id) for all trips visiting a stop. "
    "**Caveats:** A stop with many routes is not necessarily high-volume; "
    "some interchange stops have many routes but infrequent service on each. "
    "MTR heavy rail is excluded (no GTFS data published by MTR)."
)

st.divider()

# =====================================================================
# 4. Weekday vs weekend service
# =====================================================================
st.subheader("Weekday vs. weekend departures by transport mode")

ww = weekday_weekend.copy()
ww_long = pd.melt(
    ww,
    id_vars=["route_type_name"],
    value_vars=["weekday_departures", "weekend_departures"],
    var_name="day_type",
    value_name="departures",
)
ww_long["day_type"] = ww_long["day_type"].map(
    {"weekday_departures": "Weekday", "weekend_departures": "Weekend"}
)

bus_wk = int(ww[ww.route_type_name == "Bus"].weekday_departures.sum())
bus_we = int(ww[ww.route_type_name == "Bus"].weekend_departures.sum())
ratio = bus_we / bus_wk if bus_wk > 0 else 0

st.markdown(
    f"Bus weekday departures total **{bus_wk:,}** vs. **{bus_we:,}** on weekends "
    f"(**{ratio:.0%}** of weekday volume). "
    f"The reduction is consistent across modes and reflects lower commuter demand on non-working days. "
    f"Ferry and tram services follow the same pattern."
)

ww_chart = (
    alt.Chart(ww_long)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("route_type_name:N", title="Transport mode", axis=alt.Axis(labelAngle=0)),
        y=alt.Y("departures:Q", title="Total departures", scale=alt.Scale(zero=True)),
        color=alt.Color(
            "day_type:N",
            title="Day type",
            scale=alt.Scale(
                domain=["Weekday", "Weekend"],
                range=[IBM_CYAN_50, IBM_PURPLE_70],
            ),
            legend=alt.Legend(orient="top-right"),
        ),
        xOffset="day_type:N",
        tooltip=[
            alt.Tooltip("route_type_name:N", title="Mode"),
            alt.Tooltip("day_type:N", title="Day type"),
            alt.Tooltip("departures:Q", title="Departures", format=","),
        ],
    )
    .properties(height=380)
)
st.altair_chart(ww_chart, use_container_width=True)

st.caption(
    "**Source:** GTFS trips joined with calendar.txt service patterns. "
    "**Calculation:** Weekday trips are those where monday through friday are all 1 in the calendar. "
    "Weekend trips have saturday=1 or sunday=1. "
    "Departures = SUM of stop_time records for matching trips. "
    "**Caveats:** A trip can be classified as both weekday and weekend if the calendar has all days set to 1. "
    "Holiday schedules are not separated from regular weekday/weekend patterns."
)

st.divider()

# =====================================================================
# 5. Longest routes by stop count
# =====================================================================
st.subheader("Top 20 routes by number of distinct stops served")

longest_routes["label"] = (
    longest_routes["route_short_name"] + " - " + longest_routes["route_long_name"].str[:50]
)

top_route = longest_routes.iloc[0]
st.markdown(
    f"Route **{top_route.route_short_name}** "
    f"({''.join(top_route.route_long_name[:60])}) "
    f"serves **{int(top_route.total_stops)}** distinct stops across all its trips, "
    f"with up to **{int(top_route.max_stops_per_trip)}** stops in a single trip direction. "
    f"Long routes with many stops typically serve cross-territory corridors connecting "
    f"New Territories towns to urban Kowloon and Hong Kong Island."
)

selection = alt.selection_point(fields=["route_type_name"], bind="legend")

longest_bar = (
    alt.Chart(longest_routes)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("total_stops:Q", title="Distinct stops"),
        y=alt.Y("label:N", title=None, sort="-x"),
        color=alt.Color(
            "route_type_name:N",
            title="Transport mode",
            scale=alt.Scale(domain=MODE_DOMAIN, range=MODE_RANGE),
            legend=alt.Legend(orient="top-right"),
        ),
        opacity=alt.condition(selection, alt.value(1.0), alt.value(0.2)),
        tooltip=[
            alt.Tooltip("route_short_name:N", title="Route"),
            alt.Tooltip("route_long_name:N", title="Full name"),
            alt.Tooltip("route_type_name:N", title="Mode"),
            alt.Tooltip("total_stops:Q", title="Distinct stops", format=","),
            alt.Tooltip("max_stops_per_trip:Q", title="Max stops per trip"),
            alt.Tooltip("total_trips:Q", title="Total trips", format=","),
        ],
    )
    .add_params(selection)
    .properties(height=480)
)
st.altair_chart(longest_bar, use_container_width=True)

st.caption(
    "**Source:** GTFS stop_times joined with trips and routes. "
    "**Calculation:** Distinct stops = COUNT(DISTINCT stop_id) across all trips on a route. "
    "Max stops per trip = the highest stop count for any single trip_id on the route. "
    "**Caveats:** 'Distinct stops' counts both directions, so a route with 40 stops outbound "
    "and 40 inbound that share 30 stops would show 50 distinct stops. "
    "Click a mode in the legend to filter."
)

st.divider()

# =====================================================================
# 6. Geographic distribution — stop map
# =====================================================================
st.subheader("Geographic distribution of transit stops across Hong Kong")

map_df = stop_locations.copy()
map_df["stop_lat"] = pd.to_numeric(map_df["stop_lat"], errors="coerce")
map_df["stop_lon"] = pd.to_numeric(map_df["stop_lon"], errors="coerce")
map_df = map_df.dropna(subset=["stop_lat", "stop_lon"])
map_df["total_departures"] = pd.to_numeric(map_df["total_departures"], errors="coerce").fillna(0)
map_df["radius"] = (map_df["total_departures"].clip(lower=1) ** 0.5) * 3 + 10
map_df["clean_name"] = map_df["stop_name"].apply(clean_stop_name)

# IBM Cyan 50 as RGB
map_df["r"] = 17
map_df["g"] = 146
map_df["b"] = 232
map_df["a"] = 160

st.markdown(
    f"**{len(map_df):,}** stops are plotted below. Dot size is proportional to scheduled departure count. "
    f"Stop density is highest along the northern shore of Hong Kong Island, "
    f"the Kowloon urban area, and the new town corridors of Sha Tin, Tuen Mun, and Yuen Long. "
    f"The southern and eastern parts of Hong Kong Island and the rural New Territories are comparatively sparse."
)

layer = pdk.Layer(
    "ScatterplotLayer",
    data=map_df,
    get_position=["stop_lon", "stop_lat"],
    get_radius="radius",
    get_fill_color=["r", "g", "b", "a"],
    pickable=True,
    auto_highlight=True,
)

view = pdk.ViewState(latitude=22.35, longitude=114.15, zoom=10.5, pitch=0)

deck = pdk.Deck(
    layers=[layer],
    initial_view_state=view,
    map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    tooltip={"text": "{clean_name}\nDepartures: {total_departures}\nRoutes: {distinct_routes}"},
)
st.pydeck_chart(deck)

st.caption(
    "**Source:** GTFS stops.txt coordinates joined with departure counts from stop_times. "
    "**Calculation:** Dot radius = sqrt(departure_count) * 3 + 10 pixels. "
    "Stops with zero departures (present in stops.txt but not in stop_times) are plotted at minimum size. "
    "**Caveats:** Coordinates are filtered to lat 22.1-22.6, lon 113.8-114.5 to exclude any erroneous points. "
    "Basemap: CARTO Dark Matter."
)

st.divider()

# =====================================================================
# 7. MTR interchange stations
# =====================================================================
st.subheader("MTR stations ranked by number of lines served")

heavy = int(mtr_stations[mtr_stations.station_type == "Heavy Rail"].station_name_en.nunique())
light = int(mtr_stations[mtr_stations.station_type == "Light Rail"].station_name_en.nunique())
interch_count = int(mtr_stations[mtr_stations.is_interchange].station_name_en.nunique())

top_mtr = mtr_stations.iloc[0]
st.markdown(
    f"**{top_mtr.station_name_en}** is served by **{int(top_mtr.line_count)}** lines "
    f"({top_mtr.lines_served}), making it the most connected MTR station. "
    f"Light Rail stops in the Tuen Mun-Yuen Long corridor dominate the top of this ranking "
    f"because 12 Light Rail routes share stops extensively. "
    f"Among heavy rail, Admiralty (4 lines) and Nam Cheong (2 lines) are the main interchanges."
)

col_a, col_b = st.columns([2, 1])

with col_a:
    interchanges = mtr_stations[mtr_stations.is_interchange].head(15).copy()
    interchanges["label"] = (
        interchanges["station_name_en"]
        + " ("
        + interchanges["station_name_tc"].fillna("")
        + ")"
    )

    mtr_bar = (
        alt.Chart(interchanges)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("line_count:Q", title="Number of lines served"),
            y=alt.Y("label:N", title=None, sort="-x"),
            color=alt.Color(
                "station_type:N",
                title="Station type",
                scale=alt.Scale(
                    domain=["Heavy Rail", "Light Rail"],
                    range=[IBM_RED_50, IBM_CYAN_50],
                ),
                legend=alt.Legend(orient="top-right"),
            ),
            tooltip=[
                alt.Tooltip("station_name_en:N", title="Station"),
                alt.Tooltip("station_type:N", title="Type"),
                alt.Tooltip("line_count:Q", title="Lines served"),
                alt.Tooltip("lines_served:N", title="Line codes"),
            ],
        )
        .properties(height=380)
    )
    st.altair_chart(mtr_bar, use_container_width=True)

with col_b:
    st.metric("Heavy Rail stations", heavy)
    st.metric("Light Rail stops", light)
    st.metric("Interchange stations", interch_count)

st.caption(
    "**Source:** MTR Open Data Portal (opendata.mtr.com.hk) — mtr_lines_and_stations.csv "
    "and light_rail_routes_and_stops.csv. "
    "**Calculation:** Line count = COUNT(DISTINCT line_code) per station_id after deduplication. "
    "A station is classified as an interchange if it serves 2+ lines. "
    "**Caveats:** MTR does not publish GTFS data, so trip frequency, headway, and passenger volume "
    "cannot be calculated for heavy rail or Light Rail. Only station-level reference data is available."
)

st.divider()

# =====================================================================
# 8. Service span
# =====================================================================
st.subheader("Top 20 routes by daily service span (first to last departure)")

first_last["span_hours"] = first_last["service_span_minutes"] / 60.0
first_last["label"] = first_last["route_short_name"]

top_span = first_last.iloc[0]
st.markdown(
    f"Route **{top_span.route_short_name}** operates the longest daily window at "
    f"**{top_span.span_hours:.1f} hours**, from {top_span.first_departure} to "
    f"{top_span.last_departure}. "
    f"Routes with GTFS departure times exceeding 24:00:00 indicate post-midnight service "
    f"continuing from the previous calendar day."
)

span_bar = (
    alt.Chart(first_last)
    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
    .encode(
        x=alt.X("span_hours:Q", title="Service span (hours)"),
        y=alt.Y("label:N", title=None, sort="-x"),
        color=alt.Color(
            "route_type_name:N",
            title="Transport mode",
            scale=alt.Scale(domain=MODE_DOMAIN, range=MODE_RANGE),
            legend=alt.Legend(orient="top-right"),
        ),
        tooltip=[
            alt.Tooltip("route_short_name:N", title="Route"),
            alt.Tooltip("route_type_name:N", title="Mode"),
            alt.Tooltip("first_departure:N", title="First departure"),
            alt.Tooltip("last_departure:N", title="Last departure"),
            alt.Tooltip("span_hours:Q", title="Span (hours)", format=".1f"),
            alt.Tooltip("total_departures:Q", title="Total departures", format=","),
        ],
    )
    .properties(height=480)
)
st.altair_chart(span_bar, use_container_width=True)

st.caption(
    "**Source:** GTFS stop_times, MIN and MAX departure_time per route. "
    "**Calculation:** Service span = (MAX departure_minutes - MIN departure_minutes) / 60. "
    "departure_minutes is parsed from the HH:MM:SS departure_time string. "
    "**Caveats:** GTFS allows departure times >24:00:00 for trips that start before midnight "
    "but end after midnight. These are preserved as-is; "
    "a last_departure of 25:30 means 01:30 the next calendar day. "
    "Span does not account for gaps in service (e.g. a route running 06:00-09:00 and 17:00-23:00 "
    "would show a 17-hour span despite no midday service)."
)

st.divider()

# =====================================================================
# Methodology
# =====================================================================
st.subheader("Data sources and methodology")
st.markdown("""
**Data sources:**
- **GTFS static feed** from Hong Kong Transport Department via
  [data.gov.hk](https://data.gov.hk/en-data/dataset/hk-td-tis_11-pt-headway-en)
  covering bus (KMB, CTB/NWFB), tram, and ferry schedules.
- **MTR Open Data** from [opendata.mtr.com.hk](https://opendata.mtr.com.hk)
  covering station reference, fare tables, and Light Rail route/stop data.

**Processing:**
- Raw GTFS files (stops.txt, routes.txt, trips.txt, stop_times.txt, calendar.txt)
  are ingested into BigQuery with full refresh (WRITE_TRUNCATE) daily.
- Staging SQL assets cast types, filter nulls on primary keys, deduplicate by extracted_at,
  and add derived fields (route type labels, weekday/weekend flags, departure hour).
- Mart SQL assets aggregate staging tables into dashboard-ready summaries.

**Metric definitions:**
- **Scheduled departures:** Count of stop_time records (one per vehicle visiting one stop).
- **Distinct routes:** COUNT(DISTINCT route_id) per grouping dimension.
- **Service span:** Difference in minutes between earliest and latest departure_time for a route.
- **Transfer hub rank:** Stops ordered by number of distinct routes served.

**Limitations:**
- MTR heavy rail does not publish GTFS data. No trip-level analysis is possible for the MTR network.
- GTFS represents scheduled service only. Actual operations may differ due to delays, cancellations, or extras.
- Stop names concatenate multiple operator names; the dashboard extracts the primary name for readability.
- Calendar-based weekday/weekend classification does not capture public holiday schedules.
- Color palette follows [IBM Carbon Design System](https://carbondesignsystem.com/data-visualization/color-palettes/) for color-blind accessibility.
""")
