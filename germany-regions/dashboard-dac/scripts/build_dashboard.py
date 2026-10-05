"""Generate dashboards/germany-regions.yml for Bruin DAC.

Every indicator gets the same four widgets (header text, choropleth map, ranked bars,
footnote), so the YAML is generated from the INDICATORS config below instead of being
maintained by hand. Edit this file, then run:

    python3 germany-regions/dashboard-dac/scripts/build_dashboard.py
    dac validate --dir germany-regions/dashboard-dac

Insight sentences quote values from report.de_state_indicator_snapshot (reference year
2025, disposable income 2024); re-check them against the data after a refresh.

State polygons are read from raw.de_state_boundaries (GISCO NUTS 2024) with `bruin query`
and embedded once as a named Vega-Lite dataset: DAC delivers query results as flat rows,
and Vega-Lite cannot parse a GeoJSON string column, so geometry must be part of the spec.
"""

import json
import subprocess
from pathlib import Path

import yaml

OUT = Path(__file__).resolve().parents[1] / "dashboards" / "germany-regions.yml"
CONNECTION = "bruin-playground-arsalan"
SNAPSHOT = "`bruin-playground-arsalan.report.de_state_indicator_snapshot`"
COORD_DECIMALS = 3  # ~100 m; the source is already generalised to 1:10 million
BACKGROUND = "#0b0e14"  # bruin-dark surface, used for polygon outlines and label halos
TEXT = "#e6e8ec"
MAP_ROW_HEIGHT = 600

TOOLS = "**Tools:** **Bruin cli** (pipeline), **BigQuery** (warehouse), **Bruin dac** (visualization)."
GISCO = (
    "state boundaries **[Eurostat GISCO, NUTS 2024, 1:10 million]"
    "(https://ec.europa.eu/eurostat/web/gisco/geodata/statistical-units/territorial-units-statistics)** "
    "(© EuroGeographics)"
)


def genesis(code: str) -> str:
    return f"**[Destatis GENESIS-Online, table {code}](https://www-genesis.destatis.de/datenbank/online/table/{code})**"


def eurostat(code: str) -> str:
    return f"**[Eurostat, dataset {code}](https://ec.europa.eu/eurostat/databrowser/view/{code}/default/table)**"


VGRDL_GDP = (
    "**[Arbeitskreis VGR der Länder, GDP release August 2025/February 2026]"
    "(https://www.statistikportal.de/de/vgrdl/ergebnisse-laenderebene/bruttoinlandsprodukt-bruttowertschoepfung)**"
)
VGRDL_INCOME = (
    "**[Arbeitskreis VGR der Länder, income release 2025]"
    "(https://www.statistikportal.de/de/vgrdl/ergebnisse-laenderebene/einkommen)**"
)
DL_DE = "licence dl-de/by-2-0"
CC_BY = "licence CC BY 4.0"

# One entry per mapped indicator, in dashboard order.
INDICATORS = [
    {
        "id": "gdp_per_capita", "tab": "Economy",
        "title": "GDP per inhabitant by state, 2025 (EUR, current prices)",
        "short": "GDP per inhabitant, 2025", "unit": "EUR", "format": ",.0f",
        "insight": (
            "Hamburg (EUR 90,208) is 2.3 times Sachsen-Anhalt (EUR 38,452); all five eastern states rank below every western state."
        ),
        "sources": f"{VGRDL_GDP}, {DL_DE}",
        "limitations": (
            "GDP is measured where it is produced (domestic concept) but divided by residents, so city-states "
            "with many in-commuters (Hamburg, Bremen, Berlin) are overstated relative to their residents' "
            "incomes. Current prices, not adjusted for regional price levels. 2025 values are the first VGRdL "
            "estimate and may be revised."
        ),
    },
    {
        "id": "gross_hourly_earnings", "tab": "Economy",
        "title": "Average gross hourly earnings by state, April 2025 (EUR per hour)",
        "short": "Gross hourly earnings, April 2025", "unit": "EUR per hour", "format": ",.2f",
        "insight": (
            "Hamburg pays EUR 28.02 per hour, 31% more than Mecklenburg-Vorpommern (EUR 21.31); the five eastern states are the bottom five."
        ),
        "sources": f"{genesis('62361-0051')} and {genesis('62361-0046')} (Germany), {DL_DE}",
        "limitations": (
            "Averages are pulled up by high earners and reflect each state's industry and job mix. Earnings "
            "are by place of work. The survey covers one reference month (April)."
        ),
    },
    {
        "id": "gender_pay_gap", "tab": "Economy",
        "title": "Unadjusted gender pay gap by state, April 2025 (% of men's gross hourly earnings)",
        "short": "Gender pay gap, April 2025", "unit": "%", "format": ".0f",
        "insight": (
            "The gap is 4-6% in the five eastern states and 12-20% in the western states (Germany: 16%)."
        ),
        "sources": f"{genesis('62361-0051')} and {genesis('62361-0046')} (Germany), {DL_DE}",
        "limitations": (
            "Unadjusted: compares all women's and all men's average hourly earnings without controlling for "
            "occupation, industry, hours, or experience. Destatis publishes the state values rounded to whole "
            "percentage points, so several states tie."
        ),
    },
    {
        "id": "unemployment_rate", "tab": "Economy",
        "title": "Registered unemployment rate by state, 2025 annual average (% of civilian labour force)",
        "short": "Unemployment rate, 2025", "unit": "%", "format": ".1f",
        "insight": (
            "Rates range from 4.0% in Bayern to 11.5% in Bremen; the three city-states have the highest rates (Germany: 6.3%)."
        ),
        "sources": f"Bundesagentur für Arbeit via {genesis('13211-0007')} and {genesis('13211-0001')} (Germany), {DL_DE}",
        "limitations": (
            "National definition (registered with the Bundesagentur für Arbeit), not the ILO definition, so "
            "levels are higher than Eurostat unemployment rates. Annual average of monthly counts."
        ),
    },
    {
        "id": "employment_rate_20_64", "tab": "Economy",
        "title": "Employment rate of residents aged 20-64 by state, 2025 (%)",
        "short": "Employment rate 20-64, 2025", "unit": "%", "format": ".1f",
        "insight": (
            "Bayern (84.3%) is highest and Bremen (74.7%) lowest; across states, employment and unemployment rates correlate at r = -0.94."
        ),
        "sources": f"{eurostat('lfst_r_lfe2emprt')} (EU Labour Force Survey), {CC_BY}",
        "limitations": (
            "Survey-based (ILO definition, place of residence), so small states such as Bremen and Saarland have "
            "larger sampling error. Not directly comparable with the registered unemployment rate. The dot axis "
            "starts at 70% because all values lie between 74.7% and 84.3%."
        ),
    },
    {
        "id": "building_land_price", "tab": "Economy",
        "title": "Average price of building-ready land sold, by state, 2025 (EUR per m²)",
        "short": "Building land price, 2025", "unit": "EUR per m²", "format": ",.0f",
        "insight": (
            "Building land costs EUR 1,083 per m² in Hamburg and EUR 73 in Sachsen-Anhalt, a 15-fold range."
        ),
        "sources": f"{genesis('61511-0050')} and {genesis('61511-0010')} (Germany), {DL_DE}",
        "limitations": (
            "Average of the plots actually sold in 2025, so the value depends on which locations and plot types "
            "changed hands; city-states record few transactions. Not a price index."
        ),
    },
    {
        "id": "population_density", "tab": "Society",
        "title": "Population density by state, 31 Dec 2025 (residents per km²)",
        "short": "Population density, 2025", "unit": "per km²", "format": ",.0f",
        "log": True,
        "insight": (
            "Berlin (4,153 per km²) is 61 times as densely populated as Mecklenburg-Vorpommern (68)."
        ),
        "sources": f"{genesis('12411-0010')} (population) and {genesis('11111-0001')} (area), {DL_DE}",
        "limitations": (
            "Colour and dot position use a log scale (Log Scale) because the three city-states are 3 to 61 "
            "times denser than the other states. Area is the latest official figure (31 Dec 2023); state areas "
            "change by fractions of a percent between years. Population update based on the 2022 census."
        ),
    },
    {
        "id": "share_65_plus", "tab": "Society",
        "title": "Share of residents aged 65 and over by state, 31 Dec 2025 (% of population)",
        "short": "Share aged 65+, 2025", "unit": "%", "format": ".1f",
        "insight": (
            "The five eastern states are the oldest (27.1-28.9% aged 65+); Hamburg is the youngest (18.2%)."
        ),
        "sources": f"{genesis('12411-0014')}, {DL_DE}",
        "limitations": (
            "Population update based on the 2022 census; not comparable with pre-census series. "
            "The correlation describes states, not individuals."
        ),
    },
    {
        "id": "share_foreign_nationals", "tab": "Society",
        "title": "Share of residents without German citizenship by state, 31 Dec 2025 (% of population)",
        "short": "Share foreign nationals, 2025", "unit": "%", "format": ".1f",
        "insight": (
            "Bremen has the highest share (23.3%); all five eastern states are below 8.5%, under every western state."
        ),
        "sources": f"{genesis('12411-0014')}, {DL_DE}",
        "limitations": (
            "Citizenship, not migration background: naturalised residents and dual nationals with German "
            "citizenship count as German. Population update figures differ from the Central Register of "
            "Foreigners (AZR)."
        ),
    },
    {
        "id": "total_fertility_rate", "tab": "Society",
        "title": "Total fertility rate by state, 2025 (children per woman)",
        "short": "Total fertility rate, 2025", "unit": "children per woman", "format": ".2f",
        "zero": False,
        "insight": (
            "Fertility ranges from 1.16 (Sachsen) to 1.38 (Niedersachsen), all well below the replacement level of about 2.1."
        ),
        "sources": f"{genesis('12612-0104')} and {genesis('12612-0009')} (Germany), {DL_DE}",
        "limitations": (
            "Period measure for one calendar year; it changes with the timing of births and is not a forecast "
            "of completed family size. The dot axis starts at 1.0 because all values lie between 1.16 and 1.38; the "
            "Germany line marks the national value."
        ),
    },
    {
        "id": "net_migration_per_1000", "tab": "Society",
        "title": "Net migration by state, 2025 (moves in minus moves out per 1,000 residents)",
        "short": "Net migration, 2025", "unit": "per 1,000 residents", "format": ".1f",
        "insight": (
            "Brandenburg gained most (+6.5 per 1,000); Thüringen is the only state with a net loss (-0.3)."
        ),
        "sources": f"{genesis('12711-0020')} and {genesis('12411-0010')} (population), {DL_DE}",
        "limitations": (
            "Includes moves between states and to/from abroad; the Germany value contains only international "
            "migration. Per 1,000 average residents (mean of the 31 Dec 2024 and 2025 populations). The colour "
            "scale runs from the lowest to the highest state value, so white marks Thüringen (-0.3), not zero."
        ),
    },
    {
        "id": "poverty_risk_rate", "tab": "Society",
        "title": "At-risk-of-poverty rate by state, 2025 (% of population)",
        "short": "At-risk-of-poverty rate, 2025", "unit": "%", "format": ".1f",
        "insight": (
            "Bremen (27.5%) is highest and Bayern (12.6%) lowest; the eastern and western averages are almost equal (17.4% vs 17.3%)."
        ),
        "sources": f"{eurostat('ilc_li41')} (EU-SILC), {CC_BY}",
        "limitations": (
            "Poverty line is 60% of the national median equivalised income, not adjusted for regional price "
            "levels, so it understates hardship in expensive states and overstates it in cheaper ones. "
            "Survey-based; EU-SILC 2025 refers to 2024 incomes."
        ),
    },
    {
        "id": "tertiary_education_share", "tab": "Society",
        "title": "Share of residents aged 25-64 with tertiary education by state, 2025 (%)",
        "short": "Tertiary education 25-64, 2025", "unit": "%", "format": ".1f",
        "insight": (
            "Berlin (48.1%) and Hamburg (43.0%) have the highest shares; Sachsen-Anhalt the lowest (25.4%)."
        ),
        "sources": f"{eurostat('edat_lfse_04')} (EU Labour Force Survey), {CC_BY}",
        "limitations": (
            "ISCED 2011 levels 5-8, which in Germany include master craftsman (Meister) and technician "
            "qualifications. Survey-based; place of residence."
        ),
    },
    {
        "id": "life_expectancy_women", "tab": "Society",
        "title": "Life expectancy at birth of women by state, 2023-2025 (years)",
        "short": "Life expectancy, women, 2023-2025", "unit": "years", "format": ".1f",
        "zero": False,
        "insight": (
            "Women's life expectancy spans 2.4 years, from 82.0 (Saarland) to 84.4 (Baden-Württemberg)."
        ),
        "sources": f"{genesis('12621-0004')} and {genesis('12621-0002')} (Germany), {DL_DE}",
        "limitations": (
            "Period life table 2023/25, a three-year average ending in 2025 (the latest state life table). "
            "The dot axis starts at 81 years so that differences are visible; the Germany line marks the national value."
        ),
    },
    {
        "id": "life_expectancy_men", "tab": "Society",
        "title": "Life expectancy at birth of men by state, 2023-2025 (years)",
        "short": "Life expectancy, men, 2023-2025", "unit": "years", "format": ".1f",
        "zero": False,
        "insight": (
            "Men's life expectancy spans 3.9 years, from 76.4 (Sachsen-Anhalt) to 80.3 (Baden-Württemberg); all eastern states are below Germany (78.8)."
        ),
        "sources": f"{genesis('12621-0004')} and {genesis('12621-0002')} (Germany), {DL_DE}",
        "limitations": (
            "Period life table 2023/25, a three-year average ending in 2025 (the latest state life table). "
            "The dot axis starts at 75 years so that differences are visible; the Germany line marks the national value."
        ),
    },
    {
        "id": "disposable_income_per_capita", "tab": "Income (2024)",
        "title": "Disposable income of private households per inhabitant by state, 2024 (EUR)",
        "short": "Disposable income per inhabitant, 2024", "unit": "EUR", "format": ",.0f",
        "insight": (
            "Disposable income varies 1.26-fold (EUR 26,297 to 33,204), far less than GDP per inhabitant (2.38-fold in 2024)."
        ),
        "sources": f"{VGRDL_INCOME}, {DL_DE}",
        "limitations": (
            "Reference year 2024, not 2025: the latest official state-level release (June 2026) ends in 2024, "
            "so this map is kept apart from the 2025 maps. Income after taxes and social contributions, "
            "including transfers; by place of residence; current prices, not adjusted for regional price levels."
        ),
    },
]

ZERO_DOMAIN_MIN = {"employment_rate_20_64": 70, "total_fertility_rate": 1.0, "life_expectancy_women": 81, "life_expectancy_men": 75}
# Single-hue sequential blue (ColorBrewer Blues, 9 steps): near-white = lowest, dark blue = highest.
COLOR_RANGE = ["#f7fbff", "#deebf7", "#c6dbef", "#9ecae1", "#6baed6", "#4292c6", "#2171b5", "#08519c", "#08306b"]
LABEL_DARK, LABEL_LIGHT = "#111111", "#ffffff"


# ---------------------------------------------------------------------------
# Vega-Lite specs
# ---------------------------------------------------------------------------

def color_scale(ind: dict) -> dict:
    scale = {"range": COLOR_RANGE}
    if ind.get("log"):
        scale["type"] = "log"
    return scale


def tooltip(unit: str) -> list:
    return [
        {"field": "state_name", "title": "State"},
        {"field": "value_label", "title": f"Value ({unit})"},
        {"field": "reference_period", "title": "Reference period"},
        {"field": "rank_desc", "title": "Rank of 16 (1 = highest)"},
        {"field": "germany_value_label", "title": f"Germany ({unit})"},
        {"field": "diff_to_germany_label", "title": "Difference to Germany"},
    ]


def load_state_features() -> list:
    """GeoJSON features of the 16 states from BigQuery, coordinates rounded to COORD_DECIMALS."""
    out = subprocess.run(
        ["bruin", "query", "--connection", CONNECTION, "--output", "json", "--query",
         "SELECT nuts1_id, geometry_geojson FROM `bruin-playground-arsalan.raw.de_state_boundaries` ORDER BY nuts1_id"],
        check=True, capture_output=True, text=True,
    ).stdout
    rows = json.loads(out[out.index("{"):])["rows"]

    def rnd(c):
        return [rnd(x) for x in c] if isinstance(c[0], list) else [round(c[0], COORD_DECIMALS), round(c[1], COORD_DECIMALS)]

    features = []
    for nuts1_id, geometry in rows:
        geom = json.loads(geometry)
        geom["coordinates"] = rnd(geom["coordinates"])
        features.append({"type": "Feature", "properties": {"nuts1_id": nuts1_id}, "geometry": geom})
    if len(features) != 16:
        raise RuntimeError(f"Expected 16 state geometries, got {len(features)}")
    return features


STATE_FEATURES: list = []  # filled in __main__; the same list object is reused so YAML emits it once


def map_spec(ind: dict) -> dict:
    fill = {
        "field": "value", "type": "quantitative", "scale": color_scale(ind),
        "legend": {"title": ind["unit"], "format": ind["format"], "orient": "right", "gradientLength": 260, "titlePadding": 10},
    }
    label_encoding = {
        "longitude": {"field": "label_lon", "type": "quantitative"},
        "latitude": {"field": "label_lat", "type": "quantitative"},
        "text": {"field": "map_label"},
        "tooltip": tooltip(ind["unit"]),
    }
    label_mark = {"type": "text", "fontSize": 10.5, "fontWeight": 600, "lineBreak": "\n", "baseline": "middle"}
    label_transform = [
        {"joinaggregate": [{"op": "min", "field": "value", "as": "vmin"}, {"op": "max", "field": "value", "as": "vmax"}]},
        {"calculate": (
            "(log(datum.value) - log(datum.vmin)) / (log(datum.vmax) - log(datum.vmin))" if ind.get("log")
            else "(datum.value - datum.vmin) / (datum.vmax - datum.vmin)"
        ), "as": "color_pos"},
    ]
    return {
        "projection": {"type": "mercator"},
        "datasets": {"states": STATE_FEATURES},
        "layer": [
            {
                "transform": [{"lookup": "nuts1_id", "from": {"data": {"name": "states"}, "key": "properties.nuts1_id"}, "as": "geo"}],
                "mark": {"type": "geoshape", "stroke": BACKGROUND, "strokeWidth": 0.8, "strokeJoin": "round"},
                "encoding": {"shape": {"field": "geo", "type": "geojson"}, "fill": fill, "tooltip": tooltip(ind["unit"])},
            },
            {
                # Dark text with a white halo on light fills, white text with a dark halo on dark fills,
                # chosen by the value's position on the colour scale (labels of small states spill over borders).
                "transform": label_transform,
                "mark": {**label_mark, "strokeWidth": 3, "strokeJoin": "round"},
                "encoding": {
                    **label_encoding,
                    "color": {"condition": {"test": "datum.color_pos > 0.5", "value": BACKGROUND}, "value": LABEL_LIGHT},
                    "stroke": {"condition": {"test": "datum.color_pos > 0.5", "value": BACKGROUND}, "value": LABEL_LIGHT},
                },
            },
            {
                "transform": label_transform,
                "mark": label_mark,
                "encoding": {
                    **label_encoding,
                    "color": {"condition": {"test": "datum.color_pos > 0.5", "value": LABEL_LIGHT}, "value": LABEL_DARK},
                },
            },
        ],
    }


def ranking_spec(ind: dict) -> dict:
    """Ranked bars from zero; a dot plot when the axis cannot start at zero (log scale or narrow range)."""
    dot = ind.get("log") or ind["id"] in ZERO_DOMAIN_MIN
    if ind.get("log"):
        x_scale = {"type": "log"}
    elif dot:
        x_scale = {"zero": False, "domainMin": ZERO_DOMAIN_MIN[ind["id"]], "nice": True}
    else:
        x_scale = {"zero": True}
    y = {
        "field": "state_label", "type": "nominal", "title": None,
        "sort": {"field": "value", "order": "descending"},
        "axis": {"labelLimit": 220, "labelFontSize": 11},
    }
    x_title = ind["unit"] + (" (log scale)" if ind.get("log") else "")
    x = {
        "field": "value", "type": "quantitative", "title": x_title, "scale": x_scale,
        "axis": {"format": ind["format"], "tickCount": 5},
    }
    fill = {"field": "value", "type": "quantitative", "scale": color_scale(ind), "legend": None}
    if dot:
        value_mark = {"type": "point", "filled": True, "size": 130, "opacity": 1, "stroke": TEXT, "strokeWidth": 0.6}
        value_encoding = {"y": y, "x": x, "fill": fill, "tooltip": tooltip(ind["unit"])}
        label_dx = 9
    else:
        value_mark = {"type": "bar", "cornerRadiusEnd": 2}
        value_encoding = {"y": y, "x": x, "fill": fill, "tooltip": tooltip(ind["unit"])}
        label_dx = 4
    label_encoding = {"y": y, "x": {"field": "value", "type": "quantitative"}, "text": {"field": "value_label"}}
    label_mark = {"type": "text", "align": "left", "dx": label_dx, "fontSize": 10.5}
    return {
        "layer": [
            {"mark": value_mark, "encoding": value_encoding},
            {
                "mark": {"type": "rule", "strokeDash": [5, 4], "strokeWidth": 1.5, "color": TEXT, "opacity": 0.8},
                "encoding": {"x": {"aggregate": "max", "field": "germany_value", "type": "quantitative"}},
            },
            {
                "mark": {**label_mark, "stroke": BACKGROUND, "strokeWidth": 3, "strokeJoin": "round", "fill": BACKGROUND},
                "encoding": label_encoding,
            },
            {"mark": {**label_mark, "fill": TEXT}, "encoding": label_encoding},
            {
                "transform": [
                    {"filter": "datum.rank_desc === 1"},
                    {"calculate": "'Germany: ' + datum.germany_value_label", "as": "germany_text"},
                ],
                "mark": {"type": "text", "align": "left", "dx": 4, "dy": -4, "baseline": "bottom", "fontSize": 10.5,
                         "fontStyle": "italic", "fill": TEXT},
                "encoding": {
                    "x": {"field": "germany_value", "type": "quantitative"},
                    "y": {"value": 0},
                    "text": {"field": "germany_text"},
                },
            },
        ],
    }


# ---------------------------------------------------------------------------
# Widgets
# ---------------------------------------------------------------------------

def encoding_key(ind: dict) -> str:
    log = ", log scale" if ind.get("log") else ""
    if ind.get("log") or ind["id"] in ZERO_DOMAIN_MIN:
        chart = "**Dots:** sorted highest to lowest, axis not from zero"
    else:
        chart = "**Bars:** sorted highest to lowest"
    return f"**Map:** white = lowest, dark blue = highest{log}. {chart}; dashed line = Germany."


def indicator_rows(ind: dict) -> list:
    header = f"# {ind['title']}\n\n**{ind['insight']}**\n\n{encoding_key(ind)}\n"
    footnote = (
        f"**Sources:** {ind['sources']}; {GISCO}.\n\n{TOOLS}\n\n**Limitations:** {ind['limitations']}\n"
    )
    map_sql = (
        "SELECT nuts1_id, state_name, map_label, label_lon, label_lat, value, value_label, reference_period,\n"
        "  rank_desc, germany_value_label, diff_to_germany_label\n"
        f"FROM {SNAPSHOT}\n"
        f"WHERE indicator_id = '{ind['id']}'\n"
    )
    ranking_sql = (
        "SELECT CONCAT(state_name, ' (', state_abbr, ')') AS state_label, state_name, value, value_label,\n"
        "  reference_period, rank_desc, germany_value, germany_value_label, diff_to_germany_label\n"
        f"FROM {SNAPSHOT}\n"
        f"WHERE indicator_id = '{ind['id']}'\n"
        "ORDER BY value DESC\n"
    )
    tab = ind["tab"]
    return [
        {"tab": tab, "widgets": [{"name": f"{ind['id']}_header", "type": "text", "col": 12, "content": header}]},
        {
            "tab": tab,
            "height": MAP_ROW_HEIGHT,
            "widgets": [
                {"name": f"Map - {ind['short']} ({ind['unit']})", "type": "chart", "chart": "vega-lite",
                 "col": 6, "sql": map_sql, "spec": map_spec(ind)},
                {"name": f"States ranked - {ind['short']} ({ind['unit']})", "type": "chart", "chart": "vega-lite",
                 "col": 6, "sql": ranking_sql, "spec": ranking_spec(ind)},
            ],
        },
        {"tab": tab, "widgets": [{"name": f"{ind['id']}_footnote", "type": "text", "col": 12, "content": footnote}]},
        {"tab": tab, "widgets": [{"name": f"{ind['id']}_divider", "type": "divider", "col": 12}]},
    ]


def overview_rows() -> list:
    tab = "Overview"
    intro = (
        "# Germany's 16 states compared: economy and society, 2025\n\n"
        "**All maps compare the 16 states (Bundesländer) in the same reference year: 2025, the latest year "
        "with official values for every state across all indicators. The one exception, disposable income, "
        "is published only up to 2024 and has its own tab. Fifteen indicators come from Destatis, the "
        "regional accounts of the states (VGRdL), and Eurostat.**\n\n"
        "The **Economy** and **Society** tabs show one map per indicator next to a ranked bar chart of the "
        "same values. **Data quality & methodology** lists the reference period, completeness, and source of "
        "every indicator. State abbreviations: BW Baden-Württemberg, BY Bayern, BE Berlin, BB Brandenburg, "
        "HB Bremen, HH Hamburg, HE Hessen, MV Mecklenburg-Vorpommern, NI Niedersachsen, NW Nordrhein-Westfalen, "
        "RP Rheinland-Pfalz, SL Saarland, SN Sachsen, ST Sachsen-Anhalt, SH Schleswig-Holstein, TH Thüringen.\n"
    )
    kpi_sql = (
        "SELECT value{scale} AS value FROM `bruin-playground-arsalan.staging.de_state_indicators`\n"
        "WHERE region_code = 'DG' AND indicator_id = '{id}' AND year = 2025\n"
    )
    kpis = [
        ("Population, 31 Dec 2025 (million)", "population", " / 1e6", ",.2f"),
        ("GDP per inhabitant, 2025 (EUR)", "gdp_per_capita", "", ",.0f"),
        ("Registered unemployment rate, 2025 (%)", "unemployment_rate", "", ".1f"),
        ("Total fertility rate, 2025 (children per woman)", "total_fertility_rate", "", ".2f"),
    ]
    east_west_header = (
        "# East-West comparison of state averages, by indicator, 2025\n\n"
        "**Eastern states average EUR 40,000 GDP per inhabitant against EUR 56,600 in the west, but unemployment, "
        "employment and poverty rates differ by less than one percentage point.**\n\n"
        "**Columns:** unweighted means of state values; East vs West = gap as % of the western mean.\n"
    )
    east_west_sql = (
        "WITH g AS (\n"
        "  SELECT s.indicator_id, s.short_label, s.unit_label, s.reference_period, c.display_order, c.label_decimals,\n"
        "    AVG(IF(s.region_group = 'West', s.value, NULL)) AS west_mean,\n"
        "    AVG(IF(s.region_group = 'East', s.value, NULL)) AS east_mean,\n"
        "    MAX(IF(s.region_group = 'Berlin', s.value, NULL)) AS berlin,\n"
        "    MAX(s.germany_value) AS germany\n"
        f"  FROM {SNAPSHOT} AS s\n"
        "  JOIN `bruin-playground-arsalan.staging.de_indicator_catalog` AS c USING (indicator_id)\n"
        "  GROUP BY 1, 2, 3, 4, 5, 6\n"
        ")\n"
        "SELECT short_label AS indicator, unit_label AS unit, reference_period,\n"
        "  FORMAT('%\\'.*f', dec, west_mean) AS west_mean,\n"
        "  FORMAT('%\\'.*f', dec, east_mean) AS east_mean,\n"
        "  FORMAT('%\\'.*f', dec, berlin) AS berlin,\n"
        "  FORMAT('%\\'.*f', dec, germany) AS germany,\n"
        "  ROUND(100 * (east_mean - west_mean) / west_mean, 1) AS east_vs_west_pct\n"
        "FROM (SELECT *, IF(unit_label = '%', GREATEST(label_decimals, 1), label_decimals) AS dec FROM g)\n"
        "ORDER BY display_order\n"
    )
    east_west_footnote = (
        f"**Sources:** Destatis GENESIS-Online, Arbeitskreis VGR der Länder, and Eurostat (see each map's "
        f"footnote for the table), {DL_DE} / {CC_BY}.\n\n{TOOLS}\n\n"
        "**Limitations:** Unweighted means give Bremen the same weight as Nordrhein-Westfalen; population-"
        "weighted averages would move the western means toward the large states. West = ten western states "
        "without Berlin; East = Brandenburg, Mecklenburg-Vorpommern, Sachsen, Sachsen-Anhalt, Thüringen. "
        "Disposable income refers to 2024.\n"
    )
    corr_header = (
        "# Correlation between indicators across the 16 states, 2025 (Pearson r)\n\n"
        "**The share aged 65+ moves against hourly earnings (r = -0.95); GDP per inhabitant tracks earnings "
        "(r = 0.91) and land prices (r = 0.88).**\n\n"
        "**Cells:** orange = positive, blue = negative, white = none; printed value = r.\n"
    )
    corr_sql = (
        "SELECT label_x, label_y, order_x, order_y, pearson_r, r_label, n_states\n"
        "FROM `bruin-playground-arsalan.report.de_indicator_correlations`\n"
        "ORDER BY order_x, order_y\n"
    )
    corr_spec = {
        "layer": [
            {
                "mark": {"type": "rect", "stroke": BACKGROUND, "strokeWidth": 1},
                "encoding": {
                    "x": {"field": "label_x", "type": "nominal", "sort": {"field": "order_x"}, "title": None,
                          "axis": {"labelAngle": -40, "labelLimit": 170, "orient": "bottom"}},
                    "y": {"field": "label_y", "type": "nominal", "sort": {"field": "order_y"}, "title": None,
                          "axis": {"labelLimit": 170}},
                    "fill": {"field": "pearson_r", "type": "quantitative",
                             "scale": {"scheme": "blueorange", "domain": [-1, 1]},
                             "legend": {"title": "Pearson r", "format": ".1f", "gradientLength": 260}},
                    "tooltip": [
                        {"field": "label_y", "title": "Indicator"},
                        {"field": "label_x", "title": "Compared with"},
                        {"field": "r_label", "title": "Pearson r"},
                        {"field": "n_states", "title": "States"},
                    ],
                },
            },
            {
                "mark": {"type": "text", "fontSize": 9.5},
                "encoding": {
                    "x": {"field": "label_x", "type": "nominal", "sort": {"field": "order_x"}},
                    "y": {"field": "label_y", "type": "nominal", "sort": {"field": "order_y"}},
                    "text": {"field": "r_label"},
                    "color": {"condition": {"test": "abs(datum.pearson_r) > 0.6", "value": "#ffffff"},
                              "value": "#111111"},
                },
            },
        ],
    }
    corr_footnote = (
        "**Sources:** the 2025 indicators shown in the Economy and Society tabs (Destatis GENESIS-Online, "
        f"Arbeitskreis VGR der Länder, Eurostat), {DL_DE} / {CC_BY}.\n\n{TOOLS}\n\n"
        "**Limitations:** n = 16 states, so |r| below about 0.5 is not distinguishable from zero at the 5% level. "
        "Correlations describe states, not individuals, and do not imply causation; much of the pattern "
        "reflects the east-west divide and the three city-states. Disposable income (2024) is excluded so that "
        "every pair uses the same reference year.\n"
    )
    rows = [
        {"tab": tab, "widgets": [{"name": "overview_intro", "type": "text", "col": 12, "content": intro}]},
        {"tab": tab, "widgets": [
            {"name": name, "type": "metric", "col": 3, "sql": kpi_sql.format(id=ind, scale=scale),
             "value": {"field": "value", "type": "number", "format": fmt}}
            for name, ind, scale, fmt in kpis
        ]},
        {"tab": tab, "widgets": [{"name": "east_west_header", "type": "text", "col": 12, "content": east_west_header}]},
        {"tab": tab, "widgets": [{
            "name": "East-West comparison table", "type": "table", "col": 12, "sql": east_west_sql,
            "columns": [
                {"name": "indicator", "label": "Indicator"},
                {"name": "unit", "label": "Unit"},
                {"name": "reference_period", "label": "Reference period"},
                {"name": "west_mean", "label": "West, mean of 10 states", "align": "right"},
                {"name": "east_mean", "label": "East, mean of 5 states", "align": "right"},
                {"name": "berlin", "label": "Berlin", "align": "right"},
                {"name": "germany", "label": "Germany", "align": "right"},
                {"name": "east_vs_west_pct", "label": "East vs West (%)", "number": "+.1f",
                 "format": [{"backgroundColor": ["#0072B2", "#f7f7f7", "#E69F00"], "range": [-75, 0, 75],
                             "unit": "absolute"}]},
            ],
        }]},
        {"tab": tab, "widgets": [{"name": "east_west_footnote", "type": "text", "col": 12, "content": east_west_footnote}]},
        {"tab": tab, "widgets": [{"name": "overview_divider", "type": "divider", "col": 12}]},
        {"tab": tab, "widgets": [{"name": "correlation_header", "type": "text", "col": 12, "content": corr_header}]},
        {"tab": tab, "height": 700, "widgets": [{
            "name": "Correlation matrix - 15 indicators, 16 states, 2025 (Pearson r)", "type": "chart",
            "chart": "vega-lite", "col": 12, "sql": corr_sql, "spec": corr_spec,
        }]},
        {"tab": tab, "widgets": [{"name": "correlation_footnote", "type": "text", "col": 12, "content": corr_footnote}]},
    ]
    return rows


def quality_rows() -> list:
    tab = "Data quality & methodology"
    header = (
        "# Freshness and completeness of every indicator, as of September 2026\n\n"
        "**Fifteen indicators are complete for all 16 states in 2025; disposable income is complete for 2024. "
        "The low-wage job share was dropped (2025 value suppressed for Mecklenburg-Vorpommern).**\n\n"
        "**Columns:** reference period, latest year ingested, states with a value (highlighted below 16), "
        "Germany value, mapped or not.\n"
    )
    sql = (
        "SELECT indicator_label, reference_period_label, latest_year_available,\n"
        "  states_in_reference_year, IF(has_germany_value, 'Yes', 'No') AS germany_value,\n"
        "  IF(in_dashboard, 'Mapped', 'Not mapped') AS dashboard_status,\n"
        "  source_name, source_table, license\n"
        "FROM `bruin-playground-arsalan.report.de_indicator_quality`\n"
        "ORDER BY display_order\n"
    )
    footnote = (
        f"**Sources:** {genesis('12411-0010')} and the other GENESIS tables listed, Arbeitskreis VGR der Länder, "
        f"Eurostat; {DL_DE} / {CC_BY}.\n\n{TOOLS}\n\n"
        "**Limitations:** Latest year available reflects what the pipeline ingested on its last run "
        "(September 2026). 2025 values from the regional accounts and the life table are first releases and "
        "may be revised.\n"
    )
    methodology = (
        "# Methodology\n\n"
        "**Reference year rule.** Every map compares states within a single reference year. The pipeline "
        "ingests the last four years of each source and the dashboard shows the fixed reference year defined "
        "in `staging.de_indicator_catalog`: 2025 for fifteen indicators, 2024 for disposable income (latest "
        "release). No 2026 annual values exist yet for any state-level indicator. A data check fails the "
        "pipeline if any mapped indicator has fewer than 16 states or more than one year.\n\n"
        "**Sources.** Destatis GENESIS-Online REST API (population, area, age and citizenship, births, "
        "migration, registered unemployment, land prices, earnings, life tables); Arbeitskreis "
        "Volkswirtschaftliche Gesamtrechnungen der Länder (GDP and disposable income per inhabitant, Excel "
        "releases); Eurostat dissemination API (employment rate, tertiary education, at-risk-of-poverty "
        "rate, NUTS 1). State boundaries: Eurostat GISCO NUTS 2024 at 1:10 million.\n\n"
        "**Derived values.** Population density = population on 31 Dec 2025 / area on 31 Dec 2023. Shares "
        "aged 65+ and without German citizenship are computed from population by single year of age and "
        "citizenship (table 12411-0014). Net migration per 1,000 = net moves across state borders (including "
        "abroad) / mean population of 31 Dec 2024 and 2025. All other values are used as published.\n\n"
        "**Joins.** Destatis tables use the official state code (AGS 01-16); Eurostat uses NUTS 1 codes "
        "(DE1-DEG); VGRdL uses state names. `staging.de_states` maps all three. The Germany value is the "
        "published national figure where one exists, otherwise the sum of the 16 states.\n\n"
        "**Maps.** State polygons are GISCO NUTS 2024 boundaries (1:10 million) drawn with Vega-Lite geoshape "
        "in a Mercator projection; the geometry is read from the warehouse when the dashboard file is generated "
        "and joined to each query's values by NUTS 1 code. Colour scales are "
        "a single sequential blue (near-white = lowest, dark blue = highest), readable with any form of colour "
        "vision deficiency; every map is "
        "paired with a ranked bar chart and value labels so that no value depends on colour alone.\n\n"
        "**Limitations.** Sixteen states are large and unequal units: averages hide differences inside "
        "Bayern or Nordrhein-Westfalen, and the city-states combine urban cores without their suburbs. Values "
        "are in current prices and are not adjusted for regional price levels. Survey-based indicators "
        "(employment, tertiary education, poverty risk) carry sampling error, largest for Bremen and Saarland.\n"
    )
    return [
        {"tab": tab, "widgets": [{"name": "quality_header", "type": "text", "col": 12, "content": header}]},
        {"tab": tab, "widgets": [{
            "name": "Indicator quality table", "type": "table", "col": 12, "sql": sql,
            "columns": [
                {"name": "indicator_label", "label": "Indicator", "frozen": True},
                {"name": "reference_period_label", "label": "Reference period"},
                {"name": "latest_year_available", "label": "Latest year available", "number": "d"},
                {"name": "states_in_reference_year", "label": "States with value (of 16)", "number": "d",
                 "format": [{"if": "less_than", "value": 16, "backgroundColor": "#E69F00", "textColor": "#111111",
                             "bold": True}]},
                {"name": "germany_value", "label": "Germany value"},
                {"name": "dashboard_status", "label": "Dashboard"},
                {"name": "source_name", "label": "Source"},
                {"name": "source_table", "label": "Table / dataset"},
                {"name": "license", "label": "Licence"},
            ],
        }]},
        {"tab": tab, "widgets": [{"name": "quality_footnote", "type": "text", "col": 12, "content": footnote}]},
        {"tab": tab, "widgets": [{"name": "quality_divider", "type": "divider", "col": 12}]},
        {"tab": tab, "widgets": [{"name": "methodology", "type": "text", "col": 12, "content": methodology}]},
    ]


def income_intro_rows() -> list:
    content = (
        "# Disposable income per inhabitant: latest available year 2024\n\n"
        "**Shown separately because the latest official state-level release ends in 2024.**\n"
    )
    return [{"tab": "Income (2024)", "widgets": [{"name": "income_intro", "type": "text", "col": 12, "content": content}]}]


def build() -> dict:
    rows = overview_rows()
    for tab in ("Economy", "Society"):
        for ind in INDICATORS:
            if ind["tab"] == tab:
                rows += indicator_rows(ind)
    rows += income_intro_rows()
    rows += [r for ind in INDICATORS if ind["tab"] == "Income (2024)" for r in indicator_rows(ind)]
    rows += quality_rows()
    return {
        "name": "Germany by State - Economy and Society 2025",
        "description": "Choropleth maps of 16 economic and social indicators for the 16 German states, one reference year.",
        "connection": CONNECTION,
        "rows": rows,
    }


class LiteralDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        # Only the shared state geometry becomes a YAML anchor; everything else is written inline.
        return data is not STATE_FEATURES


def _str(dumper, data):
    style = "|" if "\n" in data else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=style)


LiteralDumper.add_representer(str, _str)


if __name__ == "__main__":
    STATE_FEATURES.extend(load_state_features())
    header = "# Generated by scripts/build_dashboard.py - edit the script, not this file.\n"
    OUT.write_text(header + yaml.dump(build(), Dumper=LiteralDumper, sort_keys=False, allow_unicode=True, width=120))
    print(f"Wrote {OUT}")
