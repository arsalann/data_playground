# VISUALIZATIONS.md

The single source of truth for data-visualization standards in this repo. Every chart in every dashboard MUST follow these rules. Violations are bugs.

This document covers:
1. Pre-build analysis discipline
2. Per-chart structure (title / chart / footnote)
3. Color and accessibility
4. Truthful representation
5. Encoding discipline
6. Label readability
7. Annotation and context
8. Layout
9. Visual review after build
10. DAC-specific rules (Bruin DAC quirks and fork-only fields)
11. Altair-specific rules (legacy dashboards only)
12. Matplotlib polar-plot rules (raw-asset analysis)

DAC-specific quirks that affect *how* a rule is implemented live in `DAC.md`. This file owns the *what* and *why*.

---

## 1. Before You Build Any Chart

- **Validate the data first.** Before writing any visualization code, query the data like a data analyst: check row counts, null rates, distributions, outliers, duplicates, and correlations. Understand what you actually have. Compute percentiles, check for join fanouts, verify dedup logic. Never visualize data you haven't inspected.
- **Analyze, don't summarize.** A dashboard that just shows "here's the data" is not analysis. Find correlations, compute derived metrics (e.g. price/ELO, gap ratios), identify Pareto frontiers, test hypotheses. Each chart must prove or disprove something non-obvious. If the takeaway is "the data exists," the chart has failed.
- **Follow the narrative arc.** Every dashboard tells a story: (1) hypothesis/question, (2) evidence the phenomenon exists, (3) evidence it is systematic, (4) quantification of magnitude, (5) implications and limitations. If a chart doesn't advance this arc, cut it.
- **Fewer charts, more narrative.** 2–4 well-chosen charts with clear explanatory text beats 8 charts that overwhelm the viewer. If you can say it in a sentence, don't make a chart.
- **Enrich aggressively.** Check what other datasets exist in BigQuery that could be joined. Cross-domain correlations are what make analysis interesting.
- **Be honest about sample sizes.** If a data point is based on 3 observations, say so in the chart's header text widget. Small-n medians are noise, not signal.
- **Explain the data.** Every dashboard must include: where the data comes from (with links), how it was collected and transformed, what the key metrics mean (with units), and what the limitations are. State explicitly what the data cannot tell you.
- **Don't make claims the data doesn't support.** If only 18 of 348 models have rankings, title the chart "The 18 models we can actually rank," not "Every Arena-Ranked Model." Caveat small-sample trends explicitly.
- **Tables can be better than charts.** A 10-row dataset does not need a chart. Use `type: table` and let the reader scan. Charts are for patterns in data too large to read as a table.
- **Quantify the insight.** The chart's description must state the specific finding with numbers (e.g. "r = 0.23, n = 580", "18x price difference for 5% quality gap"). The chart shows the pattern; the text states the magnitude.

---

## 2. Per-Chart Structure (Mandatory)

Every chart in every dashboard MUST be wrapped in the same three-row DAC pattern, in this exact order:

**header text widget → chart widget → footnote text widget**

Each widget on its own row at `col: 12`. The chart widget uses `hideName: true` so the WidgetFrame name strip doesn't compete with the header text. A chart that skips any part is incomplete.

### 2.1 Header text widget (`type: text`)

Contains, in order:

1. **Title** as a Markdown `#` heading that names what the chart *is* (not the finding). Example: `# Hourly temperature, six Paris stations, 2026-04-06`. Be unambiguous about entities, units, and time range. **Do not prefix titles with "Chart N —" or any numbering.** Numbering rots when charts are reordered, adds visual noise, and conveys no information; the position in the dashboard already orders them.
2. **Description** in bold paragraph form — 1–3 sentences stating the *insight*: what the reader should take away, with magnitudes (correlation, slope, ratio, top-N). State whether the data "supports", "rejects", or shows "no signal" relative to the hypothesis.
3. **Encoding key** as a final line listing what each visual channel means. Example: `**Left axis:** temperature (°C). **Right axis:** Polymarket Yes-price (0–1, dashed).` This line IS the legend for chart types that don't render one natively (see § 10.1).

### 2.2 Chart widget (`type: chart`, `hideName: true`, `col: 12`)

- Span the full row when in doubt; chart heights are not reachable from YAML (fork-controlled).
- Use `seriesNames:` to map snake_case columns to readable legend labels (line-chart fork only).
- For chart types that render no native legend, ensure the header text widget's encoding key covers every series — or switch to `chart: combo`.

### 2.3 Footnote text widget (`type: text`)

Three bolded sections separated by blank lines:

- **Sources:** every dataset used by *this* chart, with publisher and license. Bold proper-noun source names and link them. Example: `**Sources:** **[Meteostat](https://meteostat.net)**, **[Open-Meteo](https://archive-api.open-meteo.com/v1/archive)**.`
- **Tools:** `**Bruin cli** (pipeline), **BigQuery** (warehouse), **Bruin dac** (visualization).` — use "Bruin cli" for the pipeline (ingestion/staging/report) and "Bruin dac" for the visualization layer. Capital `B`.
- **Limitations:** sample-size warnings, geographic scope, time-window caveats, data-source quirks, methodology notes. Be specific. The footnote is per-chart, not per-dashboard, because each chart's caveats differ.

### 2.4 Roles cheat sheet

| Element | Role |
|---|---|
| Title | What the chart shows (entities, metric, units, time range) |
| Description | The insight (finding with magnitude) |
| Footnote | Sources, tools, limitations |

### 2.5 Methodology section

End every dashboard with a final `type: text` Methodology widget that consolidates joins, normalizations, definitions, and threshold choices that apply across charts. Include source links and explicit limitations.

---

## 3. Color and Accessibility

- **Colorblind-safe palette only — Wong (2011) from *Nature Methods*:**
  - `#D55E00` vermillion
  - `#56B4E9` sky blue
  - `#E69F00` orange
  - `#009E73` bluish green
  - `#CC79A7` reddish purple
  - `#0072B2` blue
  - `#F0E442` yellow
  - `#999999` grey

  These 8 colors are the **maximum** for categorical encoding. If you need more categories, aggregate or facet — do not invent new colors. The DAC `ibm-cb-dark` theme exposes a compatible `chart-1..chart-8` palette via CSS custom properties; prefer it for any new dashboard.

- **Never rely on color alone.** Pair color with a secondary channel: stroke-dash, shape, position, direct value label, or an explicit entry in the encoding-key line of the header text widget. A viewer who cannot distinguish any two colors must still be able to read the chart from the surrounding text.

- **No red/green for binary states.** Use vermillion (`#D55E00`) and sky blue (`#56B4E9`), or vermillion and grey instead.

- **Sequential and diverging scales.** Sequential: `blues` or `viridis`. Diverging: `blueorange`. Never `redgreen`, `redblue`, `rainbow`, or `jet` — all are colorblind-hostile or perceptually non-uniform.

- **Legends are mandatory on multi-series charts.** Where the chart type does not render a native legend (see § 10.1), the encoding-key line in the header text widget IS the legend — make it explicit and exhaustive. For line charts in DAC, the local fork renders a bottom legend automatically; use `seriesNames:` to give it readable labels.

---

## 4. Truthful Representation

- **Y-axis baseline.** Bar charts and area charts MUST start the quantitative axis at zero. A truncated axis exaggerates differences and misleads the viewer. Do not use `yMin`/`yMax` to truncate a bar chart. If zero-baseline makes the data unreadable (e.g. ELO scores clustered in 1300–1500), switch to a line or scatter chart with explicit `yMin`/`yMax`, never a truncated bar.

- **Log scales must be labeled.** When using a log scale, the chart's title or encoding-key line MUST include "(Log Scale)" and the description must explain why (e.g. "Log scale used because values span 3+ orders of magnitude"). Never use a log scale to make a trend look more dramatic.

- **No dual y-axes by default.** They are virtually always misleading — the viewer cannot compare magnitudes across two unrelated scales. Prefer vertically stacked widgets for related metrics with different scales. The single sanctioned exception is DAC's local-fork `yRight` on `chart: line` for cases where co-temporal alignment matters more than independent reading (e.g. temperature °C vs Polymarket Yes-price 0–1). In that case the description MUST call out the dual axis explicitly and the right-axis series renders dashed.

- **No pie charts, no 3D.** 3D adds no information and distorts area/length perception. Pie charts are inferior to bar charts for comparing quantities (Cleveland & McGill 1984). Use horizontal bar charts sorted by value instead.

- **Reference lines on probability charts.** Charts of probabilities, proportions, or rates that have a meaningful threshold (50% for binary, 100% for completion, etc.) should render that threshold as a constant series and call it out in the description.

---

## 5. Encoding Discipline

- **Every visual encoding must be explained** in the header text widget's encoding-key line or via a native legend. If a chart uses size, color, or shape as a data channel, name what each channel encodes.
- **Limit encodings to 3 channels max per chart.** Position (x, y) plus one of {color, size}. Adding more channels overloads working memory. If you need more dimensions, use a second widget or a `type: table`.
- **Tooltips are mandatory.** They are on by default in DAC. Ensure every encoded field has a sensible column name and a `format:` where numeric formatting is needed (`,.0f` integers, `$.3f` prices, `,.1%` percentages).
- **Sort bars by value.** Categorical bar charts must be sorted by the quantitative axis (largest to smallest or vice versa) unless there is a natural order (e.g. time, tiers). Do the sort in SQL (`ORDER BY` in the widget's query).

---

## 6. Label Readability

- **Don't pre-truncate string fields.** Pass the full string into the chart and let the rendering layer handle width — pre-slicing the data ("…") strips information from the tooltip and prevents readers from seeing the full text on hover.
- **Column names are visible.** DAC's legend (where rendered) and tooltips show the SQL column name verbatim unless you provide `seriesNames:` (line-chart fork only). Pick column names that look OK unmangled (`yes_price`, not `yp_x25_raw`).
- **Axis titles include units.** Use `yLabel:` / `yRightLabel:` (line-chart fork) to set axis titles, or include units in the encoding-key line of the header text widget.
- **Units everywhere.** Every metric in every chart — axis titles, heatmap labels, tooltips, KPIs — must include units. "Building height" is ambiguous; "Building height (m)" is not.

---

## 7. Annotation and Context

- **Reference lines.** DAC does not expose a generic reference-line API. Emit the threshold as an additional constant series in SQL (e.g. `0.5 AS fifty_pct_line`) and call it out in the description.
- **Chart titles state what is shown** (entities, metric, units, time range). The finding belongs in the description.
- **Axis titles are required** on both axes unless the meaning is unambiguous from context. For `chart: line`, use the fork's `yLabel:` / `yRightLabel:`. For other chart types, name the units in the encoding-key line.

---

## 8. Layout

- **3-row pattern per chart** (header text → chart → footnote text), each at `col: 12`, with `hideName: true` on the chart widget.
- **KPI rows above the first chart story.** Use `type: metric` widgets at `col: 3` × 4 (or `col: 4` × 3) with `prefix:`, `suffix:`, and `format:` for numeric formatting.
- **`type: divider`** between major story sections, not between every chart.
- **12-column grid.** Sum of `col:` per row ≤ 12. Full-width hero: `col: 12`. Paired comparisons: `col: 6` × 2. KPI rows: `col: 3` × 4. Never put unrelated charts side-by-side.
- **Data tables complement charts.** Show the underlying data (top-N, summary) as a `type: table` widget below a complex chart so the viewer can verify what they see.
- **Chart height must fit the data.** Size each chart so that (a) every axis tick label is fully rendered without truncation or overlap, (b) bars/points are not squished into illegible slivers, and (c) adjacent data points are visually distinguishable. Bar charts with many categories need more vertical space; scatter plots with dense clusters need more area to separate points; line charts with multiple series need enough height for legend + plot area without the plot collapsing. If the chart is squished, increase height — do not drop labels or shrink fonts. In DAC, chart height is fork-controlled; if a chart is too cramped, file it as a fork fix rather than papering over it with truncated labels.

---

## 9. Visual Review After Build

Code-level validation (`dac validate`, `dac check`, type-checks) confirms the dashboard *runs*. It does not confirm that the dashboard *reads*. Before declaring a dashboard done, open each chart in a browser and verify it is legible.

- **Screenshot every chart in the dashboard** using Playwright (or an equivalent headless-browser tool) and inspect each one. The pipeline is: start the dev server, navigate to the dashboard, capture each chart widget's bounding box, and visually review the output.
- **Check for, and fix, any of the following:**
  - Overlapping axis tick labels (rotate, truncate-with-tooltip, or increase chart width/height).
  - Overlapping data labels or annotations.
  - Truncated axis titles, legend entries, or footnote text.
  - Data points sitting on top of each other to the point that they cannot be distinguished (add jitter, switch to a binned/heatmap encoding, or resize).
  - Legends colliding with the plot area.
  - Tooltips that show raw column names instead of human-readable labels.
  - Colors that are indistinguishable when adjacent (see § 3).
- **Every text element and data point must be readable and distinct.** If two labels overlap, or two points cannot be told apart, the chart fails review and must be fixed before merge.
- **The visual review is per-chart, not per-dashboard.** A dashboard with eight charts requires eight screenshots reviewed, not one.

---

## 10. DAC-Specific Rules

All new dashboards in this repo are built with Bruin DAC. See `DAC.md` for the full set of CLI commands, install steps, and quirks. The rules below are the ones that affect *visualization correctness*.

### 10.1 Legends only render on some chart types

| Chart type | Native legend? |
|---|---|
| `line` | No upstream — **the repo's local fork adds one** |
| `bar` (unstacked) | No |
| `bar` (stacked) | Yes |
| `area` | No |
| `pie`, `funnel`, `combo`, `calendar` | Yes |
| `scatter`, `bubble`, `heatmap` | No |

To get a legend on a multi-series chart that doesn't render one natively, either use `chart: combo` or rely on the local fork's `line` legend, and ALWAYS include an explicit encoding-key line in the header text widget.

### 10.2 Local-fork fields (line charts only)

The repo maintains a DAC fork at `.context/dac-fork/` that adds these widget fields on `chart: line`:

| Field | Type | Effect |
|---|---|---|
| `yLabel` | string | Left y-axis title |
| `yRight` | string[] | Field names on a second (right) y-axis; rendered dashed |
| `yRightLabel` | string | Right y-axis title |
| `seriesNames` | object | Map snake_case column name → display label (used in legend + tooltip) |
| `hideName` | bool | Suppress the WidgetFrame title strip above any widget |

The `line` case unconditionally renders `<Legend iconType="line" verticalAlign="bottom" />`.

### 10.3 SQL → widget plumbing

- **Column names must be plain identifiers.** `bruin query` rejects spaces, parens, dashes, accents. Use `snake_case` in SQL output, then map to display names via `seriesNames:` (line-chart fork only) or via the encoding-key line.
- **ISO-timestamp x-axes get auto-stripped to date-only labels** (`Apr 6`). To preserve hour-of-day or sub-day labels, emit a non-ISO STRING in SQL: `FORMAT_TIMESTAMP('%H:%M', ts_local_paris) AS time_label`. Order rows in SQL.
- **Tooltips on by default.** Use `format:` for numeric formatting.

### 10.4 Themes are color-only

Themes change colors only. Font sizes, paddings, and the widget-title strip ("name") are hardcoded Tailwind classes. The fork's `hideName: true` suppresses the title strip; otherwise it's always there. Maximum reachable heading size from a text widget's Markdown is `# h1` (~19.5 px).

### 10.5 Text widget Markdown is plain `react-markdown`

No raw HTML, no GFM extensions, no `rehype-raw`.

### 10.6 Do not invent DAC features that don't exist

The widget schema is in `DAC.md` § "All widget properties". Anything not on that list (or in the fork-only fields above) will be ignored or fail validation.

---

## 11. Altair-Specific Rules (Legacy Only)

These apply only when modifying pre-DAC Streamlit dashboards. New work uses DAC.

- **Layered charts must share field names.** If a dot layer uses `population_2015` and a line layer uses `y`, Altair creates independent scales. Rename DataFrame columns to match across layers.
- **Angle values must be 0–360.** `angle=-33` raises a validation error in newer Altair. Use `angle=327` (equivalent rotation).
- **Log scales on every layer.** When using `alt.Scale(type="log")`, every layer sharing that axis must also specify the log scale explicitly — it does not propagate.
- **`zero=False` on Y-axis for tight ranges.** When data clusters in a narrow range (e.g., building heights 10–50 m), use `alt.Scale(zero=False)` on scatter/point charts to spread the data. **Never on bar charts** — that violates § 4 (truthful representation).
- **Interactive legends.** Use `alt.selection_point(fields=[...], bind="legend")` so viewers can click legend entries to filter. Encode both color and shape on the same field for dual encoding (accessibility).
- **Standard heights:** 380 px default; 450–500 px for scatter/dense charts.

---

## 12. Matplotlib Polar-Plot Rules (Raw-Asset Analysis)

Only relevant for raw-asset notebooks or one-off Python analyses producing polar plots (e.g. street-orientation rose diagrams). Do not use Matplotlib for DAC dashboards.

- **Default orientation is wrong for compass bearings.** Matplotlib's default: 0° at East (right), counter-clockwise. For street-orientation plots, set `ax.set_theta_zero_location("N")` and `ax.set_theta_direction(-1)` BEFORE drawing bars to get North at top, clockwise.

---

## When to Update This File

Append to (or amend) this file any time:
- A new visualization rule is established by the user or by review.
- A DAC quirk affects how a rule is implemented (cross-link to `DAC.md` for the mechanism).
- A pipeline-specific convention proves useful enough to promote to a global standard.

Keep it tight. Every rule here exists because skipping it produced a chart that misled, confused, or wasted the reader's time.
