# DAC — Working Notes

Living reference for [`bruin-data/dac`](https://github.com/bruin-data/dac) as we use it in this repo. Read this *first* before re-fetching upstream files. Append findings as new constraints / quirks are discovered.

---

## What it is

DAC = "Dashboard-as-Code". Go CLI + embedded Vite/React frontend. You write
YAML (or `.dashboard.tsx`) plus `.sql` files, DAC validates/serves them, queries
go through `bruin query` so any Bruin-supported warehouse works (BigQuery here).

Brand brief from upstream `CLAUDE.md`: precise, confident, minimal. Linear /
Vercel / Hex aesthetic. Density over decoration. Dark mode is first-class.

Stack: React 19 + TS + Vite + Tailwind v4 + Recharts. Theme tokens live in
CSS custom properties prefixed `--dac-*`.

## Install / connect

```bash
curl -fsSL https://raw.githubusercontent.com/bruin-data/dac/main/install.sh | bash
# in this repo, ~/.local/bin/dac is already on $PATH
```

`.bruin.yml` discovery walks UP from `--dir`. The repo-root `.bruin.yml`
is the one DAC picks up — no need for a per-project copy. The connection
we use is `bruin-playground-arsalan` (Google Cloud Platform, ADC creds).

`dac connections` will show "✗" rows for the `generic` API-token connections
in `.bruin.yml`; those are noise — only the `google_cloud_platform` row
matters for the dashboard.

## CLI cheat sheet

```bash
dac validate --dir .                # schema + reference checks
dac check    --dir .                # validate + execute every query
dac query    --dir . --dashboard NAME --widget NAME   # debug single widget
dac serve    --dir . --port 8321    # live-reload dev server (SSE)
dac connections                     # ping every connection
dac ls                              # list discovered dashboards
```

`--debug` for verbose logs. `--environment NAME` switches `.bruin.yml`
environments. Cache invalidates on file change; queries cached 5 min.

## Project layout (this repo)

```
polymarket-weather/dashboard-dac/
├── dashboards/
│   ├── polymarket-weather.yml
│   └── queries/
│       ├── spike_temps_apr06.sql
│       └── spike_temps_apr15.sql
└── semantic/                       # unused (semantic layer not needed yet)
```

## Dashboard YAML

Top-level fields:
- `name` (required), `description`, `connection`, `model` (semantic), `theme`,
  `refresh: { interval: "5m" }`, `filters: []`, `queries: {}`, `rows: []`.
- 12-column grid. Each `widgets: []` row has `col: 1..12` per widget; sum should
  not exceed 12.

Widget query sources (mutually exclusive):
1. `query: name` (named in `queries:`)
2. `sql: "..."` inline
3. `file: queries/foo.sql` relative to dashboard
4. `metric: <semantic-metric>` (needs a `model`)
5. Direct semantic fields (`dimension`, `metrics`, `filters`, etc.)

## Widget types

`metric, chart, table, text, divider, image`. From the v1 schema.

### Chart types (from schema enum)

`line, bar, area, pie, scatter, bubble, combo, histogram, boxplot, funnel,
sankey, heatmap, calendar, sparkline, waterfall, xmr, dumbbell`

### All widget properties

`alt, bins, chart, col, column, columns, connection, content, description,
dimension, dimensions, file, filters, format, granularity, label, limit,
lines, metric, metrics, model, name, prefix, query, segments, size, sort,
source, sql, src, stacked, suffix, target, type, value, x, y, yMax, yMin`

Notably absent: `legend`, `tooltip`, `series`, `style`, `class`, per-series
colour or stroke-width override.

## Hard-won quirks

These are the things that will bite you if you don't already know them.

### 1. Only some chart types render a legend

In `frontend/src/components/widgets/ChartWidget.tsx`:

| chart type | `<Legend />` rendered? |
|---|---|
| `line`     | **no** |
| `bar`      | only when `stacked: true` |
| `area`     | no |
| `pie`      | yes |
| `funnel`   | yes |
| `combo`    | yes |
| `calendar` | yes |
| heatmap, scatter, bubble, etc. | no |

If you need a legend on a multi-series chart, the cheapest workaround is
`chart: combo`.

### 2. The `combo` chart's API is non-obvious

It does NOT iterate both `y` and `lines`. It iterates **`y` only**, and for
each field:
- if the field is also in `lines` → render as `<Line />`
- else → render as `<Bar />`

So to draw 7 lines on a combo chart, list all 7 in BOTH `y` and `lines`. To
mix bars and lines, list everything in `y` and put the line-fields in `lines`.
`y` cannot be empty (validator rejects `y: []` for combo).

### 3. No dual y-axis on any chart type

`line` and `combo` both instantiate exactly one `<YAxis />`. There is no per-
series `yAxisId`, no `yLeft`/`yRight`, no second `<YAxis />`. To overlay two
metrics with very different scales (e.g. temperature in °C vs price in
[0–1]), you scale one of them in SQL onto the same numeric range as the
other and call out the scaling in the chart's caption / footnote.

Example we use today: `yes_price * 25 AS yes_price_x25` so a 0–1 implied
probability shares the 0–25 °C temperature axis.

### 4. X-axis ISO timestamps get auto-stripped

`formatAxisTick` turns any string matching `^\d{4}-\d{2}-\d{2}T...` into
`"Apr 6"` / `"Apr 6 19"`-style labels — the time of day is discarded. To
get hour-of-day labels, emit a non-ISO STRING in SQL:

```sql
FORMAT_TIMESTAMP('%H:%M', ts_local_paris) AS time_label
```

`time_label` is opaque to the formatter and renders identity. Order is
preserved by the `ORDER BY ts_local_paris` in SQL.

### 5. BigQuery output column names must be plain identifiers

DAC's `y: ["Paris CDG (suspect)"]` style with spaces / parens / dashes /
accents fails — `bruin query` rejects:

> Invalid field name "Paris-CDG (suspect)". Fields must contain the allowed
> characters, and be at most 300 characters long.

Use `snake_case` (`cdg_suspect`, `le_bourget`, `open_meteo_grid`). The
chart legend (when `combo`) shows the column name verbatim — there is no
display-name override field in the widget schema. Pick names that look OK
unmangled.

### 6. Themes are color-only

Theme tokens (`--dac-*` CSS vars) cover background, surface, border, accent,
text, success/warning/error, and `chart-1..chart-8` colours. **Font sizes,
spacing, paddings are hardcoded Tailwind classes** (e.g. the chart widget
title is `text-[11px]` in `frontend/src/themes/bruin/WidgetFrame.tsx`).
Themes can't change them.

### 7. Markdown text widgets use default `react-markdown`

`TextWidget.tsx` does `<Markdown>{content}</Markdown>` with no plugins. Raw
HTML is stripped. No `rehype-raw`. No GFM extensions. `### h3` is the
common heading; sizes baked into `frontend/src/index.css`:

```css
.dac-prose          { font-size: 13px; }
.dac-prose h1       { font-size: 1.5em;  }   /* 19.5 px */
.dac-prose h2       { font-size: 1.25em; }   /* 16.25 px */
.dac-prose h3       { font-size: 1.1em;  }   /* 14.3 px  */
.dac-prose h4       { font-size: 1em;    }   /* 13 px    */
.dac-prose strong   { font-weight: 600; }
.dac-prose code     { font-family: "Geist Mono"; bg: var(--dac-surface-hover); }
```

So the **maximum** heading size reachable from YAML is `# h1` ≈ 19.5 px.
For anything larger you have to fork the frontend.

### 8. `name` is required and `minLength: 1`

Every widget needs a name, even text widgets where you'd prefer no caption.
`WidgetFrame.tsx` renders the widget.name as a small uppercase tracking-wide
header above the chart for everything except `text` and `divider`. There's
no way to suppress it via YAML.

### 9. `dac serve` SSE keeps the connection open forever

Playwright's `wait_until="networkidle"` will time out. Use
`wait_until="domcontentloaded"` plus a manual `time.sleep(N)` for queries
to settle.

### 10. No axis labels in upstream — line chart now has them via a local fork

Upstream DAC instantiates Recharts `<XAxis>` and `<YAxis>` without a `label`
prop and renders only one `<YAxis>` per chart type. Neither axis titles
nor true dual y-axis are reachable from YAML.

We maintain a fork at `.context/dac-fork/` that adds three optional widget
fields **for `chart: line` only** and rebuilds the binary:

| YAML field | Type | Effect |
|---|---|---|
| `yLabel`       | string   | Renders an axis title on the LEFT y-axis. |
| `yRight`       | string[] | Field names rendered on a SECOND y-axis on the RIGHT. Each gets its own dashed line styled `colors[(y.length+i) % colors.length]`. |
| `yRightLabel`  | string   | Renders an axis title on the RIGHT y-axis. |
| `seriesNames`  | object   | Map from data-column name → display label. Used as the `name=` prop on each `<Line>`, which is what the legend (and tooltip) shows. Keys with no entry fall through to the snake_case column name. |
| `hideName`     | bool     | Suppresses the small uppercase title strip rendered by `WidgetFrame` above the chart/table/metric. Useful when an adjacent text widget already serves as the section title and you want the chart frame to render just the data. Applies to any widget type. |

The `line` case also unconditionally renders a `<Legend iconType="line" verticalAlign="bottom" />` (upstream rendered no legend at all). Chart height is bumped from 240 px to 280 px to give the legend ~30 px without squishing the data area.

Example:

```yaml
- name: Temperature + price
  type: chart
  chart: line
  x: time_label
  y: [cdg, le_bourget, ...]                # left axis (°C)
  yLabel: Temperature (°C)
  yRight: [yes_price]                      # right axis (0-1)
  yRightLabel: Polymarket Yes-price (0–1)
```

Build / install steps:

```bash
cd .context/dac-fork
cd frontend && npm ci --legacy-peer-deps && npm run build && cd ..
make build                         # produces ./bin/dac
cp bin/dac ~/.local/bin/dac
codesign --force --deep --sign - ~/.local/bin/dac   # macOS Gatekeeper
```

Files patched in the fork:
- `pkg/dashboard/model.go` — `YLabel` / `YRight` / `YRightLabel` on `Widget`.
- `pkg/dashboard/jsloader.go` — same fields wired into the TSX prop loader.
- `schemas/dac/dashboard/v1/schema.json` — added the three properties.
- `frontend/src/types/dashboard.ts` — same fields on the TS `Widget` interface.
- `frontend/src/components/widgets/ChartWidget.tsx` — line case now renders
  `<YAxis yAxisId="left">` always plus `<YAxis yAxisId="right" orientation="right">`
  when `yRight` is non-empty, and routes each `<Line>` through the right
  `yAxisId`. Right-axis lines render `strokeDasharray="4 2"` to stay visually
  distinct from the left-axis lines.

Other chart types still have only one y-axis upstream-style. If we need
dual axis on `combo` or `area`, replicate the same change in those cases.

For non-line charts, fall back to the description-text-widget pattern from
the upstream constraint:

```yaml
- name: Foo header
  type: text
  col: 12
  content: |
    # Foo
    **Y-axis:** temperature in °C.
    **X-axis:** local hour-of-day (Europe/Paris, 00:00–23:00).
```

### 11. DAC content scrolls inside an inner container, not the document

`document.scrollingElement.scrollHeight === window.innerHeight` initially.
To scroll the dashboard in headless tests, find the largest descendant
with `overflow-y: auto/scroll` and call `el.scrollBy(...)` on that.

## Conventions for footnotes / descriptions

Sourcing block on every chart:

```
**Sources:** **[Source A](url)**, **[Source B](url)**, ...

**Tools:** **Bruin cli**, **BigQuery**, **Bruin dac**.

**Limitations:** ...
```

Use `Bruin cli` for the pipeline (ingestion/staging/report) and `Bruin dac`
for the visualization layer (capital `B`). Bold the proper-noun source /
tool names.

## Connection: BigQuery details

- Project / dataset prefix: `bruin-playground-arsalan.polymarket_weather_*`
- Datasets: `polymarket_weather_raw`, `polymarket_weather_staging`,
  `polymarket_weather_report`.
- ADC is configured via `gcloud auth application-default login`.

## Useful warehouse views (current dashboard)

- `polymarket_weather_report.spike_evidence` — hourly per-station readings on
  event days plus ±1 day of context (CDG flagged via `is_cdg`).
- `polymarket_weather_staging.markets_enriched` — market metadata
  (`bucket_value_c`, `bucket_kind`, `series_slug`, `end_date`).
- `polymarket_weather_staging.market_resolutions` — per-event resolved bucket
  including `winning_bucket_observed` and `winning_bucket_kind_observed`
  (`point` / `le` / `ge`).
- `polymarket_weather_staging.prices_enriched` — tick-level Yes-side prices,
  joined to `markets_enriched.market_id`.

## When to update this file

Append a new section (or extend the relevant one above) any time:
- A YAML field doesn't behave the way the schema implies.
- A chart type renders differently than expected.
- A theme token changes the wrong thing (or the right thing).
- An upstream commit changes a default we relied on.

Keep it terse: every paragraph here exists because we hit the wall it
describes.
