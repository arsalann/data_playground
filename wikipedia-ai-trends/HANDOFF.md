# wikipedia-ai-trends — Handoff

## Goal

Build a Bruin DAC dashboard that analyzes how widely English Wikipedia's
Vital Articles Level 4 (~9,907 curated articles across 11 top-level subjects)
reference AI-topic articles, across 14 semiannual snapshots from
Dec 2019 → May 2026. Three questions:

1. **Prevalence by subject** — which subjects reference AI most? least?
2. **Trend over time** — which subjects' AI-reference share grew fastest?
3. **Surprises** — sub-subjects with higher/lower AI presence than their
   parent subject's mean.

## Current state

Dashboard is live at **http://localhost:8321** (served by `dac serve`).

Pipeline (9 assets) all PASSing. `dac check` clean (26/26 widgets).
BigQuery state, project `bruin-playground-arsalan`:

| Snapshot     | valid rows | null rows |
| ------------ | ---------: | --------: |
| 2019-12-01   |      9,894 |        13 |
| 2020-05-01 → 2026-05-01 | ≥9,849 | ≤58 each |

Dec-2019 backfill complete. Every top-level subject shows positive growth
in AI-reference share; Society and social sciences leads (+2.31pp),
History trails (+0.14pp). Article counts now stable across snapshots
(Math 293→293, Philosophy 430→430, People 1933→1933).

Chart titles no longer use "Chart N —" numbering; the rule was promoted
into `VISUALIZATIONS.md` § 2.1 as a global standard. The trend-over-time
line chart now formats the x-axis as `Mon YYYY` (e.g. `Dec 2019`,
`May 2020`) via a `FORMAT_DATE` string column, rather than the
DAC-auto-stripped `Dec 1 / May 1` labels.

## Files in flight

None — all changes from this session are committed.

```
wikipedia-ai-trends/
├── HANDOFF.md                                ← this file
├── pipeline.yml
├── assets/
│   ├── raw/
│   │   ├── wat_vital_articles.py
│   │   ├── wat_ai_seed_articles.py
│   │   └── wat_article_snapshots.py
│   ├── staging/
│   │   ├── wat_universe.sql
│   │   ├── wat_ai_articles.sql
│   │   └── wat_ai_reference_counts.sql
│   └── report/
│       ├── wat_category_metrics.sql
│       ├── wat_subcategory_metrics.sql
│       └── wat_article_growth.sql
├── dashboard-dac/
│   └── dashboards/wikipedia-ai-trends.yml
├── screenshots/
│   ├── take.mjs                              ← Playwright capture + slice
│   ├── 00_full_page.png + per-widget PNGs
│   ├── slice_00.png … slice_05.png
│   └── zoom_chart3*.png, zoom_chart4.png
└── scripts/
    ├── backfill_dec2019_fetch.py             ← Phase A (Wikipedia → DuckDB)
    ├── backfill_dec2019_load.py              ← Phase B (DuckDB → BigQuery)
    └── wat_dec2019_backfill.duckdb           ← local cache (gitignored)
```

## Changed (most recent session)

- **`VISUALIZATIONS.md`** § 2.1: added a global rule prohibiting "Chart N —"
  numbering in chart titles. Numbering rots when charts are reordered, adds
  visual noise, and conveys no information.
- **`dashboard-dac/dashboards/wikipedia-ai-trends.yml`**:
  - Stripped "Chart N —" prefixes from every section heading, widget
    `name:` field, and section comment header.
  - Renamed widget `name:` fields from `Chart N header / footnote` to
    descriptive names (e.g. `Trend over time header`).
  - Trend line chart x-axis: replaced `snapshot_date` (DATE) with a
    SQL-formatted `FORMAT_DATE('%b %Y', snapshot_date) AS snapshot_label`
    so labels render `Dec 2019` … `May 2026` instead of DAC's
    auto-stripped `Dec 1 / May 1`. Applied to both the 8-subject panel
    and the 3-subject (People/History/Geography) panel. Encoding-key
    copy updated to "month + year".
- **`.gitignore`**: added `node_modules/` and `*.duckdb`.

## Changed (prior sessions)

- **`wat_article_snapshots.py`**: `WINDOW_START` widened from
  `2019-11-30T00:00:00Z` to `2001-01-01T00:00:00Z`; added `WAT_REQUEST_DELAY`
  env var; wrapped `r.json()` in try/except for `ValueError`; bumped retries
  to 10 attempts; tightened progress checkpoints from every-200 → every-50
  (Pass 1) and every-20 → every-5 (Pass 2).
- **`scripts/backfill_dec2019_fetch.py`** (NEW): standalone Phase A that
  re-scrapes Vital Articles, fetches the closest revision ≤ 2019-12-01 per
  article, batch-fetches content, extracts wikilinks, writes to local DuckDB.
  Resumable.
- **`scripts/backfill_dec2019_load.py`** (NEW): Phase B — reads DuckDB, joins
  against existing BigQuery rows, appends only the missing Dec-2019 rows
  (schema fields declared REQUIRED for primary-key columns).
- **`screenshots/take.mjs`** (NEW): Playwright headless capture — full page,
  per-widget cards (by climbing from text locator to nearest card container),
  and full-page slices via sharp.

## Failed attempts

1. **`bruin run --downstream` after BQ delete of 7,867 NULL Dec-2019 rows**:
   silent failure at ~30 min with no python log lines, suspected runner
   timeout because every API call was 429-throttled and producing no stdout.
   Wikimedia had us in a per-IP throttle bucket from the original 130K-row
   ingestion.
2. **Slow re-run with `WAT_MAX_WORKERS=1` + 2s delay**: died at exactly
   30 min, same silent pattern. Confirmed bruin/runner timeout on prolonged
   stdout silence, not laptop sleep or OOM.
3. **VPN re-run via bruin**: Wikipedia worked, but Google blocked the VPN
   exit IP with `403 Forbidden` from BigQuery API. Script failed at the
   first `bq.query()` call (universe load).
4. **First Phase B run**: `400 Bad Request` — schema mismatch on
   `load_table_from_dataframe` because `article_title` and `snapshot_date`
   are `REQUIRED` in the target table but were declared `NULLABLE` in the
   load schema. Fixed by adding `mode="REQUIRED"`.
5. **Line stroke width via YAML**: DAC's line chart has no `strokeWidth`
   field, and the local fork referenced in `DAC.md` at `.context/dac-fork/`
   does not currently exist in this workspace. Thicker lines would require
   restoring/rebuilding the fork. Skipped for now.

## Next steps

- Optionally delete `scripts/wat_dec2019_backfill.duckdb` (no longer needed
  now that data is in BigQuery; gitignored so not committed either way).
- Consider whether `scripts/backfill_dec2019_*.py` should stay as
  documentation of the two-phase pattern, or be removed as one-off scripts.
- If thicker line strokes are wanted on the trend chart: restore/clone the
  DAC fork at `.context/dac-fork/`, expose `strokeWidth` on the line widget
  (or bump Recharts' default from 1.5 → ~2.5), rebuild the binary, install
  to `~/.local/bin/dac`. ~30 min of fork plumbing.
- If the dashboard moves beyond local: `dac serve` is one-shot; productionize
  via `dac build` + static hosting, or expose under a real domain.
- The pipeline currently runs `bruin run` end-to-end. Future snapshots
  (Dec 2026, May 2027, …) can be added by editing `SNAPSHOT_DATES` in
  `wat_article_snapshots.py` — the asset is idempotent.
