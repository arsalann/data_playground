# Wikipedia AI Trends - handoff (session 2026-05-18, curated report refinement)

## Goal

1. Trim `wikipedia-ai-reference-report.yml` to a focused, narrative dashboard: a headline "biggest gainers" chart plus a 6x2 small-multiples grid of sub-subject trends, with per-panel filtering rules that avoid small-denominator noise.
2. Iterate on layout and filter rules in response to user feedback: bigger titles, top-N selection by article count rather than share, drop sub-subjects with zero AI refs at latest, drop sub-subjects below a minimum article count, reorder panels.
3. Remove repetitive text and put sources/tools/limitations directly under the chart title rather than in a separate footnote widget.

## Current state

- **`wikipedia-ai-reference-report.yml` - DONE this session.** Final structure:
  - Row 1: "Biggest gainers in AI-reference share since Dec 2019, by sub-subject" - one full-width line chart of growth-from-baseline (cumulative pp change). All 8 lines start at zero at Dec 2019. Series are the eight sub-subjects (n>=30) with the largest absolute pp gain to latest: Mass media, Education, Internet culture (extended), Business and economics, Computing and IT, Electronics, Film and television, Statistics and probability. Includes inline sources/tools/limitations under the title.
  - Rows 2-13: 6x2 small-multiples grid. Each grid cell uses two YAML rows (title text widget at `col: 6` with `# Subject` markdown heading, then chart widget at `col: 6` with `hideName: true`). Reordered per user request: Row 1 Technology + Society; Row 2 People + Arts; Row 3 Mathematics + Philosophy & religion; Row 4 Everyday life + Geography; Row 5 Biology & health + Physical sciences; Row 6 History alone.
  - Methodology section at the bottom (single text widget).
- **Filter rule** for each grid panel: pick the top-7 sub-subjects by total article count at the latest snapshot, restricted to those with (a) at least 20 articles AND (b) at least one AI-referencing article at the latest snapshot. The 20-article floor was added after the user spotted "Arts: General" jumping 0% -> 50% on a single AI-ref edit (n=2 denominator). Final panel line counts: History 2; Everyday life and Geography 3 each; Mathematics 4; Arts and Physical sciences 5 each; Biology & health 6; Society, Technology, People 7 each; Philosophy & religion 2 (only 2 sub-subjects exist).
- **Em dashes removed** across the report file (replaced with plain hyphens). Memory feedback saved: never use em/en dashes anywhere.
- **Sources line corrected.** Previously included the BigQuery table path; now names the upstream API/publisher only (Wikipedia MediaWiki Action API + Wikipedia Vital Articles / Level 4). Memory feedback saved: "Sources" = upstream origin, never the internal BQ table.
- **`dac serve` still running on port 8321** with live reload; dashboard renders correctly with the new content.

- **`dac check` is currently FAILING for all three dashboards.** The local `dac` binary at `~/.local/bin/dac` was rebuilt earlier today (Mar timestamp 12:08) without the fork's schema patch for `hideName`, `yLabel`, `seriesNames`. The YAMLs use these fork-only fields and the upstream schema rejects them. This was passing earlier in the session, so it broke mid-session when the binary was rebuilt. Fix is on the fork side - reapply `schemas/dac/dashboard/v1/schema.json` patch in `.context/dac-fork/` and rebuild per DAC.md, not in any YAML.

- **Prior session's Plan A/D pipeline changes - STILL UNCOMMITTED but being included in this commit per user request.** Important context:
  - `report.wat_category_metrics` and `report.wat_subcategory_metrics` have a `cohort` column (`all` | `balanced`); composite PK includes cohort.
  - Universe extended from 9,907 (Vital L4) to 14,004 articles via `raw.wat_wikiproject_articles` (Companies, Brands, Computing, Internet culture, Business at Top + High importance).
  - **Aggregate widget queries reading `wat_category_metrics` / `wat_subcategory_metrics` are returning 2x rows in the main `wikipedia-ai-trends.yml` dashboard because no widget filters on cohort.** This bug pre-dates this session and remains unresolved. See "Next steps".
  - May-2026 cross-subject share with the extended universe: 4.73% (cohort='all') vs 4.51% (balanced).

## Files in flight

This session's changes:

- `wikipedia-ai-trends/dashboard-dac/dashboards/wikipedia-ai-reference-report.yml` - major restructure (see "Changed" below).
- `wikipedia-ai-trends/HANDOFF.md` - this file.

Carried over from prior session (also being committed now):

- `wikipedia-ai-trends/dashboard-dac/dashboards/wikipedia-ai-trends.yml` - per-subject summary table + 3x4 small-multiples grid added in prior session. Currently exposes the cohort bug (duplicate bars).
- `wikipedia-ai-trends/dashboard-dac/dashboards/wikipedia-ai-trends-investigation.yml` (NEW) - third dashboard from prior session.
- `wikipedia-ai-trends/assets/raw/wat_wikiproject_articles.py` (NEW) - WikiProject article puller for the extended universe.
- `wikipedia-ai-trends/assets/raw/wat_article_snapshots.py` - prior-session edits.
- `wikipedia-ai-trends/assets/staging/wat_universe.sql` - extended universe joins.
- `wikipedia-ai-trends/assets/staging/wat_ai_reference_counts.sql` - matches extended universe.
- `wikipedia-ai-trends/assets/report/wat_category_metrics.sql` - cohort column added.
- `wikipedia-ai-trends/assets/report/wat_subcategory_metrics.sql` - cohort column added.
- `.gitignore` - `**/screenshots/` excluded.
- `AGENTS.md` - screenshot-cleanup rule under "Things to Avoid".
- `VISUALIZATIONS.md` - prior-session edits (chart anatomy, etc).

## Changed this session

1. **Trimmed report to grid only.** Removed the over-scoped "Where / When / What" three-act framing from the previous session: dropped the About widget, headline metrics row, prevalence-by-subject bar (and its header + footnote), both "When" trend-line panels (and their header + footnote), the "Subject-level change Dec 2019 -> latest" table, and the per-subject summary table ("What drives each subject?"). Methodology widget retained and trimmed.

2. **Grid moved to top; 6x2 layout (col: 6 each).** Was 3x4 then 4x3 in prior iterations. Each subject panel now occupies two YAML rows (title text widget + chart widget) so the panel title can render as a markdown `# h1` heading - visibly larger than the hardcoded Tailwind name-strip. Charts have `hideName: true` to suppress the name strip.

3. **Added a headline "biggest gainers" chart at the top.** A growth-from-baseline line chart: every series starts at 0 at the Dec 2019 snapshot and shows cumulative pp change at each subsequent snapshot. Diverging trajectories visualize which sub-subjects gained the most AI-reference share over the 6.5-year window. n>=30 article floor applied.

4. **Filter rule on grid panels evolved through three steps:**
   - First pass: top-7 sub-subjects by latest-snapshot AI-reference share (the prior-session rule).
   - Second pass (user feedback "filter by article count"): top-7 by total article count at latest snapshot, regardless of AI-share.
   - Third pass (user feedback "drop zeros"): top-7 by article count among sub-subjects with >0 AI ref at latest. Created `History` panel with only 3 lines, etc.
   - Fourth pass (user spotted Arts: General 50% jump, n=2): also require >=20 articles. Final filter.

5. **Inlined Sources/Tools/Limitations under the chart title** for both the gainers chart and the grid - removed the standalone footnote widgets that previously sat below each chart. Also moved the standalone "Reading the grid" legend cell into the grid header's description ("Y-axis is per-panel ... compare shapes, not heights").

6. **Reordered grid panels per user request.** New top-left-to-bottom-right order: Technology, Society & social sciences, People, Arts, then the rest. Used a Python helper script (`/tmp/reorder3.py`) to extract chart blocks by name and rebuild the rows section cleanly.

7. **Removed em dashes** across the file (replaced with plain hyphens). Memory feedback saved.

8. **Fixed Sources line.** Originally listed the BigQuery table path (`bruin-playground-arsalan.staging.wat_ai_reference_counts`); user pushed back that Sources must name the upstream origin (the API), never the warehouse table. Memory feedback saved.

## Failed attempts

1. **First reorder script left stale row blocks.** Initial `/tmp/reorder_panels.py` regex captured chart blocks loosely and didn't trim trailing orphan "Row N titles / Row N charts" markers, so the output had duplicated row comments interspersed with empty widget arrays. Second pass (`reorder2.py`) trimmed at blank lines and "  - widgets:" but still picked up old title-row content sitting between chart blocks. Third pass (`reorder3.py`) explicitly truncates each block at the first stale "# Row" or orphan "  - widgets:" line and only keeps lines through `ORDER BY snapshot_date`; this produced a clean reordering.

2. **`dac check` schema validation regressed mid-session.** Around the time the user did unrelated work, `~/.local/bin/dac` was rebuilt without the fork's `hideName / yLabel / seriesNames` schema patch; subsequent `dac check` runs began failing for all three dashboards. The YAMLs themselves are unchanged in shape - this is a fork build artifact, not a YAML bug. `dac serve` (which serves at runtime rather than re-validating) still renders.

3. **Schema-validation-as-source-of-truth assumption.** Earlier in the session I trusted `dac check`'s "all passing" output as proof the dashboard was complete; that signal disappeared once the fork rebuild dropped the schema patch. Lesson: render the dashboard in `dac serve` and inspect, don't rely only on `dac check`.

## Next steps

1. **Re-apply the fork's schema patch and rebuild `dac` binary** so `dac check` validates again. Per DAC.md: edit `.context/dac-fork/schemas/dac/dashboard/v1/schema.json` to allow `hideName`, `yLabel`, `yRight`, `yRightLabel`, `seriesNames` on widget objects; `make build`; `cp bin/dac ~/.local/bin/dac`; `codesign --force --deep --sign - ~/.local/bin/dac`.

2. **Fix the cohort duplicate-row bug in `wikipedia-ai-trends.yml` and `wikipedia-ai-trends-investigation.yml`.** Every aggregate widget reading `report.wat_category_metrics` or `report.wat_subcategory_metrics` needs `WHERE cohort = 'all'` (or `'balanced'`, depending on the intent) added to its SQL. Without this, every bar/line/table renders 2x rows because the new cohort column doubles every aggregate. The bug pre-dates this session and is unresolved; the report dashboard is unaffected because it reads from `staging.wat_ai_reference_counts` directly.

3. **Visual review of the report dashboard.** Per VISUALIZATIONS.md section 9, screenshot every chart and inspect for label overlap, legend collisions, truncated text. Headline gainers chart in particular has 8 series with long names - tooltip and legend should be legible.

4. **Clear `wikipedia-ai-trends/screenshots/`** per the AGENTS.md screenshot-cleanup rule. Folder is gitignored but on-disk PNGs and `_*.mjs` scripts remain.

5. **Surface the dashboard URL to the user when re-opened:** http://localhost:8321 -> select "Wikipedia AI Reference Report".

## Pointers

- `dac check --dir wikipedia-ai-trends/dashboard-dac` will currently fail until the fork schema is re-patched; use `dac serve --dir wikipedia-ai-trends/dashboard-dac` to render the dashboards regardless.
- `dac serve` was auto-watching files during this session; YAML edits live-reload without restart.
- Wong palette caps multi-series line charts at 8 colours - the per-panel 7-sub-subject cap is intentional.
- Each small-multiples panel uses its own y-range. The headline gainers chart at the top is the only place absolute pp magnitudes are directly comparable across sub-subjects.
- Reorder helper scripts saved at `/tmp/reorder_panels.py`, `/tmp/reorder2.py`, `/tmp/reorder3.py` - the third is the canonical one if the grid needs to be reordered again.
