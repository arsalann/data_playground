# ai-economy — Pipeline Build Prompt

Generated from `/prompt.md` template. Hand this prompt to an agent to build the pipeline end-to-end.

---

## Prompt

Build a new pipeline for **Anthropic Economic Index — AI adoption across the real-world economy**.

**Context:** Utilize Bruin MCP and Bruin CLI, reference Bruin docs. Follow `@AGENTS.md` strictly — these are the rules for this repo. If you are about to break any rule in `AGENTS.md`, stop and ask for clarification and permission before proceeding. Use `baby-bust/` and `city-pulse/` as the primary reference implementations (canonical data validation + multi-source joins).

The Anthropic Economic Index (AEI) is Anthropic's quarterly-ish release of anonymized telemetry about how Claude is used in real economic work — by O*NET task, country, collaboration pattern, and (for the 1P API) by token cost. It is the closest public dataset to a ground-truth measurement of "which jobs is AI actually doing, and how." Our hypothesis: **high-wage cognitive work is being augmented while low-wage task categories are being automated**, and this pattern should be measurable by joining AEI task usage against BLS wage data and O*NET occupational mappings.

### Data source

**Primary — Anthropic Economic Index (HuggingFace `Anthropic/EconomicIndex`, license CC BY 4.0):**

| File | Path | Description |
|---|---|---|
| Claude.ai usage (2026-01-15 release) | `release_2026_01_15/data/intermediate/aei_raw_claude_ai_2025-11-13_to_2025-11-20.csv` | Long-format: geo × facet × variable × cluster_name → value. Covers country and country-state (ISO 3166-2). Facets include `onet_task`, `collaboration`, `request`, `use_case`, `human_only_ability`, `ai_autonomy`, `human_education_years`, etc. |
| 1P API usage (2026-01-15 release) | `release_2026_01_15/data/intermediate/aei_raw_1p_api_2025-11-13_to_2025-11-20.csv` | Same long schema but GLOBAL-only. Adds token/cost-index metrics per task: `onet_task::cost`, `::prompt_tokens`, `::completion_tokens`. |
| ISO country code map | `release_2026_01_15/data/intermediate/iso_country_codes.csv` | Maps ISO 3166-1 alpha-2 ↔ alpha-3 ↔ country name. Join key for enrichment. |
| Prior Claude.ai snapshot (2025-09-15 release) | `release_2025_09_15/data/intermediate/aei_raw_claude_ai_*.csv` | Same schema, 2-month-earlier snapshot. Used only for release-over-release comparison on overlapping tasks. |

Base URL pattern: `https://huggingface.co/datasets/Anthropic/EconomicIndex/resolve/main/<release>/<file>`. No auth.

**Enrichment sources:**

| Source | URL | Field(s) we need | Auth / License |
|---|---|---|---|
| O*NET Task Statements | `https://www.onetcenter.org/dl_files/database/db_29_2_text/Task%20Statements.txt` | `Task ID`, `O*NET-SOC Code`, `Task`, `Task Type` | None / CC BY 4.0 |
| O*NET Occupation Data | `https://www.onetcenter.org/dl_files/database/db_29_2_text/Occupation%20Data.txt` | `O*NET-SOC Code`, `Title`, `Description` | None / CC BY 4.0 |
| BLS OES 2024 National | `https://www.bls.gov/oes/special-requests/oesm24nat.zip` → `national_M2024_dl.xlsx` | `OCC_CODE` (SOC), `OCC_TITLE`, `TOT_EMP`, `A_MEDIAN` (annual median wage) | None / US public domain |
| World Bank Open Data API | `https://api.worldbank.org/v2/` | `NY.GDP.PCAP.CD` (GDP per capita), `SL.TLF.CACT.ZS` (labor force participation %), `SP.POP.TOTL` (population) | None / CC BY 4.0 |

### What to extract

Each raw asset lands the source data largely as-is (the pivot to wide happens in staging). Specifically:

- **AEI CSVs** — ingest all rows with no pre-filtering beyond `not_null` on `(geo_id, facet, variable, cluster_name)`. Cast `value` to DOUBLE, keep `date_start`/`date_end` as DATE. Add `release_id` column derived from the source release folder. Add `extracted_at` UTC timestamp.
- **ISO mapping** — keep `iso_alpha_2`, `iso_alpha_3`, `country_name`. Primary key: `iso_alpha_3`.
- **O*NET** — load Task Statements joined to Occupation Data. Keep `task_id` (unique), `onet_soc_code`, `task_description`, `task_type` (Core / Supplemental), `occupation_title`. Primary key: `task_id`.
- **BLS OES** — national-level only, detailed occupations only (filter `OCC_GROUP = 'detailed'` or equivalent). Keep `soc_code` (6-digit), `occupation_title`, `total_employment`, `median_annual_wage`. Primary key: `soc_code`.
- **World Bank** — three indicators × every country × year 2020–2024. Long format `(country_code, indicator_code, year, value)`. `append` strategy per repo convention. Chunk in 10-year windows, 5 retries with exponential backoff (see `baby-bust/assets/raw/worldbank_indicators.py` for the exact pattern).

### Naming

All asset/table names have prefix `aei_`.

Destination: BigQuery (connection `bruin-playground-arsalan`).

**Raw (Python, `image: python:3.11`, `create+replace` except World Bank which is `append`):**

- `raw.aei_claude_usage` — Claude.ai long-format CSV, 2026-01-15 release
- `raw.aei_api_usage` — 1P API long-format CSV, 2026-01-15 release
- `raw.aei_prior_snapshot` — Claude.ai long-format CSV, 2025-09-15 release
- `raw.aei_onet_tasks` — O*NET Task Statements + Occupation Data
- `raw.aei_bls_wages` — BLS OES national wages and employment
- `raw.aei_worldbank_context` — World Bank GDP / labor / population indicators (`append`)

**Staging (SQL, `bq.sql`, `create+replace`):**

- `staging.aei_task_exposure` — one row per O*NET task (current release), pivoted wide: `usage_count`, `usage_pct`, `ai_autonomy_mean`, `human_only_time_mean`, `human_with_ai_time_mean`, `human_education_years_mean`, `collaboration_directive_pct`, `collaboration_feedback_pct`, `collaboration_validation_pct`, `collaboration_learning_pct`, enriched with `onet_soc_code`, `occupation_title`, `bls_total_employment`, `bls_median_annual_wage`.
- `staging.aei_geographic_adoption` — one row per country × release, pivoted wide: `usage_count`, top-5 tasks by `onet_task_pct_index`, total distinct tasks with ≥200 conversations, enriched with `gdp_per_capita`, `labor_force_participation_pct`, `population`, `usage_per_million_people`.
- `staging.aei_collaboration_patterns` — one row per `(task_id, collaboration_type)` with `share_pct` (sums to 100 within task), joined to task/occupation metadata.
- `staging.aei_consumer_vs_api` — one row per task with `consumer_pct` (Claude.ai global), `api_pct` (1P API global), `delta_pp` (percentage-point difference), `api_cost_index`, `api_prompt_tokens_index`, `api_completion_tokens_index`.

### Dashboard questions

The goal is a final Streamlit + Altair dashboard that answers:

1. **Is AI augmenting or automating?** For each O*NET task, what is the mean `ai_autonomy` (0=pure assistance, 1=fully delegated)? Does it correlate with median wage or employment size?
2. **Where is Claude most used and for what?** Per-country usage-per-capita, and what is each country's top specialization task (highest positive `onet_task_pct_index`)?
3. **Consumer vs enterprise** — which tasks show the biggest split between Claude.ai (consumer) and 1P API (developer/enterprise) usage shares, and how does API token cost scale with that split?
4. **Collaboration DNA** — for the top-20 tasks by usage, what is the breakdown across collaboration patterns (directive / feedback loop / validation / learning)? Which tasks are delegated vs iteratively worked?

### Build order

Execute in this exact 12-phase sequence. Do not skip phases. Each phase gates the next.

1. **Initial preparations.** Create `ai-economy/pipeline.yml`, `README.md` skeleton, per-layer `requirements.txt` (raw: `pandas requests openpyxl`; reports: `streamlit google-cloud-bigquery db-dtypes altair pandas numpy pydeck`). Place an empty `.streamlit/secrets.toml` in `ai-economy/assets/reports/` (gitignored). Write minimal Bruin headers on empty asset stubs so that `bruin validate ai-economy/` passes before any code is written.
2. **Raw ingestion.** Write all six raw assets. Test each **individually** with small scope first:
   - AEI CSVs: use `AEI_MAX_ROWS=5000` env var to cap the first run. Verify the long schema lands as expected (`geo_id`, `facet`, `variable`, `cluster_name`, `value`, `date_start`, `date_end`, `release_id`, `extracted_at`).
   - World Bank: `bruin run --start-date 2023-01-01 --end-date 2023-12-31` first to smoke-test chunked retry logic.
   - BLS: fetch the full file (it's small) but verify row count matches expected ~820 detailed occupations.
   - After each asset, query BigQuery: row count, distinct PK, null rates on PK columns, extracted_at populated.
3. **Staging transformations.** Build the four staging SQLs. Test each individually after raw data lands. For each staging asset, write a parity query that joins staging back to the raw long table on the natural key and asserts zero diff on the pivoted columns.
4. **Reports layer.** Build `streamlit_app.py` + per-chart standalone SQL files (query fully-qualified `bruin-playground-arsalan.staging.*`). Iterate chart-by-chart; render each with `python3 -m streamlit run` and visually confirm before moving on. Use the Wong 2011 colorblind palette (see `AGENTS.md`). Register a global Altair theme for consistent legend/axis sizing (see `baby-bust/assets/reports/streamlit_app.py`).
5. **First full pipeline run end-to-end.** `bruin validate ai-economy/` then `bruin run ai-economy/`. Confirm no failures; confirm staging reflects latest raw; confirm all charts still render.
6. **Data analysis + metadata enrichment.** For every asset (raw and staging), add:
   - **Table-level `description:`** — what it is, data source URL, update cadence, license, row count expectation.
   - **Column-level `description:`** — every column gets units, value domain, and (for staging) lineage back to the raw source column.
   - **`tags:`** — `domain:ai_economy`, `domain:labor`, one of `source:anthropic`/`source:onet`/`source:bls`/`source:worldbank`, `update_pattern:create+replace` or `append_only`, `sensitivity:public`.
   - **`checks:`** — `not_null` on PK columns + `extracted_at`; `unique` on the composite PK; `accepted_values` on categorical columns (e.g. `geography IN ('country','country-state','global')`, `collaboration_type` in the known enum); `min`/`max` bounds where physically meaningful (`ai_autonomy_mean` ∈ [0,1], `usage_pct` ∈ [0,100], `year` ∈ [2015, 2026]).
   - **`custom_checks:`** — at least one SQL expression per staging asset (see Custom quality checks below), asserting zero failing rows.
7. **Common-sense (apples-to-apples) validation.** Before any chart claims a comparison, walk through the eight hazards listed in "Apples-to-apples rules" below. Fix any chart or staging logic that violates them. Add a `Methodology` section to the README documenting every normalization decision.
8. **Run `bruin ai enhance`** on each asset, one at a time. **CRITICAL**: after each `enhance`, immediately run `bruin validate ai-economy/`. If the YAML is corrupted, rewrite the columns block manually — never bulk-regex (`AGENTS.md`). Never run `enhance` on all assets in a batch.
9. **Second full pipeline run end-to-end.** `bruin run ai-economy/` again. Verify all enhance-added checks pass. If a check fails, decide whether it's a real data edge case (fix data or relax check with documentation) or an invalid check from enhance (correct manually).
10. **Spot-check validation.** For each chart, pick **3 data points** and trace end-to-end:
    - Query the raw long-format CSV directly (`SELECT value FROM raw.aei_claude_usage WHERE geo_id=? AND facet=? AND variable=? AND cluster_name=?`).
    - Query the staging pivoted value.
    - Query what the chart SQL file returns.
    - All three must agree to the byte. Document each trace in the README under a `Validation` section. If any mismatch, fix the pipeline (not the chart), re-run, repeat until zero mismatches.
11. **Final pipeline run.** `bruin run ai-economy/`. Capture the `MAX(extracted_at)` of every raw table in the README as the "as-of" date.
12. **Deep insight analysis.** Compute, quantify, and (if interesting) visualize:
    - **Pearson and Spearman correlation** between `ai_autonomy_mean` and `LOG10(bls_median_annual_wage)` across tasks with BLS match.
    - **Top-10 tasks with biggest consumer–enterprise divergence** (|`delta_pp`| descending). Compare to `api_cost_index` — is enterprise usage concentrated in the expensive tasks?
    - **Country specialization outliers** — tasks where a single country's `onet_task_pct_index` > 2× global mean (z-score > 2). Categorize by theme (e.g. translation, legal, coding).
    - **Release-over-release change** (2025-09-15 vs 2026-01-15) — for every task present in both releases, compute `Δ usage_pct` and `Δ ai_autonomy_mean`. Call out tasks moving fastest toward automation.
    - **Usage-per-capita z-scores** by country — flag `|z| > 2` for sanity check (are outliers real or sampling artifacts?).
    - **Collaboration clustering** — for top-50 tasks, hierarchically cluster on the 4-vector `(directive_pct, feedback_pct, validation_pct, learning_pct)`. Call out 2–3 cluster archetypes with example tasks.
    Every finding must survive a spot check (Phase 10) before going into a chart or insight blockquote. Findings that don't replicate in raw data must be cut.

### Apples-to-apples rules (do not violate)

1. **Country code vintage.** AEI raw files use ISO 3166-1 alpha-2 (`US`); the bundled `iso_country_codes.csv` and World Bank use alpha-3 (`USA`). Always normalize to alpha-3 in staging before joining. Never compare `US` to `USA` directly.
2. **SOC version alignment.** O*NET uses O*NET-SOC (extended, e.g. `15-1252.00`); BLS OES uses 6-digit BLS SOC. Truncate O*NET-SOC to the 6-digit root (`15-1252`) before joining. Document the SOC version used (SOC 2018) and note any occupations where the 6-to-8-digit mapping is ambiguous.
3. **Time-window parity.** AEI is a **1-week snapshot** per release (e.g. 2025-11-13 → 2025-11-20 for the 2026-01-15 release). Do **not** compare it to BLS annual 2024 averages as if they were the same granularity; always caveat that wage is annual-mean while usage is a weekly snapshot. Never compare the 2025-09-15 release (Sonnet 4) to the 2026-01-15 release (Sonnet 4.5) without annotating the model-version change.
4. **Geographic-granularity parity.** AEI has `country` and `country-state` facets. Do not mix a US state with a European country in the same chart. World Bank is country-level only — do not impute state-level GDP.
5. **Metric-definition parity.** `onet_task_pct` is share-of-conversations-within-geography (denominator = that country's total). A low-usage country cannot be "ranked lowest on AI adoption" using `onet_task_pct` — it simply has less volume. Always use `onet_task_pct_index` (specialization ratio vs global baseline) for cross-geography comparison.
6. **Minimum-sample threshold.** AEI documentation requires ≥200 conversations per country and ≥100 per country-state. Filter out entities below those thresholds before any cross-geography comparison. Annotate sample size on charts where n < 500.
7. **Consumer vs enterprise scoping.** 1P API data is GLOBAL-only. Do not join 1P API metrics to country-level Claude.ai metrics as if they were parallel observations. Consumer-vs-enterprise comparisons happen at the global/task level only.
8. **Release-over-release comparability.** Only compare tasks appearing in **both** snapshots (inner join on `cluster_name` at the task level). Report the drop-out count — tasks that exist only in one release cannot be compared.

### Custom quality checks (required per staging asset)

Write these as `custom_checks:` in the asset YAML. Each must return 0 rows on a healthy run.

- `staging.aei_task_exposure`
  ```sql
  SELECT COUNT(*) FROM `bruin-playground-arsalan.staging.aei_task_exposure`
  WHERE ai_autonomy_mean IS NOT NULL
    AND ai_autonomy_mean NOT BETWEEN 0 AND 1
  ```
- `staging.aei_geographic_adoption`
  ```sql
  SELECT COUNT(*) FROM `bruin-playground-arsalan.staging.aei_geographic_adoption`
  WHERE usage_count < 200 AND geography = 'country'
  ```
- `staging.aei_collaboration_patterns`
  ```sql
  SELECT task_id FROM `bruin-playground-arsalan.staging.aei_collaboration_patterns`
  GROUP BY task_id
  HAVING ABS(SUM(share_pct) - 100) > 1
  ```
- `staging.aei_consumer_vs_api`
  ```sql
  SELECT COUNT(*) FROM `bruin-playground-arsalan.staging.aei_consumer_vs_api`
  WHERE ABS(consumer_pct - api_pct) > 100
  ```

Raw assets get the standard checks: `not_null` + `unique` on composite PK, `not_null` on `extracted_at`, `accepted_values` on known categoricals (`geography`, `facet`, `release_id`).

### Constraints

- **Primary release is 2026-01-15.** The 2025-09-15 release is ingested only for release-over-release comparison of overlapping tasks. Do not attempt to unify all six AEI releases — earlier schemas differ and the model-version delta is non-trivial.
- **Use `append` strategy for World Bank, `create+replace` for everything else.** Deduplicate World Bank in staging using `QUALIFY ROW_NUMBER() OVER (PARTITION BY country_code, indicator_code, year ORDER BY extracted_at DESC) = 1` per `AGENTS.md`.
- **BLS wages are US-only.** Any chart using wage data is inherently a US view. Caveat explicitly; do not impute to other countries.
- **1P API data is GLOBAL-only.** Never break 1P API down by country in any chart.
- **AEI `value` column contains many different metric types** (counts, percentages, means, CI bounds, histograms). The staging pivot must be selective — pick only the variables needed for the 4 charts; do not attempt to pivot every variable.
- **Do not write documentation files** beyond `README.md` and this `prompt.md`/`plan.md` pair. No extra `.md` files in the pipeline folder.
