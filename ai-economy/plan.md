# ai-economy — Technical Plan

Detailed design for the `ai-economy` pipeline. Pair with `prompt.md` in this folder (the prompt is the build instructions; this is the design document).

---

## Context & motivation

The Anthropic Economic Index (AEI) is the closest public dataset to a ground-truth measurement of how AI is being used in real economic work. Each release contains anonymized, aggregated telemetry keyed by geography, O*NET occupational task, and dozens of other facets (collaboration pattern, request complexity, AI autonomy, required education years, task completion time with and without AI). The hypothesis this pipeline tests:

> **Claude augments high-wage cognitive work and automates low-wage task categories, and the split shows up as a measurable correlation between `ai_autonomy_mean` and BLS median annual wage.**

A secondary hypothesis: **consumer Claude.ai and 1P API usage diverge sharply by task — developers and enterprises use the API for very different things than individuals use Claude.ai for**. We test this by joining the two datasets and measuring the percentage-point delta at the task level.

The analysis is cross-domain: AEI tells us what AI does, but it doesn't tell us which jobs those tasks map to, how much those jobs pay, or how developed the economy is where the usage happens. Enrichment via O*NET (task → occupation), BLS OES (occupation → wage & employment), and World Bank (country → GDP & labor force) is what turns AEI from descriptive telemetry into an analytical instrument.

---

## Data inventory

### Primary — Anthropic Economic Index (HuggingFace `Anthropic/EconomicIndex`)

| File | Path | Rows (est.) | Size | Schema |
|---|---|---|---|---|
| Claude.ai 2026-01-15 release | `release_2026_01_15/data/intermediate/aei_raw_claude_ai_2025-11-13_to_2025-11-20.csv` | ~5M | ~100 MB | Long format keyed by `geo_id, geography, date_start, date_end, facet, level, variable, cluster_name` → `value` |
| 1P API 2026-01-15 release | `release_2026_01_15/data/intermediate/aei_raw_1p_api_2025-11-13_to_2025-11-20.csv` | ~500K | ~15 MB | Same long schema; `geo_id='GLOBAL'` only; adds `onet_task::cost`, `::prompt_tokens`, `::completion_tokens` indexed facets |
| ISO country code map | `release_2026_01_15/data/intermediate/iso_country_codes.csv` | ~250 | <1 MB | `iso_alpha_2, iso_alpha_3, country_name` |
| Claude.ai 2025-09-15 release | `release_2025_09_15/data/intermediate/aei_raw_claude_ai_*.csv` | ~4M | ~80 MB | Same schema as 2026-01 |

Base download URL pattern: `https://huggingface.co/datasets/Anthropic/EconomicIndex/resolve/main/<release>/<path>`. No auth required. License: CC BY 4.0.

### Enrichment sources

| Source | URL | Rows | Format | License | Auth |
|---|---|---|---|---|---|
| O*NET Task Statements v29.x | `https://www.onetcenter.org/dl_files/database/db_29_2_text/Task%20Statements.txt` | ~19,000 tasks | Tab-separated | CC BY 4.0 | None |
| O*NET Occupation Data v29.x | `https://www.onetcenter.org/dl_files/database/db_29_2_text/Occupation%20Data.txt` | ~900 occupations | Tab-separated | CC BY 4.0 | None |
| BLS OES May 2024 National | `https://www.bls.gov/oes/special-requests/oesm24nat.zip` (contains `national_M2024_dl.xlsx`) | ~820 detailed occupations | XLSX inside ZIP | US public domain | None |
| World Bank Open Data | `https://api.worldbank.org/v2/country/all/indicator/<code>?format=json&per_page=20000&date=2020:2024` | ~4K rows per indicator | JSON | CC BY 4.0 | None |

World Bank indicators: `NY.GDP.PCAP.CD`, `SL.TLF.CACT.ZS`, `SP.POP.TOTL`.

---

## Schema mental model

### AEI long-format schema (both Claude.ai and 1P API)

```
geo_id       geography         date_start    date_end      facet        level  variable                       cluster_name                     value
-----------  ---------------   -----------   -----------   ----------   -----  ----------------------------   ------------------------------   -------
USA          country           2025-11-13    2025-11-20    onet_task    2      onet_task_pct                  15-1252.00:Write code            4.82
GLOBAL       global            2025-11-13    2025-11-20    onet_task    2      ai_autonomy_mean               15-1252.00:Write code            0.41
USA          country           2025-11-13    2025-11-20    collaboration 0     onet_task_collaboration_pct    directive::15-1252.00:Write code 38.5
GLOBAL       global            2025-11-13    2025-11-20    onet_task    2      onet_task::cost_index          15-1252.00:Write code            1.24
```

Each row is a single `(geography, task/pattern, variable)` observation. `cluster_name` is the specific entity (task ID, collaboration type, or `base::category` for intersection metrics).

### Staging pivot target (wide)

`staging.aei_task_exposure` — one row per `task_id`:

```
task_id            task_description                 onet_soc_code   occupation_title     usage_count_global  usage_pct_global  ai_autonomy_mean  human_only_time_mean  human_education_years_mean  collab_directive_pct  collab_feedback_pct  collab_validation_pct  collab_learning_pct  bls_total_employment  bls_median_annual_wage
15-1252.00:Write…  Write, analyze, review, rewrite  15-1252.00      Software Developers  41,083              4.82              0.41              48.2                  16.8                        38.5                   31.1                  18.7                    11.7                  1,853,800             132,270
```

The pivot selects ~10 variables from a universe of ~50+. Variables not selected are left in raw; staging should not attempt a complete pivot.

---

## Asset-by-asset design

### Raw layer

#### `raw.aei_claude_usage`
- **Type:** `python`, `image: python:3.11`, `materialization: create+replace`
- **Source:** `https://huggingface.co/datasets/Anthropic/EconomicIndex/resolve/main/release_2026_01_15/data/intermediate/aei_raw_claude_ai_2025-11-13_to_2025-11-20.csv`
- **Strategy:** Stream download with `requests.get(url, stream=True)` and chunked `iter_content(65536)` to a temp file; read with `pd.read_csv`. Cast types explicitly. Add `release_id = 'release_2026_01_15'` and `extracted_at = datetime.now(timezone.utc)`.
- **Env vars for testing:** `AEI_MAX_ROWS` (default unset = all rows).
- **Primary key:** composite `(geo_id, date_start, facet, variable, cluster_name)`.
- **Columns:** `geo_id VARCHAR`, `geography VARCHAR`, `date_start DATE`, `date_end DATE`, `platform_and_product VARCHAR`, `facet VARCHAR`, `level INTEGER`, `variable VARCHAR`, `cluster_name VARCHAR`, `value DOUBLE`, `release_id VARCHAR`, `extracted_at TIMESTAMP`.
- **Quality checks:** `not_null` on all PK columns + `extracted_at`; `unique` on composite PK; `accepted_values` on `geography IN ('country', 'country-state', 'global')`; `accepted_values` on `platform_and_product = 'Claude AI (Free and Pro)'`.

#### `raw.aei_api_usage`
- Same shape as `raw.aei_claude_usage` but sources the `aei_raw_1p_api_*.csv` file from the same release.
- **Additional `accepted_values` check:** `geo_id = 'GLOBAL'`, `geography = 'global'`.

#### `raw.aei_prior_snapshot`
- Same shape as `raw.aei_claude_usage` but sources `release_2025_09_15/data/intermediate/aei_raw_claude_ai_*.csv`. `release_id = 'release_2025_09_15'`.
- Used only for release-over-release comparison downstream.

#### `raw.aei_onet_tasks`
- **Source:** Download two tab-separated O*NET files (Task Statements + Occupation Data). Join on `O*NET-SOC Code` in Python.
- **Strategy:** `create+replace`, single-shot ingestion.
- **Primary key:** `task_id` (unique in Task Statements).
- **Columns:** `task_id VARCHAR`, `onet_soc_code VARCHAR`, `task_description VARCHAR`, `task_type VARCHAR` (Core/Supplemental), `occupation_title VARCHAR`, `occupation_description VARCHAR`, `extracted_at TIMESTAMP`.
- **Quality checks:** `not_null` + `unique` on `task_id`; `accepted_values` on `task_type IN ('Core', 'Supplemental')`.

#### `raw.aei_bls_wages`
- **Source:** Download `oesm24nat.zip`, unzip in-memory, read `national_M2024_dl.xlsx` with `pd.read_excel(engine='openpyxl')`.
- **Filter:** `OCC_GROUP = 'detailed'` (excludes aggregate rows like major/minor groups).
- **Primary key:** `soc_code` (6-digit).
- **Columns:** `soc_code VARCHAR`, `occupation_title VARCHAR`, `total_employment INTEGER`, `median_annual_wage DOUBLE`, `mean_annual_wage DOUBLE`, `extracted_at TIMESTAMP`.
- **Quality checks:** `not_null` + `unique` on `soc_code`; `min: 0` on `total_employment` and both wages.
- **Known caveat:** BLS uses asterisks (`*`) to indicate suppressed values in the XLSX. Parse these as null.

#### `raw.aei_worldbank_context`
- **Source:** World Bank API, 3 indicators × 2020–2024.
- **Strategy:** `materialization: append` (per `AGENTS.md` and `baby-bust/`). Deduplicate in staging.
- **Pattern:** Reuse the chunked/retried fetch pattern from `baby-bust/assets/raw/worldbank_indicators.py` — 10-year chunks, `per_page=20000`, 5 retries with exponential backoff (10s network, 30s rate-limit, 15s server error).
- **Primary key:** `(country_code, indicator_code, year)`.
- **Columns:** `country_code VARCHAR`, `country_name VARCHAR`, `indicator_code VARCHAR`, `indicator_name VARCHAR`, `year INTEGER`, `value DOUBLE`, `extracted_at TIMESTAMP`.
- **Quality checks:** `not_null` on PK + `extracted_at`; `min: 2020`, `max: 2026` on `year`.

### Staging layer

All staging assets use `materialization: create+replace` and start with a dedup CTE over upstream raw tables (per `AGENTS.md`).

#### `staging.aei_task_exposure`

**Depends:** `raw.aei_claude_usage`, `raw.aei_api_usage`, `raw.aei_onet_tasks`, `raw.aei_bls_wages`.

**Logic:**

```sql
WITH deduped AS (
  SELECT *
  FROM raw.aei_claude_usage
  WHERE cluster_name IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY geo_id, date_start, facet, variable, cluster_name
    ORDER BY extracted_at DESC
  ) = 1
),

global_facet AS (
  SELECT cluster_name AS task_id, variable, value
  FROM deduped
  WHERE geo_id = 'GLOBAL' AND facet = 'onet_task'
),

pivoted AS (
  SELECT
    task_id,
    MAX(CASE WHEN variable = 'usage_count' THEN value END) AS usage_count_global,
    MAX(CASE WHEN variable = 'onet_task_pct' THEN value END) AS usage_pct_global,
    MAX(CASE WHEN variable = 'ai_autonomy_mean' THEN value END) AS ai_autonomy_mean,
    MAX(CASE WHEN variable = 'human_only_time_mean' THEN value END) AS human_only_time_mean,
    MAX(CASE WHEN variable = 'human_with_ai_time_mean' THEN value END) AS human_with_ai_time_mean,
    MAX(CASE WHEN variable = 'human_education_years_mean' THEN value END) AS human_education_years_mean
  FROM global_facet
  GROUP BY task_id
),

collab AS (
  -- cluster_name is "<collab_type>::<task_id>" for intersection facet
  SELECT
    SPLIT(cluster_name, '::')[OFFSET(1)] AS task_id,
    SPLIT(cluster_name, '::')[OFFSET(0)] AS collaboration_type,
    value AS share_pct
  FROM deduped
  WHERE geo_id = 'GLOBAL'
    AND facet = 'collaboration'
    AND variable = 'onet_task_collaboration_pct'
),

collab_pivoted AS (
  SELECT
    task_id,
    MAX(CASE WHEN collaboration_type = 'directive' THEN share_pct END) AS collab_directive_pct,
    MAX(CASE WHEN collaboration_type = 'feedback' THEN share_pct END) AS collab_feedback_pct,
    MAX(CASE WHEN collaboration_type = 'validation' THEN share_pct END) AS collab_validation_pct,
    MAX(CASE WHEN collaboration_type = 'learning' THEN share_pct END) AS collab_learning_pct
  FROM collab
  GROUP BY task_id
)

SELECT
  p.task_id,
  o.task_description,
  o.onet_soc_code,
  -- SOC alignment: truncate O*NET-SOC "15-1252.00" to BLS SOC "15-1252"
  REGEXP_EXTRACT(o.onet_soc_code, r'^\d{2}-\d{4}') AS soc_code_6digit,
  o.occupation_title,
  p.usage_count_global,
  p.usage_pct_global,
  p.ai_autonomy_mean,
  p.human_only_time_mean,
  p.human_with_ai_time_mean,
  p.human_education_years_mean,
  c.collab_directive_pct,
  c.collab_feedback_pct,
  c.collab_validation_pct,
  c.collab_learning_pct,
  b.total_employment AS bls_total_employment,
  b.median_annual_wage AS bls_median_annual_wage,
  -- Augmentation vs automation flag (derived)
  CASE
    WHEN p.ai_autonomy_mean IS NULL THEN NULL
    WHEN p.ai_autonomy_mean < 0.33 THEN 'Augmentation'
    WHEN p.ai_autonomy_mean < 0.67 THEN 'Hybrid'
    ELSE 'Automation'
  END AS exposure_pattern
FROM pivoted p
LEFT JOIN raw.aei_onet_tasks o USING (task_id)
LEFT JOIN raw.aei_bls_wages b ON b.soc_code = REGEXP_EXTRACT(o.onet_soc_code, r'^\d{2}-\d{4}')
LEFT JOIN collab_pivoted c USING (task_id)
ORDER BY p.usage_count_global DESC
```

**Custom check:** `ai_autonomy_mean NOT BETWEEN 0 AND 1` — expect 0 rows.

#### `staging.aei_geographic_adoption`

**Depends:** `raw.aei_claude_usage`, `raw.aei_prior_snapshot`, `raw.aei_worldbank_context`.

**Logic:** Dedupe both snapshots; pivot country-level metrics (`usage_count`, distinct task count, top-task specialization index per country); join to latest-available World Bank values per country (`NY.GDP.PCAP.CD`, `SL.TLF.CACT.ZS`, `SP.POP.TOTL`) using the max-year available per country; compute `usage_per_million = usage_count / (population / 1e6)`; include both snapshots to enable release-over-release comparison downstream.

Output one row per `(country_iso3, release_id)`.

**Columns:** `country_iso3 VARCHAR PK`, `release_id VARCHAR PK`, `country_name VARCHAR`, `usage_count INTEGER`, `distinct_tasks_observed INTEGER`, `top_task_id VARCHAR`, `top_task_specialization_index DOUBLE`, `gdp_per_capita DOUBLE`, `labor_force_participation_pct DOUBLE`, `population BIGINT`, `usage_per_million DOUBLE`.

**Custom check:** `usage_count < 200 AND geography = 'country'` — expect 0 rows (minimum-sample filter).

#### `staging.aei_collaboration_patterns`

**Depends:** `raw.aei_claude_usage`, `raw.aei_onet_tasks`.

**Logic:** Unpack the `collaboration` facet intersection (`cluster_name` = `<collab_type>::<task_id>`) from GLOBAL rows; join task metadata. Output one row per `(task_id, collaboration_type)` with `share_pct`.

**Columns:** `task_id VARCHAR PK`, `collaboration_type VARCHAR PK`, `share_pct DOUBLE`, `task_description VARCHAR`, `occupation_title VARCHAR`, `task_usage_count_global INTEGER` (for filtering to top-20 tasks in chart).

**Custom check:** `SELECT task_id GROUP BY task_id HAVING ABS(SUM(share_pct) - 100) > 1` — expect 0 rows (shares sum to 100 within epsilon).

#### `staging.aei_consumer_vs_api`

**Depends:** `raw.aei_claude_usage`, `raw.aei_api_usage`, `raw.aei_onet_tasks`.

**Logic:** For each task, extract `onet_task_pct` from Claude.ai GLOBAL (= `consumer_pct`) and from 1P API GLOBAL (= `api_pct`); also extract `cost_index`, `prompt_tokens_index`, `completion_tokens_index` from API intersection facets. Inner-join on `task_id` (drops tasks that appear in only one channel).

**Columns:** `task_id VARCHAR PK`, `task_description VARCHAR`, `occupation_title VARCHAR`, `consumer_pct DOUBLE`, `api_pct DOUBLE`, `delta_pp DOUBLE` (= `api_pct - consumer_pct`), `api_cost_index DOUBLE`, `api_prompt_tokens_index DOUBLE`, `api_completion_tokens_index DOUBLE`.

**Custom check:** `ABS(consumer_pct - api_pct) > 100` — expect 0 rows.

---

## Dashboard narrative arc

Four charts, each answering one question, each followed by a quantified blockquote (`st.markdown("> ...")`). Wong 2011 palette throughout. Global Altair theme registered for consistent typography (cf. `baby-bust/assets/reports/streamlit_app.py`).

### Chart 1 — "Where Claude Works: Augmentation vs Automation by Task"
- **Question:** Do high-wage tasks get augmented and low-wage tasks get automated?
- **Data:** `staging.aei_task_exposure` filtered to tasks with BLS wage match (expect ~400 of ~19K tasks).
- **Encoding:** Altair scatter. X = `ai_autonomy_mean` (0–1, linear). Y = `bls_median_annual_wage` (log scale). Size = `bls_total_employment` (sqrt scale). Color = `exposure_pattern` (Augmentation / Hybrid / Automation) using Wong palette. Text labels on top-15 tasks by `usage_count_global`.
- **Reference lines:** vertical rules at `ai_autonomy_mean = 0.33` and `0.67` (Augmentation/Hybrid/Automation boundaries).
- **Tooltip:** task_description, occupation_title, ai_autonomy_mean (`.2f`), bls_median_annual_wage (`$,.0f`), bls_total_employment (`,.0f`).
- **Hypothesis blockquote:** "Pearson correlation between `ai_autonomy_mean` and `log10(median_wage)`: **r = {corr:+.2f}** across {n} tasks. {Interpretation}."

### Chart 2 — "Where Claude Is Used: Geographic Specialization"
- **Question:** Which countries use Claude most per capita, and what is each one's signature task?
- **Data:** `staging.aei_geographic_adoption` filtered to `release_id = 'release_2026_01_15'`, `usage_count >= 200`, `population IS NOT NULL`.
- **Encoding:** Pydeck ScatterplotLayer on a dark CARTO basemap (cf. `city-pulse`). Point size = `sqrt(usage_per_million) * 15`. Color = diverging blue→orange on `log10(gdp_per_capita)`. Tooltip shows `country_name`, `usage_per_million`, `top_task_id`, `top_task_specialization_index`, `gdp_per_capita`.
- **Companion table (below map):** top-20 countries by `usage_per_million` with their top specialization task.
- **Hypothesis blockquote:** "Correlation between `log(gdp_per_capita)` and `log(usage_per_million)`: **r = {corr:+.2f}** across {n} countries. Top specializer: **{country}** on **{task}** (index = {idx:.1f})."

### Chart 3 — "Consumer vs Enterprise: Where Developers Diverge"
- **Question:** Which tasks show the biggest gap between Claude.ai (consumer) and 1P API (developer) usage share?
- **Data:** `staging.aei_consumer_vs_api`, top-20 tasks by `|delta_pp|`.
- **Encoding:** Altair dumbbell. Y = task label (sorted by `delta_pp`). X-axis position pair = `consumer_pct` (blue circle) and `api_pct` (orange diamond). Connecting line in muted grey. Size of API marker = `api_cost_index`.
- **Hypothesis blockquote:** "{N} of the top-20 divergent tasks are skewed toward enterprise. Mean API cost-index for API-skewed tasks: **{x:.2f}**, vs **{y:.2f}** for consumer-skewed tasks. {Interpretation}."

### Chart 4 — "Collaboration DNA"
- **Question:** Among the most-used tasks, which are delegated vs iteratively worked?
- **Data:** `staging.aei_collaboration_patterns` filtered to top-20 tasks by `task_usage_count_global`.
- **Encoding:** Altair horizontal stacked bar. Y = task label. X = `share_pct` stacked by `collaboration_type` (directive / feedback / validation / learning). Color by `collaboration_type` using 4 Wong palette colors.
- **Interactive legend filter:** `alt.selection_point(fields=['collaboration_type'], bind='legend')`.
- **Hypothesis blockquote:** "**{top_directive_task}** is the most delegated (directive = {x:.0f}%). **{top_feedback_task}** shows the highest iterative feedback pattern ({y:.0f}%). Median directive share across top-20 tasks: **{z:.0f}%**."

---

## Workflow (12 phases — verbatim from `prompt.md`)

1. **Initial preparations** — directory, `pipeline.yml`, README skeleton, per-layer `requirements.txt`, `.streamlit/secrets.toml` (gitignored), minimal Bruin headers so `bruin validate` passes.
2. **Raw ingestion** — all six raw assets. Test each individually with small scope (`AEI_MAX_ROWS=5000`, single-year World Bank window, full BLS file which is already small).
3. **Staging transformations** — four staging SQLs. Parity query per staging asset: join back to raw on natural key, assert zero diff on pivoted columns.
4. **Reports layer** — `streamlit_app.py` + per-chart SQL files. Iterate chart by chart; verify each renders with real data.
5. **First full pipeline run end-to-end** — `bruin validate ai-economy/` then `bruin run ai-economy/`. Confirm no failures.
6. **Data analysis + metadata enrichment** — table/column descriptions, tags, `checks:` (not_null, unique, accepted_values, min/max), `custom_checks:` per staging asset.
7. **Common-sense (apples-to-apples) validation** — walk every chart against the 8 hazards. Add a Methodology section to README.
8. **`bruin ai enhance`** per asset, one at a time, with `bruin validate` immediately after each. Rewrite YAML columns manually if enhance corrupts them.
9. **Second full pipeline run end-to-end** — confirm all enhance-added checks pass.
10. **Spot-check validation** — 3 data points per chart, traced raw → staging → chart, all must agree. Fix mismatches at the pipeline level, re-run, repeat until zero mismatches.
11. **Final pipeline run** — capture `MAX(extracted_at)` per raw table in README as the "as-of" date.
12. **Deep insight analysis** — Pearson/Spearman correlations, consumer-vs-enterprise divergence top-10, country specialization z-scores, release-over-release task shifts, usage-per-capita outliers, collaboration clustering. Findings must survive spot-check.

---

## Apples-to-apples hazards (verbatim from `prompt.md`)

1. **Country code vintage** — AEI uses alpha-2; enrichment uses alpha-3. Normalize to alpha-3 in staging.
2. **SOC version alignment** — O*NET-SOC (8-digit) vs BLS SOC (6-digit). Truncate to 6 digits before joining.
3. **Time-window parity** — AEI is a 1-week snapshot per release; BLS wages are annual. Caveat every cross-source chart. Never silently compare 2025-09-15 (Sonnet 4) to 2026-01-15 (Sonnet 4.5).
4. **Geographic-granularity parity** — country vs country-state. Don't mix in one chart. World Bank is country-only.
5. **Metric-definition parity** — use `onet_task_pct_index` (specialization ratio) for cross-geography; never raw `onet_task_pct`.
6. **Minimum-sample threshold** — ≥200 conversations per country, ≥100 per state. Filter below threshold. Annotate n < 500.
7. **Consumer vs enterprise scoping** — 1P API is GLOBAL-only. Do not join 1P API to country-level metrics.
8. **Release-over-release comparability** — inner join on `cluster_name` across releases; report drop-out count.

---

## Custom quality checks (per asset YAML)

### Raw

- All raw assets: `not_null` + `unique` on composite PK; `not_null` on `extracted_at`.
- `raw.aei_claude_usage`: `accepted_values` on `geography IN ('country','country-state','global')`; `accepted_values` on `platform_and_product = 'Claude AI (Free and Pro)'`.
- `raw.aei_api_usage`: `accepted_values` on `geography = 'global'`, `geo_id = 'GLOBAL'`.
- `raw.aei_onet_tasks`: `accepted_values` on `task_type IN ('Core', 'Supplemental')`.
- `raw.aei_bls_wages`: `min: 0` on `total_employment`, `median_annual_wage`, `mean_annual_wage`.
- `raw.aei_worldbank_context`: `min: 2020`, `max: 2026` on `year`; `min: 0` on `value` (for the 3 indicators used).

### Staging (custom SQL)

```sql
-- staging.aei_task_exposure
SELECT COUNT(*) FROM `bruin-playground-arsalan.staging.aei_task_exposure`
WHERE ai_autonomy_mean IS NOT NULL
  AND ai_autonomy_mean NOT BETWEEN 0 AND 1;
-- Expected: 0

-- staging.aei_geographic_adoption
SELECT COUNT(*) FROM `bruin-playground-arsalan.staging.aei_geographic_adoption`
WHERE usage_count < 200
  AND country_iso3 NOT IN ('GLOBAL');
-- Expected: 0 (AEI publication threshold)

-- staging.aei_collaboration_patterns
SELECT task_id FROM `bruin-playground-arsalan.staging.aei_collaboration_patterns`
GROUP BY task_id
HAVING ABS(SUM(share_pct) - 100) > 1;
-- Expected: 0 rows

-- staging.aei_consumer_vs_api
SELECT COUNT(*) FROM `bruin-playground-arsalan.staging.aei_consumer_vs_api`
WHERE ABS(consumer_pct - api_pct) > 100
   OR consumer_pct < 0 OR consumer_pct > 100
   OR api_pct < 0 OR api_pct > 100;
-- Expected: 0
```

---

## Known limitations

- **1-week snapshot per release.** AEI is not a continuous time series. Each release is a single 7-day window of telemetry. Comparisons across releases conflate time, model version, and sampling choices.
- **1P API is GLOBAL-only.** No country breakdown for enterprise usage. Any "which countries use AI for what" chart is consumer-only.
- **BLS wages are US-only.** The augmentation-vs-automation chart (Chart 1) is inherently a US view. International wage data at O*NET granularity doesn't exist.
- **Country-state coverage is uneven.** ISO 3166-2 regions require ≥100 conversations each, leaving most subnational regions outside small handful of countries unobserved.
- **Model version changes between releases.** 2025-09-15 used Sonnet 4; 2026-01-15 used Sonnet 4.5. Behavior changes between models confound release-over-release task-share deltas.
- **AEI's task labels come from O*NET-SOC but occasionally use custom cluster formats** (`15-1252.00:Write code…`). Robust parsing of `cluster_name` is required; assume the SOC portion precedes the first `:`.
- **Specialization index denominator.** `onet_task_pct_index` compares a geography's task share to the global baseline. A tiny country with a single unusual user will show extreme indices. Filter by minimum sample.
- **BLS asterisk suppression.** BLS redacts small employment/wage cells with `*`. These must be read as null, not as 0 or literal string.
- **No enterprise cost data at the country level.** API cost indexes only exist globally; no way to localize enterprise spend.

---

## Verification (Phase 12 specifics)

Concrete queries to run during the deep analysis phase. Each returns a single scalar or small table; each finding must survive a spot-check before being quoted in the dashboard.

### Correlation: AI autonomy vs wage (Chart 1 hypothesis)

```sql
SELECT
  CORR(ai_autonomy_mean, LOG10(bls_median_annual_wage)) AS pearson_r,
  COUNT(*) AS n
FROM `bruin-playground-arsalan.staging.aei_task_exposure`
WHERE ai_autonomy_mean IS NOT NULL
  AND bls_median_annual_wage IS NOT NULL;
```

If `|r| > 0.1` and n > 200, the correlation is worth reporting. Always quote n alongside r.

### Top consumer–enterprise divergence (Chart 3 hypothesis)

```sql
SELECT
  task_id,
  task_description,
  consumer_pct,
  api_pct,
  delta_pp,
  api_cost_index
FROM `bruin-playground-arsalan.staging.aei_consumer_vs_api`
ORDER BY ABS(delta_pp) DESC
LIMIT 20;
```

### Country specialization outliers (Chart 2 hypothesis)

```sql
SELECT
  country_iso3,
  country_name,
  top_task_id,
  top_task_specialization_index,
  usage_per_million,
  gdp_per_capita
FROM `bruin-playground-arsalan.staging.aei_geographic_adoption`
WHERE release_id = 'release_2026_01_15'
  AND top_task_specialization_index > 2
ORDER BY top_task_specialization_index DESC;
```

### Release-over-release task shifts

```sql
WITH latest AS (
  SELECT task_id, usage_pct_global, ai_autonomy_mean
  FROM `bruin-playground-arsalan.staging.aei_task_exposure`
),
prior AS (
  -- equivalent pivot from raw.aei_prior_snapshot, not created as a staging table here
  SELECT
    cluster_name AS task_id,
    MAX(CASE WHEN variable = 'onet_task_pct' THEN value END) AS usage_pct_prior,
    MAX(CASE WHEN variable = 'ai_autonomy_mean' THEN value END) AS ai_autonomy_prior
  FROM `bruin-playground-arsalan.raw.aei_prior_snapshot`
  WHERE geo_id = 'GLOBAL' AND facet = 'onet_task'
  GROUP BY task_id
)
SELECT
  l.task_id,
  l.usage_pct_global - p.usage_pct_prior AS delta_usage_pp,
  l.ai_autonomy_mean - p.ai_autonomy_prior AS delta_autonomy
FROM latest l
INNER JOIN prior p USING (task_id)
ORDER BY delta_autonomy DESC
LIMIT 10;
```

Report drop-out count (tasks in one release but not the other) alongside.

### Collaboration clustering

Export `staging.aei_collaboration_patterns` pivoted to one row per task with 4 columns; run `scipy.cluster.hierarchy.linkage` (ward method) on euclidean distance; identify 3 clusters; call out 2–3 representative tasks per cluster.
