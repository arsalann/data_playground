# ai-economy — Anthropic Economic Index

Pipeline analyzing the Anthropic Economic Index (AEI), which measures how Claude is used in real-world economic work, joined against BLS wages, O*NET occupational tasks, and World Bank country context.

## Hypothesis

Claude **augments high-wage cognitive work and automates low-wage task categories**. If true, the split should appear as a negative correlation between `ai_autonomy_mean` (AEI) and `log10(median_annual_wage)` (BLS), across the subset of O*NET tasks we can match across both sources.

A secondary hypothesis: **consumer Claude.ai usage diverges from 1P API usage at the task level** — developers and enterprises use the API for very different things than individuals use Claude.ai for.

## Data sources

| Source | URL | License | Auth |
|---|---|---|---|
| Anthropic Economic Index (AEI) | https://huggingface.co/datasets/Anthropic/EconomicIndex | CC BY 4.0 | None |
| O*NET Task Statements + Occupation Data v29.2 | https://www.onetcenter.org/database/db_29_2_text.html | CC BY 4.0 | None |
| BLS OES May 2024 National | https://www.bls.gov/oes/special-requests/oesm24nat.zip | US public domain | None |
| World Bank Open Data | https://api.worldbank.org/v2/ | CC BY 4.0 | None |

### AEI files used

| File | Path | Release |
|---|---|---|
| Claude.ai usage (current) | `release_2026_01_15/data/intermediate/aei_raw_claude_ai_2025-11-13_to_2025-11-20.csv` | 2026-01-15 |
| 1P API usage (current) | `release_2026_01_15/data/intermediate/aei_raw_1p_api_2025-11-13_to_2025-11-20.csv` | 2026-01-15 |
| ISO country code map | `release_2026_01_15/data/intermediate/iso_country_codes.csv` | 2026-01-15 |
| Claude.ai prior snapshot | `release_2025_09_15/data/intermediate/aei_raw_claude_ai_*.csv` | 2025-09-15 |

## Assets

### Raw

| Asset | Source | Strategy |
|---|---|---|
| `raw.aei_claude_usage` | AEI Claude.ai 2026-01-15 CSV | `create+replace` |
| `raw.aei_api_usage` | AEI 1P API 2026-01-15 CSV (GLOBAL only) | `create+replace` |
| `raw.aei_prior_snapshot` | AEI Claude.ai 2025-09-15 CSV | `create+replace` |
| `raw.aei_onet_tasks` | O*NET Task Statements + Occupation Data | `create+replace` |
| `raw.aei_bls_wages` | BLS OES May 2024 national detailed occupations | `create+replace` |
| `raw.aei_worldbank_context` | World Bank GDP/labor/population 2020-2024 | `append` |

### Staging

| Asset | Grain | Joins |
|---|---|---|
| `staging.aei_task_exposure` | One row per O*NET task | AEI GLOBAL × O*NET × BLS |
| `staging.aei_geographic_adoption` | One row per country × release | AEI country × World Bank |
| `staging.aei_collaboration_patterns` | One row per (task, collaboration_type) | AEI GLOBAL × O*NET |
| `staging.aei_consumer_vs_api` | One row per task | AEI Claude.ai × 1P API × O*NET |

### Reports

Streamlit dashboard with 4 charts aligned to the four dashboard questions in `prompt.md`.

## Run commands

```bash
bruin validate ai-economy/
bruin run ai-economy/assets/raw/aei_claude_usage.py            # test with AEI_MAX_ROWS=5000 first
bruin run ai-economy/                                          # full pipeline
python3 -m streamlit run ai-economy/assets/reports/streamlit_app.py
```

## Methodology

(Populated during Phase 7.)

## Validation

(Populated during Phase 10.)

## Known limitations

- AEI is a **1-week snapshot** per release, not a continuous time series. Cross-release comparisons conflate time, model version, and sampling.
- 1P API data is **GLOBAL-only** — no country breakdown exists for enterprise usage.
- BLS wages are **US-only**. The augmentation-vs-automation chart is a US view.
- O*NET-SOC and BLS SOC differ in digit count — staging truncates O*NET-SOC to 6 digits.
- `onet_task_pct` is a within-geography share. Cross-country comparisons must use the specialization index, not raw share.
- Minimum publication threshold: ≥200 conversations per country, ≥100 per state.
