# Data Quality: semantic dashboard

Migrates [Bruin Cloud dashboard 1135](https://cloud.getbruin.com/bruincommunitydemo/dashboards/1135)
from eight handwritten SQL queries to 23 direct semantic widgets. The five tabs,
widget IDs, inclusive `start_date` / `end_date` filters, source comparisons,
daily trends and top-50 gap tables are preserved. No dashboard `sql`, `query`,
`queries`, `file` or static `data` sources remain.

## Data and definitions

This is **DUMMY DATA**, reproduced from the existing dashboard, not a connection
to Firebase, Adjust or AppLovin. There is no additional external dataset or
external source license. The fixture covers September 6–October 5, 2026: 1,000
synthetic users and one unattributed AppLovin bucket per day, yielding 15,136
user-day rows and 211,904 measurement rows. Fixture boundaries and the simulated
Firebase capture drop are pipeline variables, rather than SQL date literals.

| Layer | Asset | Purpose |
|---|---|---|
| Raw | `raw.data_quality_demo_user_days` | Materializes the original deterministic fixture equations once. |
| Staging | `staging.data_quality_user_days` | Deduplicates user/date keys, hashes synthetic IDs and labels source membership. |
| Staging | `staging.data_quality_comparison_values` | Unpivots measurements into comparable metric/unit rows, retaining unsupported-source NULLs. |

The reusable catalog models live at the repository root:

- `semantic/data_quality_user_days.yml`: distinct users, additive revenue/events,
  ratios of aggregates, user/day gaps and period-level user gaps.
- `semantic/data_quality_comparisons.yml`: source comparisons grouped by metric
  and unit, with overview, revenue and event segments. The Users row counts
  distinct IDs rather than summing daily counts.

The standalone DAC project links to these canonical files from its `semantic/`
directory. Model definitions are not copied into the dashboard or duplicated.
Source relations are materialized tables; semantic sources contain no embedded
SELECT statements. SQL for fixture generation and unpivoting lives only in
Bruin assets, not dashboard files.

Revenue and impressions from the unattributed AppLovin bucket remain in source
totals but are excluded from user counts and gap tables. Source membership is
constant per user in this fixture, so presence buckets remain valid for any
selected subinterval. Differences and ranking use unrounded totals; display
metrics round revenue to two decimals. Division by zero returns NULL.

## Run and verify

From the repository root, using the existing automatically discovered Bruin
configuration and the `bruin-playground-arsalan` connection:

```bash
bruin validate data-quality
bruin run data-quality/assets/raw/data_quality_demo_user_days.sql
bruin run data-quality/assets/staging/data_quality_user_days.sql
bruin run data-quality/assets/staging/data_quality_comparison_values.sql

bruin semantic validate --model data_quality_user_days --model data_quality_comparisons
bruin semantic check --model data_quality_user_days --model data_quality_comparisons

dac validate --dir data-quality/dashboard-dac
dac check --dir data-quality/dashboard-dac
dac serve --dir data-quality/dashboard-dac --port 8321
# Developer preview: http://localhost:8321

python -m pip install -r data-quality/tests/requirements.txt
python -m unittest discover -s data-quality/tests -v
```

Eight semantic quality checks preserve the original full-period, four-day and
single-day distinct-user counts and overview comparisons, exclude unattributed
users, and enforce one row per user/day. The migration was also compared against
the original eight queries for all 23 widgets across four intervals: full
period, September 20–23, September 21 alone, and an out-of-coverage interval.
All 92 widget comparisons passed. The empty-period assertion expects no
observations: summary tables now show no rows rather than the old SQL's scaffold
of NULL-valued metric rows.

Charts retain the Wong palette and source-specific line dashes. Each chart has
a full-width context, chart and source-footnote row; ratio charts include a
1.00 parity guide. Current DAC 0.28.1 does not expose the repo fork's `hideName`
or `seriesNames` fields, so this project uses its supported widget titles and
explicit encoding keys instead. The final Daily Detail section contains the
shared Methodology.

## Apply to Bruin Cloud

Bruin Cloud imports semantic models from the project's configured branch,
currently `main`. **Merge the PR and let the project sync before applying the
dashboard definition.** Saving beforehand is rejected because these new models
are not yet in the Cloud catalog. The migration has not changed the published
dashboard.

After sync, run:

```bash
bash data-quality/apply-dashboard.sh
```

This updates the existing dashboard **1135** as a draft. It preserves its
visibility and folder and does not publish. Review the draft in Bruin Cloud;
publishing is a separate action.

## Limitations

- This fixture is a fixed 30-day demo. Daily runs rebuild the same small tables;
  they do not fetch fresh production events. Date intervals outside coverage
  return no observations.
- Changing fixture variables intentionally changes the data; update fixture
  expectations in the semantic checks at the same time.
- The long-form comparison model must be grouped by `metric` and `unit`, or
  constrained to a single metric. Adding amounts with different units is not a
  meaningful total.
- Raw and staging tables use `create+replace` because the fixture is small and
  deterministic. Use production ingestion and appropriate incremental
  materialization before replacing the synthetic sources.
