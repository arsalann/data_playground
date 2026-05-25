---
name: schema-drift-check
description: Detect and respond to schema drift between a source system and a Bruin asset's declared columns. Compares live schema to the asset definition, classifies the drift, and either opens a maintenance PR or escalates. Use when diagnose identifies a column/type mismatch error.
argument-hint: "<asset> [suspected_column]"
---

# Schema Drift Check

Sources change schemas. Sometimes the change is additive and harmless; sometimes it breaks every downstream consumer. This skill figures out which, and proposes the minimum-impact response.

## When to Use

- `pipeline-diagnose` returned hypothesis `schema-drift`.
- A `bruin validate` warning flagged drift.
- A source vendor announced a schema change and we want to be ready.
- A query returns unexpected nulls in a column that used to be populated.

Do not use for: data quality issues where the schema is fine but values are wrong (use `data-quality-investigate`), missing rows (use `freshness-sla-check`), or proposing brand-new columns nobody asked for.

## Inputs

| Input | Required | Example | Notes |
|---|---|---|---|
| `asset` | yes | `raw.wikipedia_pageviews` | Asset to check. |
| `suspected_column` | no | `view_count` | If known, narrows the diff. |
| `source_connection` | no | `wikipedia_api` | Inferred from asset if not provided. |

## Context to Gather

1. **Declared schema** - the asset's `columns:` block in YAML, or the SELECT projection in the SQL file.
2. **Live source schema** - query the source. Method depends on connector:
   - Database: `INFORMATION_SCHEMA.COLUMNS` or `DESCRIBE`.
   - REST API: fetch one record, infer field shape.
   - File: read the header / first record of the latest file.
3. **Sample data** - 100 rows from the source to confirm types, not just names.
4. **Recent source changes** - if the source has changelogs/release notes accessible, scan for the last 30 days. Skip if not available.
5. **Downstream consumers** - every asset that references the drifted column. Use `bruin lineage` or grep the repo.
6. **Last successful run** - what was the schema then? If we have stored DDL history, diff against it.

## Drift Classification

| Drift type | Definition | Default response |
|---|---|---|
| `column-added` | New column in source, not in asset declaration | `maintenance-pr` to add the column (if useful) or note as ignored |
| `column-removed` | Column in asset declaration is gone from source | `maintenance-pr` to remove the column AND update downstream — requires approval if downstream count > 0 |
| `column-renamed` | A new column appeared with the same data shape as a removed one | `maintenance-pr` proposing the rename across asset + downstream |
| `type-narrowed` | e.g. `string` → `int`, `timestamp` → `date` | Escalate — almost always a breaking change for consumers |
| `type-widened` | e.g. `int` → `bigint`, `varchar(10)` → `varchar(255)` | `maintenance-pr` to update declared type only |
| `nullability-change` | Column became nullable, or stopped being nullable | Update declaration; warn if downstream assumed non-null |
| `enum-value-added` | Categorical column got a new distinct value | Update any hardcoded `CASE` / filter logic, then PR |
| `cardinality-shift` | Distinct count of a categorical column changed > 50% | Not strictly drift — route to `anomaly-investigate` |

A single check can produce multiple drift findings. Handle each independently.

## Decision Tree

```
declared = read_declared_schema(asset)
live = fetch_live_source_schema(asset.source)
diffs = compute_diff(declared, live)

if not diffs:
    return result(status='no-drift')

findings = []
for diff in diffs:
    finding = classify_drift(diff, declared, live, sample=fetch_sample(asset.source))
    findings.append(finding)

# Sort by risk: removals and type narrowings first.
findings.sort(by=risk_level, descending=True)

actions = []
for finding in findings:
    if finding.type in ('type-narrowed',):
        actions.append(escalate(finding))
    elif finding.type == 'column-removed' and count_downstream(finding.column) > 0:
        actions.append(escalate(finding, reason='downstream consumers exist'))
    elif finding.type == 'cardinality-shift':
        actions.append(reroute(finding, skill='anomaly-investigate'))
    else:
        actions.append(propose_pr(finding))

return result(status='drift-detected', findings=findings, actions=actions)
```

## Actions & Guardrails

- **Auto-allowed**: read schemas, sample source data (limit 100 rows), compute diffs, write findings to `.context/`, draft PR content.
- **Requires approval**: opening a PR that removes columns, opening a PR that changes types of columns with downstream consumers, anything classified `type-narrowed`.
- **Never allowed**: modifying source-system schemas (we are a consumer, not an owner), silently ignoring drift, opening a PR without listing every downstream consumer in the body.

## Verification

After a PR is opened (via `maintenance-pr`):

1. CI must pass on the PR branch — specifically `bruin validate` on the modified asset.
2. A `bruin run --dry-run` on the modified asset must succeed.
3. If the PR removes or renames a column, every downstream asset listed in the PR body must be either updated in the same PR or have its own follow-up PR linked.

The skill does not auto-merge — that is always a human decision.

## Reporting

Write findings to `.context/drift-<asset>-<timestamp>.yml`:

```yaml
asset: raw.wikipedia_pageviews
checked_at: 2026-05-22T14:30:00Z
source_schema_version: live
findings:
  - column: view_count
    type: column-renamed
    from: view_count
    to: views
    confidence: high
    evidence: same dtype, same distribution on 100-row sample, removed and added on same day
    downstream_consumers: [marts.daily_top_articles, marts.weekly_trends]
    action: maintenance-pr
    pr_url: null
  - column: page_size_bytes
    type: column-added
    dtype: int64
    nullable: true
    action: noted-not-adopted
    reason: no current consumer; will reconsider on request
no_drift_columns: 14
```

Reports must distinguish "no drift" (we checked, schema is unchanged) from "could not check" (the source was unreachable). Never conflate the two.
