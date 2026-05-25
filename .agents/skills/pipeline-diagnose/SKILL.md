---
name: pipeline-diagnose
description: Deep-dive on a single failed or suspicious Bruin asset. Pulls logs, error patterns, dependency graph, and recent commits to produce a root-cause hypothesis. Use when triage classified an issue as 'unknown', or a human asks "why did X fail".
argument-hint: "<asset name> [run_id]"
---

# Pipeline Diagnose

Forensics, not repair. Take one asset, gather every signal about its last failure, and produce a structured hypothesis another skill (or human) can act on. This skill should leave the system in the exact state it found it.

## When to Use

- `pipeline-triage` returned class `unknown` for an asset.
- A human asks "why did this fail" without enough context to pick a specialist skill.
- A retry already failed and we need to know more before retrying again.
- A quality check passed but a downstream consumer reported wrong numbers.

Do not use for: scanning a whole pipeline (use `pipeline-triage`), executing a fix, or answering "is X stale" (use `freshness-sla-check`).

## Inputs

| Input | Required | Example | Notes |
|---|---|---|---|
| `asset` | yes | `marts.daily_top_articles` | Fully qualified asset name. |
| `run_id` | no | `run_01HXYZ...` | Specific run to diagnose. Defaults to most recent failed run. |
| `pipeline` | no | `wikipedia-ai-trends` | Inferred from asset if not provided. |

## Context to Gather

Gather all of these — partial diagnosis is worse than no diagnosis because it biases the hypothesis.

1. **Asset definition** - read the YAML/SQL file. Note: materialization type, partition column, dependencies, declared columns, quality checks.
2. **Last successful run** - timestamp, duration, row count. Establishes baseline.
3. **Failed run** - full stderr/stdout, exit code, duration before failure, partial output if any.
4. **Error fingerprint** - extract the deepest error message (not the wrapper). Hash it so we can match against known patterns.
5. **Upstream state** - for each declared dependency, was its last run successful? When? Did the schema or row count change meaningfully?
6. **Downstream impact** - which assets depend on this one and are now blocked or stale?
7. **Recent changes** - `git log --since="7d" -- <asset_file> <upstream_files>`. Was the asset or any upstream touched recently?
8. **Connection health** - if the error mentions a source connector, check whether other assets using the same connector also failed.
9. **Resource signals** - duration trend over last 14 runs. A 10x slowdown before failure points at capacity, not logic.

## Error Pattern Library

Match the error fingerprint against these common shapes. The match is a hypothesis, not a verdict.

| Pattern | Likely cause | Suggested next skill |
|---|---|---|
| `column "X" does not exist` / `unknown field X` | Schema drift in source or upstream | `schema-drift-check` |
| `cannot cast TYPE_A to TYPE_B` | Type drift or new enum value | `schema-drift-check` |
| `Connection reset` / `EOF` / `5xx from <host>` | Transient source flake | retry once, then `pipeline-report` if it persists |
| `Quota exceeded` / `slot pool full` / `rate limit` | Capacity / billing | `pipeline-report`, do not retry |
| `Query exceeded resource limits` / OOM | Capacity or a join that grew | flag for human; suggest partition or filter |
| `Permission denied` / `access denied` / 401/403 | Credential expiry or IAM change | `pipeline-report`, do not retry |
| `Custom check failed: <name>` | Data quality issue | `data-quality-investigate` |
| `timeout` on a query that historically completed | Capacity or a join blowup | check upstream row counts |
| Empty pattern but exit code != 0 | Bruin runner issue or signal kill | check Bruin Cloud status |
| Output produced 0 rows but check expects > 0 | Source emitted nothing — could be expected (e.g. weekend) or a silent break | `freshness-sla-check` to compare against same-day-last-week |

## Decision Tree

```
asset = load_asset_definition(input.asset)
run = load_run(input.run_id or last_failed)
upstreams = check_upstream_states(asset.dependencies)
fingerprint = extract_error_fingerprint(run.logs)

# Eliminate upstream-caused failures first.
broken_upstreams = [u for u in upstreams if not u.healthy]
if broken_upstreams:
    return hypothesis(
        cause='upstream-failure',
        upstreams=broken_upstreams,
        recommendation='diagnose upstream(s) before retrying this asset'
    )

# Match against known patterns.
pattern = match_pattern(fingerprint)
if pattern:
    return hypothesis(cause=pattern.cause, recommendation=pattern.next_skill)

# Check for code regression.
recent = git_log(asset.file, since='7d')
if recent and run.first_failed_at > recent[0].timestamp:
    return hypothesis(
        cause='code-regression',
        commit=recent[0],
        recommendation='review commit and revert or fix'
    )

# Capacity check.
if run.duration > 3 * baseline.duration:
    return hypothesis(cause='capacity-or-data-volume', recommendation='check input row counts')

return hypothesis(cause='unknown', recommendation='escalate to human with full context dump')
```

## Actions & Guardrails

- **Auto-allowed**: read asset files, read git log, query Bruin MCP for run state, write diagnosis to `.context/diag-<asset>-<timestamp>.md`.
- **Requires approval**: nothing — this skill never acts on the pipeline.
- **Never allowed**: speculation without a matched signal. If no pattern matches and no commit lines up, the cause is `unknown` — say so. Do not pick the most-plausible-sounding cause.

## Verification

The diagnosis is verified by the skill that acts on it. This skill is correct if the recommended next skill resolves the issue; record the outcome back in the diagnosis file so the pattern library can be improved over time.

## Reporting

Write the diagnosis to `.context/diag-<asset>-<timestamp>.md` in this shape, and return the file path:

```yaml
asset: marts.daily_top_articles
run_id: run_01HXYZ...
diagnosed_at: 2026-05-22T14:30:00Z
hypothesis:
  cause: schema-drift
  confidence: high
  evidence:
    - error fingerprint matched 'column does not exist' pattern
    - upstream raw.wikipedia_pageviews was modified by commit abc123 on 2026-05-21
    - asset declared column `view_count` but query references `views`
recommendation:
  skill: schema-drift-check
  inputs:
    asset: marts.daily_top_articles
    suspected_column: views
upstreams_checked: [raw.wikipedia_pageviews, raw.wikipedia_articles]
downstream_blocked: [marts.weekly_trends, dashboards.top_articles]
```

If `cause: unknown`, the report must list every signal that was checked and came back clean. "I do not know" with evidence is a valid output; "I do not know" with no work shown is not.
