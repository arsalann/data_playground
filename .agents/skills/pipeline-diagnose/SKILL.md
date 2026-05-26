---
name: pipeline-diagnose
description: Deep-dive on a single failed or suspicious Bruin asset. Pulls logs, error patterns, dependency graph, and recent commits to produce a root-cause hypothesis. Use when triage classified an issue as 'unknown', or a human asks "why did X fail".
argument-hint: "<asset name> [run_id]"
---

# Pipeline Diagnose

Forensics, not repair. Take one asset, gather every signal about its last failure, and produce a structured hypothesis another skill (or human) can act on. This skill should leave the system in the exact state it found it.

This is a Bruin Cloud skill. Use Bruin Cloud MCP first; use `bruin cloud ... --output json` as the CLI fallback. `bruin-cloud` is the repo convention for the `.bruin.yml` `bruin` connection, but the CLI cannot select that connection by name; export `BRUIN_CLOUD_API_KEY` when multiple `bruin` connections exist. Do not use local `bruin run` for operational execution.

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
| `project_id` | no | `01krk817ys2j45frftg1q4xfgv` | Bruin Cloud project ID. Required if the account can see multiple projects and it cannot be inferred from a triage snapshot. |
| `variant` | no | `prod-us` | Concrete Bruin pipeline variant, if the Cloud pipeline is variant-backed. Pass this to local static commands. |

## Context to Gather

Gather all of these — partial diagnosis is worse than no diagnosis because it biases the hypothesis.

1. **Cloud run diagnosis** - use Bruin Cloud MCP or `bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline> --latest --output json` / `bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline> --run-id <run-id> --output json` for the consolidated failed-assets/checks/logs view.
2. **Cloud run details** - use `bruin cloud runs get --project-id <project-id> --pipeline <pipeline> --run-id <run-id> --output json` or `bruin cloud runs get --project-id <project-id> --pipeline <pipeline> --latest --output json`.
3. **Cloud asset details** - use `bruin cloud assets get --project-id <project-id> --pipeline <pipeline> --asset <asset> --output json`.
4. **Cloud instance details and logs** - use exact commands for the run:
   - `bruin cloud instances get --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --asset <asset> --output json`
   - `bruin cloud instances logs --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) (--asset <asset> [--step-name <step>] | --step-id <step-id>) --output json`
   - `bruin cloud instances failed-logs --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json`
5. **Asset definition from repo** - read the YAML/SQL/Python file. Note materialization type, strategy, interval use, dependencies, declared columns, and quality checks. If parsing/validation evidence matters, run `bruin validate <asset-file-path> --output json` and include `--variant <variant>` when applicable.
6. **Last successful run** - timestamp, duration, row count. Establishes baseline.
7. **Failed run** - status, failed instance/check, duration before failure, partial output if any, and Cloud run URL/ID.
8. **Error fingerprint** - extract the deepest error message (not the wrapper). Hash it so we can match against known patterns.
9. **Upstream state** - for each declared dependency, was its last run successful? When? Did the schema or row count change meaningfully?
10. **Downstream impact** - which assets depend on this one and are now blocked or stale? Use Bruin Cloud asset state and local `bruin lineage <asset-file-path> --output json --full` when the repo is available. Include `--variant <variant>` for variant-backed Cloud pipelines.
11. **Recent changes** - inspect `git log --since="7d" -- <asset_file> <upstream_files>` and recent PRs/branches touching the asset or upstreams.
12. **Connection health** - if the error mentions a source connector, check whether other assets using the same connector also failed.
13. **Resource signals** - duration trend over recent Cloud runs. A 10x slowdown before failure points at capacity, not logic.

## Error Pattern Library

Match the error fingerprint against these common shapes. The match is a hypothesis, not a verdict.

| Pattern | Likely cause | Suggested next skill |
|---|---|---|
| `column "X" does not exist` / `unknown field X` | Schema drift in source or upstream | `schema-drift-check` |
| `cannot cast TYPE_A to TYPE_B` | Type drift or new enum value | `schema-drift-check` |
| `Connection reset` / `EOF` / `5xx from <host>` | Transient source flake | route to `pipeline-backfill`, which may call `bruin cloud runs rerun ... --only-failed` or `bruin cloud runs trigger ...` after preflight |
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

- **Auto-allowed**: read asset files, read git/PR history, query Bruin Cloud MCP or `bruin cloud` for run/asset/instance/log state, write diagnosis to `.context/diag-<asset>-<timestamp>.md`.
- **Requires approval**: nothing — this skill never acts on the pipeline.
- **Never allowed**: local operational runs, speculation without a matched signal. If no pattern matches and no commit lines up, the cause is `unknown` — say so. Do not pick the most-plausible-sounding cause.

## Verification

The diagnosis is verified by the skill that acts on it. This skill is correct if the recommended next skill resolves the issue; record the outcome back in the diagnosis file so the pattern library can be improved over time.

## Reporting

Write the diagnosis to `.context/diag-<asset>-<timestamp>.md` in this shape, and return the file path:

```yaml
asset: marts.daily_top_articles
run_id: run_01HXYZ...
bruin_cloud:
  project_id: 01krk817ys2j45frftg1q4xfgv
  pipeline: wikipedia-ai-trends
  run_url: https://cloud.getbruin.com/...
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
