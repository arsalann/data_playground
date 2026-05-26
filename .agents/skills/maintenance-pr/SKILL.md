---
name: maintenance-pr
description: Open a pull request for routine pipeline maintenance proposed by another skill (column rename, type bump, removed-column cleanup, dependency update, check threshold adjustment). Never opens PRs unprompted; always gated on a prior skill's finding.
argument-hint: "<finding file path>"
---

# Maintenance PR

The only skill in the set with write access to the repository. Every PR it opens must be traceable to a finding file produced by another skill — no freelancing.

This skill may use local Bruin validation because it edits repository files, but it must not create operational local runs. Use Bruin Cloud MCP or docs/source-verified `bruin cloud ... --output json` commands for Cloud context. `bruin-cloud` is the repo convention for the `.bruin.yml` `bruin` connection, but the CLI cannot select that connection by name; export `BRUIN_CLOUD_API_KEY` when multiple `bruin` connections exist.

## When to Use

- `schema-drift-check` produced a finding with `action: maintenance-pr`.
- `data-quality-investigate` recommended a transform fix.
- `freshness-sla-check` recommended documenting an inferred freshness/growth expectation or adding a custom check, with approval.
- A scheduled tick found a known-safe maintenance task (e.g. a dependency bump on the allow-list).
- A human explicitly asks for a routine maintenance PR.

Do not use for: feature work, refactors, anything that changes pipeline _behavior_ in a way users would notice, or any change without a finding file. New features need a human-authored design, not a skill-authored PR.

## Inputs

| Input | Required | Example | Notes |
|---|---|---|---|
| `finding_file` | yes | `.context/drift-raw.wikipedia_pageviews-20260522.yml` | The finding from another skill. Must exist and be valid. |
| `branch_name` | no | `self-healing/drift-wikipedia-views-rename` | Derived from finding if not provided. Must start with `self-healing/`. |
| `draft` | no | `true` \| `false` | Default `true`. Non-trivial PRs should land as drafts for human review. |

## Allowed Change Types

Only these change types are auto-allowed. Anything else is a human task.

| Change | Examples | Approval |
|---|---|---|
| `column-add` | Add a declared column that exists in source but not in asset YAML | auto |
| `column-rename` | Rename a column across asset YAML + all downstream SQL | auto if downstream count <= 5, approval otherwise |
| `column-remove` | Remove a declared column that no longer exists in source | approval required if any downstream references it |
| `type-widen` | `int` → `bigint`, `varchar(N)` → `varchar(M)` where M > N | auto |
| `type-narrow` | Any narrowing | NEVER auto — escalate |
| `check-threshold-adjust` | Loosen a quality check threshold based on documented evidence | approval required |
| `custom-check-create` | Add a new custom check backed by a finding, e.g. minimum recent row growth | approval required |
| `custom-check-update` | Update an existing custom check based on documented evidence | approval required |
| `asset-description-update` | Document cadence, freshness expectations, expected row growth, source caveats, or operational notes in an asset description | approval required |
| `column-description-update` | Improve or correct column-level descriptions, units, caveats, or expected values | auto if non-behavioral, approval if tied to a check/contract change |
| `dedup-window-adjust` | Change a windowed dedup interval | approval required |
| `dependency-bump` | Patch-version bump of an allow-listed dependency | auto |
| `dependency-bump-minor-or-major` | Anything beyond patch | approval required |
| `dead-code-removal` | Remove an asset that has no downstream consumers and zero successful runs in 90 days | approval required |

## Pre-flight Checks

Before any branch is created:

1. **Finding is valid** - file exists, parses, has a recognized `action` and `recommendation`.
2. **Working tree is clean** - we are operating on `main` (or the project's configured base branch) with no uncommitted changes.
3. **No existing PR** for the same finding - search open PRs by branch name pattern. If one exists, comment on it instead of opening another.
4. **Recent PR/code context is reviewed** - inspect recent PRs and commits touching the same assets so the new PR does not duplicate, conflict with, or hide a recent change.
5. **Change type is on the allow-list** - if not, abort and route to `pipeline-report` as an escalation.
6. **Validation scope is decided** - asset-only validation is faster (`bruin validate <asset-file-path> --output json`), but pipeline validation is required when dependency definitions, downstream SQL, pipeline defaults, or shared config changed (`bruin validate <pipeline-dir> --output json`). For variant-bearing pipelines, validate all affected concrete variants or explicitly pass `--variant <variant>`.
7. **Cloud validation context is checked** - inspect `bruin cloud pipelines errors --output json` where relevant and filter client-side; if Cloud reports unrelated active errors, note them in the PR.
8. **No secrets in the diff** - scan the diff for credential-shaped strings; abort if anything matches.
9. **Behavioral write fields are reviewed** - any change to materialization strategy, `primary_key`, `update_on_merge`, `merge_sql`, or `incremental_key` requires explicit human approval because it changes how Bruin writes data.

## PR Construction

Branch name: `self-healing/<change-type>/<short-slug>-<YYYYMMDD>`.

Commit message:

```
<change-type>: <one-line summary>

Trigger: <skill that produced the finding>
Finding: <relative path to finding file>
Affected assets: <comma-separated list>
Downstream impact: <count, or "none">
```

PR title: `[self-healing] <change-type>: <one-line summary>` (max 80 chars).

PR body template:

```markdown
## What

<one short paragraph describing the change>

## Why

<paste the relevant section of the finding, including evidence>

## Triggered by

- Skill: <skill name>
- Finding: `<path>`
- Detected at: <timestamp>

## Scope

- Files changed: <list>
- Affected assets: <list>
- Downstream consumers (updated in this PR): <list, or "none">
- Downstream consumers (NOT updated, need follow-up): <list, or "none">

## Verification

- [ ] `bruin validate <pipeline-dir-or-asset-file> --output json` passes, with `--variant <variant>` where applicable
- [ ] Bruin Cloud validation/dry-run equivalent is checked where available
- [ ] Quality checks passed only if verified by Cloud state or `bruin run --only checks <asset-file>`; otherwise marked not executed
- [ ] End-to-end tested in a development/shadow/sandbox environment, or explicitly marked NOT TESTED when no safe environment exists

## Rollback

Revert this commit. No state outside the repo was changed.

## Notes

- This PR was opened by the `maintenance-pr` skill. The repo is the source of truth — review the diff, not the body.
```

Never include language like "should fix" or "may resolve". State what the change does and what evidence supported it. Anything more is speculation.

## Decision Tree

```
finding = parse(input.finding_file)
if finding.action not in ('maintenance-pr', 'column-add', 'column-rename', ...):
    return abort('finding does not request a PR')

change_type = finding.change_type
if change_type not in ALLOWED_CHANGE_TYPES:
    return escalate(reason=f'change type {change_type} not auto-allowed')

if requires_approval(change_type, finding):
    emit_pr_plan_for_approval(finding)
    return  # do not open until approved

checks = preflight(finding)
if checks.failed:
    return abort(checks.failures)

branch = create_branch(name=derive_branch_name(finding))
if not branch.name.startswith('self-healing/'):
    return abort('self-healing branch prefix is required')
apply_edits(finding.edits)
if not bruin_validate(scope='<asset-file-path or pipeline-dir>', output='json', variant=input.variant):
    discard_branch(branch)
    return abort('bruin validate failed after edits')

commit(message=build_commit_message(finding))
push(branch)
pr = open_pr(title=..., body=..., draft=input.draft)
return result(pr_url=pr.url, branch=branch)
```

## Actions & Guardrails

- **Auto-allowed**: branch creation, file edits scoped to the finding, static `bruin validate <path> --output json`, Bruin Cloud validation-error inspection through `bruin cloud pipelines errors --output json`, commit, push, `gh pr create` with `--draft`.
- **Requires approval**: non-draft PRs, any change type marked as such above, force-pushing to an existing PR, opening more than 3 PRs in a single invocation, and end-to-end tests that touch production.
- **Never allowed**: operational local runs, merging the PR (always a human action), pushing to the base branch directly, modifying files outside the finding's declared scope, opening a PR without a finding file, using `gh pr create --no-draft` for an auto-allowed change without explicit approval, claiming quality checks passed without Cloud verification or `bruin run --only checks <asset-file>`, or claiming end-to-end test coverage when no safe test environment was used.

For PR verification, try to test end to end only when a development, shadow, sandbox, or otherwise safe non-production environment exists. If no safe environment exists, the PR body and report must clearly state: **NOT TESTED END TO END — MUST BE TESTED BEFORE DEPLOYMENT**. Do not run end-to-end tests against production unless a human explicitly approves and the test is safe.

## Verification

A PR is "successfully opened" when:

1. The PR URL is returned.
2. CI starts on the branch (or is not configured — note which).
3. The finding file is updated to include the PR URL.
4. End-to-end test status is recorded as either tested in a safe non-production environment or explicitly not tested.

The PR is not "verified" — only humans verify. The skill's job ends when the PR is open and waiting for review.

## Reporting

Update the source finding file with the PR URL, then hand off to `pipeline-report` with:

```yaml
pr_url: https://github.com/org/repo/pull/123
branch: self-healing/column-rename/wikipedia-views-20260522
finding: .context/drift-raw.wikipedia_pageviews-20260522.yml
change_type: column-rename
draft: true
files_changed:
  - raw.wikipedia_pageviews/pageviews.yml
  - marts/daily_top_articles.sql
affected_assets:
  - raw.wikipedia_pageviews
  - marts.daily_top_articles
downstream_consumers_not_updated: []
ci_status: pending
validation_status: passed
end_to_end_test_status: NOT TESTED END TO END — MUST BE TESTED BEFORE DEPLOYMENT
```

If the PR could not be opened, the report must explain which pre-flight check failed. "Tried and failed" is a valid outcome; "silently skipped" is never acceptable.
