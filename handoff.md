# Handoff

## Goal

Update the `.agents/skills` documentation and self-healing pipeline skills based on `skill-review.md`, while keeping skill discovery safe by leaving skill directories flat under `.agents/skills/<skill-name>/SKILL.md`.

## Current State

The self-healing pipeline skill review has been applied to the relevant self-healing skills. The root skills README has been converted into a short global index, and the full self-healing guide now lives in `.agents/skills/self-healing-pipelines.md`.

The `create-dashboard` skill was intentionally reverted after the review changes, per user request. It remains a standalone skill under `.agents/skills/create-dashboard/SKILL.md`.

`skill-review.md` has been cleaned from an unchecked TODO list into a resolved review summary. This handoff file has been cleaned into the requested structure.

## Files in Flight

- `.agents/skills/README.md` - short global skills index.
- `.agents/skills/self-healing-pipelines.md` - collection-level guide for self-healing pipeline skills.
- `.agents/skills/pipeline-triage/SKILL.md`
- `.agents/skills/pipeline-diagnose/SKILL.md`
- `.agents/skills/pipeline-backfill/SKILL.md`
- `.agents/skills/schema-drift-check/SKILL.md`
- `.agents/skills/data-quality-investigate/SKILL.md`
- `.agents/skills/freshness-sla-check/SKILL.md`
- `.agents/skills/anomaly-investigate/SKILL.md`
- `.agents/skills/maintenance-pr/SKILL.md`
- `.agents/skills/pipeline-report/SKILL.md`
- `skill-review.md`
- `handoff.md`

`create-dashboard/SKILL.md` is clean and was not kept in the review change set.

## Changed

- Added exact Bruin Cloud CLI forms and required flags across self-healing skills.
- Clarified Cloud token resolution: `bruin-cloud` is a repo convention, while the CLI reads `--api-key`, `BRUIN_CLOUD_API_KEY`, or the first `.bruin.yml` `bruin` connection.
- Standardized Cloud run statuses to `success`, `failed`, and `running`, with `checks_failed` noted for asset instances.
- Documented that `bruin cloud pipelines errors --output json` has no `--project-id` flag and must be filtered client-side.
- Documented that `bruin cloud runs trigger` and `bruin cloud runs rerun` return success envelopes, so run IDs must be obtained by polling `runs list` and verifying with `runs get`.
- Added read-only `bruin query` guidance with `--description`, `--limit`, `--timeout`, `--dry-run`, and `--output json`.
- Added human-approval guardrail for `bruin query --dangerously-bypass-soft-limits`.
- Added local static commands for validation, lineage, and quality checks, including variant-aware examples.
- Clarified Bruin quality-check behavior, including runtime default `blocking: true` for custom checks and column checks.
- Recast `meta.freshness` as a project convention rather than an official Bruin freshness SLA field.
- Split docs:
  - root `.agents/skills/README.md` is a global index.
  - `.agents/skills/self-healing-pipelines.md` is the full self-healing collection guide.
- Rewrote `skill-review.md` as a concise resolved review summary.
- Rewrote `handoff.md` as this concise current handoff.

## Failed Attempts

- Review changes were initially applied to `create-dashboard/SKILL.md`, but the user asked to revert that directory. It has been restored.
- The original `skill-review.md` still showed unchecked TODOs after the changes were applied. It has now been replaced with a resolved summary.
- The original `handoff.md` contained stale session history and fake-pipeline context that no longer matched the active task. It has now been replaced with the current state.

## Next Steps

- Review the remaining diff for the self-healing skill updates and docs split.
- Decide whether `skill-review.md` and `handoff.md` should be committed as project artifacts or kept only as local working notes.
- If keeping these changes, run `git diff --check` before committing.
