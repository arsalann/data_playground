# Self-Healing Pipeline Skills

A set of composable skills for Bruin Cloud scheduled agents to perform routine data-engineering and analytics-engineering work: triaging failures, diagnosing issues, backfilling, investigating data quality, handling schema drift, explaining anomalies, opening maintenance PRs, and reporting to Slack.

Each skill is a single `SKILL.md` file in its own directory. The skills are atomic on purpose: one skill, one job. Composition happens at the scheduled-agent layer, not inside any individual skill.

## The 7-Section Contract

Every skill follows the same shape so the scheduler can pick the right one and the human can audit the run:

1. **When to use** — and explicitly when NOT to.
2. **Inputs** — required vs. optional, with examples.
3. **Context to gather** — the read-only calls run before any decision.
4. **Decision tree** — the classification logic, in pseudo-code.
5. **Actions & guardrails** — auto-allowed / requires-approval / never-allowed.
6. **Verification** — how the skill knows the action stuck.
7. **Reporting** — what gets written to `.context/` and to Slack.

`.context/` is the shared filesystem between skills. Triage drops a snapshot; diagnose reads it; maintenance-pr requires a finding file as input; report annotates them all with the resulting permalink. The chain is auditable end-to-end.

## Skill Map

| Skill | One-line job | Writes to repo? | Posts to Slack? | Calls Bruin CLI? |
|---|---|---|---|---|
| [`pipeline-triage`](pipeline-triage/SKILL.md) | Classify what's wrong and route | no | no (hands to report) | yes (read) |
| [`pipeline-diagnose`](pipeline-diagnose/SKILL.md) | Single-asset forensics | no | no | yes (read) |
| [`pipeline-backfill`](pipeline-backfill/SKILL.md) | Rerun a range safely | no | no (hands to report) | yes (write) |
| [`schema-drift-check`](schema-drift-check/SKILL.md) | Detect & classify source drift | no | no | yes (read) |
| [`data-quality-investigate`](data-quality-investigate/SKILL.md) | Walk failed checks to root cause | no | no | yes (read) |
| [`freshness-sla-check`](freshness-sla-check/SKILL.md) | Find stale data, classify cause | no | no | yes (read + limited retrigger) |
| [`anomaly-investigate`](anomaly-investigate/SKILL.md) | Explain a metric spike/dip | no | no | yes (read) |
| [`maintenance-pr`](maintenance-pr/SKILL.md) | Open a PR for an allow-listed fix | **yes** | no (hands to report) | yes (validate) |
| [`pipeline-report`](pipeline-report/SKILL.md) | Slack status / incident / digest | annotates findings | **yes** | no |

Only `maintenance-pr` writes to the repo. Only `pipeline-report` posts to Slack. Every other skill is read-only against the world.

## Typical Run Shapes

### Alert-driven (most common)

```
alert webhook
  → pipeline-triage             (classify each failing asset)
      → schema-drift-check         (if any class = schema-drift)
          → maintenance-pr
      → data-quality-investigate   (if any class = quality-fail)
          → maintenance-pr            → pipeline-backfill
      → anomaly-investigate        (if any class = anomaly)
      → freshness-sla-check        (if any class = stale)
      → pipeline-diagnose          (if class = unknown)
  → pipeline-report             (always last; one consolidated message)
```

### Scheduled tick (no alert)

```
cron tick (every 15-30 min)
  → freshness-sla-check         (catch missed runs even when nothing errored)
  → pipeline-triage             (full state scan)
  → pipeline-report             (only if there's something to say)
```

### Human-asked ("why did this fail?")

```
human question
  → pipeline-diagnose <asset>   (gather context, produce hypothesis)
  → pipeline-report             (post the hypothesis to a thread)
```

### Daily digest

```
daily cron
  → pipeline-triage --pipeline all --since 24h
  → pipeline-report             (severity: info, digest format)
```

## Per-Skill Reference

### `pipeline-triage`

**Purpose** — Entry point and dispatcher. Pulls full pipeline state, classifies every issue into exactly one of 9 classes, hands each one to the right specialist.

**Use when** — an alert fires, a scheduled tick runs, a human asks "what's broken", or another skill needs a fresh state snapshot.

**Don't use for** — single-asset deep dives (use `pipeline-diagnose`), running fixes (use specialists), or composing the human-facing message (use `pipeline-report`).

**Key inputs** — `pipeline` (name or `all`), `since` (lookback), `severity_floor`.

**Key guardrail** — never modifies state; every issue must receive a class, even if `unknown`; partial state never reported as health.

**Hands off to** — every other specialist skill, and always ends with `pipeline-report`.

---

### `pipeline-diagnose`

**Purpose** — Forensics on one asset. Pulls logs, error fingerprint, upstream state, recent commits, and matches against a library of common error patterns to produce a root-cause hypothesis.

**Use when** — triage classified an asset as `unknown`, a retry already failed, or a human asks "why".

**Don't use for** — scanning a whole pipeline (use `pipeline-triage`), or executing a fix.

**Key inputs** — `asset`, optional `run_id`.

**Key guardrail** — speculation without a matched signal is forbidden. "I don't know" with evidence is a valid output; "I don't know" with no work shown is not.

**Hands off to** — whichever specialist the matched pattern recommends, or `pipeline-report` if `unknown`.

---

### `pipeline-backfill`

**Purpose** — Safely rerun an asset for a specific time range, partition-by-partition, with row-count verification at each step.

**Use when** — a fix has merged, a transient failure needs a retry, or an upstream republished historical data.

**Don't use for** — first-time runs (those happen on schedule), running an asset with no prior success, or "just rerun everything" requests with no scoped range.

**Key inputs** — `asset`, `start`, `end`, `reason` (required, logged for audit), `mode` (`replace`/`append`/`merge`), `dry_run` (default true).

**Key guardrail** — 8 pre-flight checks must pass; backfills always run one partition per `bruin run` call (bounded blast radius); ranges > 7 days require approval; future-dated backfills are never allowed.

**Hands off to** — `pipeline-report` with full row-count delta.

---

### `schema-drift-check`

**Purpose** — Compare a Bruin asset's declared schema to the live source schema, classify each diff into one of 8 drift types (`column-added`, `column-renamed`, `type-narrowed`, `enum-value-added`, etc.), and propose the minimum-impact fix.

**Use when** — `pipeline-diagnose` hypothesizes schema drift, `bruin validate` warns about drift, or a vendor announces a schema change.

**Don't use for** — wrong values with a correct schema (use `data-quality-investigate`), or proposing new columns nobody asked for.

**Key inputs** — `asset`, optional `suspected_column`.

**Key guardrail** — `type-narrowed` is never auto-actioned (it always breaks consumers); `column-removed` requires approval when downstream count > 0.

**Hands off to** — `maintenance-pr` for most drift types; escalates to `pipeline-report` for narrowings.

---

### `data-quality-investigate`

**Purpose** — When a Bruin custom check or column check fails, pull the offending rows, profile them by partition/dimension, bisect history to find the first-failure date, and classify the failure into one of 9 modes (`source-bug`, `transform-bug`, `late-arriving-data`, `seasonality-miss`, etc.).

**Use when** — a quality check failed on an otherwise successful run, or a downstream consumer reported wrong numbers.

**Don't use for** — schema problems (use `schema-drift-check`), missing data (use `freshness-sla-check`), or designing new checks (that's a human task).

**Key inputs** — `asset`, `check_name`, optional `run_id`.

**Key guardrail** — never deletes failing rows to make a check pass; never silences a check; threshold changes require approval.

**Hands off to** — `maintenance-pr` for transform fixes, `pipeline-backfill` once a fix is merged.

---

### `freshness-sla-check`

**Purpose** — Find assets past their freshness SLA and classify why: upstream stale, source down, scheduler missed a tick, run is stuck, or genuine outage. Resolves SLA from explicit `meta.freshness`, schedule cadence, partition cadence, or historical median (in that order).

**Use when** — scheduled tick (every 15-30 min), triage flagged staleness, after a maintenance window, or a human asks "is the data fresh".

**Don't use for** — failed runs that produced errors (use `pipeline-diagnose`), assets with no SLA at all (those are listed as `unmonitored`).

**Key inputs** — `pipeline` (or `all`), `grace_minutes`.

**Key guardrail** — never marks an asset fresh that has no recent successful run; never fabricates an SLA; can retrigger at most one missed run per asset auto, more requires approval.

**Hands off to** — `pipeline-diagnose` for `attempted-failed` cases, `pipeline-report` for everything else.

---

### `anomaly-investigate`

**Purpose** — Explain a metric spike or dip via dimension slicing and upstream attribution. Distinguishes pipeline causes (double-count, undercount, definitional change) from real-world causes (new segment, seasonality, distribution shift).

**Use when** — a tracked metric is out of expected range but no error or quality check fired, or a human asks "why did X spike on day Y".

**Don't use for** — failed runs, schema problems, freshness issues, or claiming real-world causation (we surface signals, not interpret).

**Key inputs** — `metric` (or `asset.column`), `window`, optional `dimensions`, `baseline` method.

**Key guardrail** — read-only; attribution > 100% is forbidden (a common bug with overlapping dimensions); coverage < 50% must be reported as `anomaly-unexplained`, not stretched.

**Hands off to** — `data-quality-investigate` if pipeline cause attributed, `pipeline-report` otherwise.

---

### `maintenance-pr`

**Purpose** — The only skill with repo write access. Opens a PR for routine, allow-listed maintenance: column rename, type widening, dependency patch bump, dedup-window adjust, dead-code removal. Every PR is traceable to a finding file produced by another skill.

**Use when** — another skill produced a finding with `action: maintenance-pr`, a scheduled tick found an allow-listed task, or a human explicitly requested a routine PR.

**Don't use for** — feature work, refactors, anything that changes pipeline behavior in a user-visible way, or any change without a finding file.

**Key inputs** — `finding_file` (must exist and validate), optional `branch_name`, `draft` (default true).

**Key guardrail** — only allow-listed change types; never merges (humans only); branch scoped to finding's declared files; secrets scan on the diff; CI must run before the PR is considered "open".

**Hands off to** — `pipeline-report` with the PR URL and CI status.

---

### `pipeline-report`

**Purpose** — Post structured Slack messages for status, incidents, escalations, and digests. Consistent shape (subject → what happened → what was done → what needs attention → evidence → suggested follow-up) so a human can pick up cold.

**Use when** — end of every self-healing run (even when nothing was done), a specialist produced an escalation, or a scheduled digest is due.

**Don't use for** — ad-hoc conversation, posting raw query results, or replacing PR review comments.

**Key inputs** — `channel`, `severity` (info/warn/error/critical), `subject`, optional `source_files`, `thread_ts`, `mentions`.

**Key guardrail** — severity is set by the caller, never silently changed; dedups against last hour of channel history (replies in thread instead of double-posting); never includes secrets or full row dumps; never pages someone not on current on-call.

**Hands off to** — nothing; this is always the terminal skill.

## Composition Rules

A few invariants that hold across the whole system:

1. **Every run ends with `pipeline-report`.** Silent runs are forbidden — if nothing was wrong, post an `info` digest line; if nothing was done because approval is required, post a `warn` with the plan.
2. **`pipeline-triage` is the only entry point.** Other skills can be called directly by humans, but scheduled agents should always start from triage so classification is consistent.
3. **Findings flow through `.context/`.** Skills don't pass structured state to each other through memory — they write a file and pass the path. This is what makes the whole chain auditable.
4. **Read-only by default.** A skill that touches state (`pipeline-backfill`, `maintenance-pr`) is the exception, not the rule, and must declare every write in its guardrails section.
5. **One specialist per class.** Triage groups issues by class so each specialist sees a batch of similar problems, not a mixed bag.

## What These Skills Deliberately Don't Cover

- **Feature work** — building new assets, new dashboards, new metrics. That's a human task.
- **Designing new quality checks** — the skills act on checks that exist, they don't author new ones.
- **Capacity / billing decisions** — the agent can detect capacity exhaustion but always escalates.
- **Source-system changes** — we're consumers; we don't push schema changes back to vendors.
- **Merge decisions** — `maintenance-pr` opens PRs as drafts. A human reviews and merges.
- **Real-world causation** — `anomaly-investigate` attributes deltas to dimensions; it doesn't claim to know why the world changed.

## Testing Locally

Three fake-data pipelines exist for exercising the skills end-to-end without touching production:

- `fake-shop/` — orders + products. Injects schema drift, duplicate keys, country-concentration anomaly, freshness gap.
- `fake-iot/` — sensor readings. Injects impossible values, type narrowing, late-arriving data.
- `fake-webevents/` — pageviews. Injects single-dimension anomaly, new browser segment, multi-day freshness gap.

Each pipeline's raw asset file documents the injected issues, their dates, and which skill should detect each one. Run `bruin run <pipeline> --start-date 2026-01-01 --end-date 2026-05-25` to materialize.
