---
name: pipeline-report
description: Post a structured status, incident, or digest message to Slack about pipeline state. Aggregates outputs from other skills into a consistent shape. Use as the final step of every self-healing run so a human always knows what happened.
argument-hint: "<channel> <severity> <subject>"
---

# Pipeline Report

Every self-healing run ends here. If the agent did something — or decided to do nothing — a human needs to be able to read one message and understand what happened, what was fixed, and what still needs attention.

Reports should link to Bruin Cloud runs/assets and source finding files. If the report needs Cloud context, use Bruin Cloud MCP first and docs/source-verified `bruin cloud ... --output json` commands as fallback. `bruin-cloud` is the repo convention for the `.bruin.yml` `bruin` connection, but the CLI cannot select that connection by name; export `BRUIN_CLOUD_API_KEY` when multiple `bruin` connections exist. Never include API tokens, `.bruin.yml` connection values, `BRUIN_CLOUD_API_KEY`, command output containing `api_token`, full row dumps, or raw secrets in Slack.

Slack posting is outside the Bruin CLI. Bruin Cloud docs cover pipeline notification configuration, but this skill posts through the configured Slack integration or MCP connector.

## When to Use

- End of any self-healing run, even when no action was taken.
- A specialist skill produced a finding that needs human attention.
- A scheduled digest is due (daily, weekly).
- An explicit escalation from another skill (`source-down`, `type-narrowed`, `unexplained anomaly`).

Do not use for: ad-hoc human-to-human conversation, posting raw query results without summary, or replacing PR review comments (use the PR itself).

## Inputs

| Input | Required | Example | Notes |
|---|---|---|---|
| `channel` | yes | `#data-pipeline-alerts` | Slack channel name or ID. |
| `severity` | yes | `info`, `warn`, `error`, `critical` | Drives formatting and mentions. |
| `subject` | yes | `Schema drift: wikipedia_pageviews` | One-line summary, max 80 chars. |
| `source_files` | no | `[.context/drift-...yml, .context/diag-...md]` | Finding/report files to summarize. |
| `thread_ts` | no | `1700000000.000100` | If updating an existing thread instead of posting new. |
| `mentions` | no | `[@oncall, @data-eng]` | Used only on `error` and `critical`. |

## Severity → Format

| Severity | Color | Mentions | Posts to | Notes |
|---|---|---|---|---|
| `info` | grey | none | thread or digest channel | Routine "did this thing, all good" messages. |
| `warn` | yellow | none | main alerts channel | Something needs eyes within a day. |
| `error` | orange | oncall | main alerts channel | Active impact; needs eyes within an hour. |
| `critical` | red | oncall + leadership | main alerts channel + page | Customer-visible or data-integrity at risk. |

The skill must never silently upgrade or downgrade severity — the caller picks it.

## Message Structure

Every message follows this shape. Sections marked optional are omitted when empty, not left blank.

```
:<severity-emoji>: *<subject>*
Pipeline: `<pipeline>` · Time: `<UTC timestamp>` · Run: `<skill or alert handle>`

*What happened*
<2-3 sentences. Plain English. No jargon a new oncall would not recognize.>

*What was done*  (optional — omit if no action was taken)
- <bullet per concrete action>
- Include PR URLs, Bruin Cloud run IDs/URLs, and affected intervals.

*What needs attention*  (optional)
- <bullet per item requiring a human>
- Each item names the person, team, or skill that should follow up.

*Evidence*
- Finding: <link to .context/ file via Slack attachment or repo URL>
- Diagnosis: <link>
- Run logs: <link to Bruin Cloud run>

*Suggested follow-up*  (optional)
- One sentence. The next sensible action, not a list of options.
```

Critical formatting rules:

- Lead with the subject, not "Hi team" or any greeting.
- Never use phrases like "I noticed", "it appears", "looks like". State what is true and what is uncertain, with evidence.
- Quote numbers, not vibes. "Row count dropped 47% (4.2M → 2.2M)" not "row count is way down".
- Link, do not paste. Long error messages go in a thread reply, not the main message.
- No emojis except the severity indicator.

## Cloud Context Commands

When source files do not already contain reliable Cloud IDs, source run IDs from `runs get/list` and asset names from `assets list/get` rather than free-form text:

```shell
bruin cloud runs list --project-id <project-id> --pipeline <pipeline> --limit 20 --output json
bruin cloud runs get --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json
bruin cloud assets list --project-id <project-id> --pipeline <pipeline> --output json
bruin cloud assets get --project-id <project-id> --pipeline <pipeline> --asset <asset> --output json
bruin cloud instances failed-logs --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json
```

Redact `.bruin.yml` values, `BRUIN_CLOUD_API_KEY`, any `api_token` fields, connection strings, credential file paths, and row-level samples before constructing Slack content.

## Digest Messages

For scheduled digests (daily/weekly), use this structure instead:

```
*Pipeline digest — <window>*
Pipelines scanned: N · Healthy: N · With issues: N

*Resolved this <window>*
- <one line per resolved issue, with link to PR or run>

*Open*
- <one line per still-open issue, with owner and ETA>

*Recurring patterns*  (optional, only if 2+ similar issues)
- <pattern description and suggested systemic fix>
```

Digests should be info or warn severity only — anything critical should already have been posted at the time of detection.

## Decision Tree

```
inputs = validate(input)
if not inputs.valid:
    return abort(inputs.errors)

# Load and summarize source files.
summaries = []
for file in inputs.source_files or []:
    parsed = load(file)
    summaries.append(summarize_finding(parsed))

# Build message body.
body = build_message(
    severity=inputs.severity,
    subject=inputs.subject,
    summaries=summaries,
    mentions=resolve_mentions(inputs.severity, inputs.mentions),
)

# Dedup: have we posted essentially-this-message in the last hour?
recent = search_recent_messages(inputs.channel, window='1h')
if matches_existing(body, recent):
    return reply_in_thread(recent.match, body, prefix='Repeat detection:')

posted = slack_send(channel=inputs.channel, body=body, thread_ts=inputs.thread_ts)

# Update finding files with the message URL so future skills can link back.
for file in inputs.source_files or []:
    annotate(file, posted_at=now, message_url=posted.permalink)

return result(message_url=posted.permalink, channel=inputs.channel)
```

## Actions & Guardrails

- **Auto-allowed**: posting `info` and `warn` messages, replying in existing threads, annotating finding files with the message URL.
- **Requires approval**: posting `critical` severity outside business hours when the pipeline being reported on is not on the on-call rotation list.
- **Never allowed**: posting to channels not on the allow-list, paging individuals who are not on the current on-call rotation, posting message bodies that include secrets or full row dumps, suppressing a report because "the previous one looked similar" (always reply in thread instead — never drop).

## Verification

A report is complete when:

1. The Slack message returned a permalink.
2. Source finding files were annotated with the permalink.
3. If `severity >= error`, an on-call acknowledgment is expected within the channel's SLA — track but do not enforce; humans handle escalation from there.

## Reporting About Reporting

Yes, even this skill writes a record. Append to `.context/reports-<YYYY-MM-DD>.jsonl`:

```json
{"ts":"2026-05-22T14:31:00Z","channel":"#data-pipeline-alerts","severity":"warn","subject":"Schema drift: wikipedia_pageviews","permalink":"https://...","sources":[".context/drift-raw.wikipedia_pageviews-20260522.yml"]}
```

This lets the agent answer "have we already told someone about this" without scanning Slack history every time.
