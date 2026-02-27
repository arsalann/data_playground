# Scheduling Bruin Pipelines with Cron

A wrapper around cron for scheduling Bruin pipeline runs. The `scheduler.sh` script manages cron entries, and when cron triggers a job it calls back into the same script to pull the latest code, run the pipeline via `bruin run`, and write a timestamped log file.

Reference: [Deploying Bruin on Ubuntu VMs](https://getbruin.com/docs/bruin/deployment/vm-deployment.html)

## Setup

Make the script executable. Only needed once.

```bash
chmod +x scheduler.sh
```

## Schedule a Pipeline

Adds a cron job that will run the given pipeline on the specified schedule. The script validates that the pipeline isn't already scheduled before adding it.

```bash
./scheduler.sh start
```

```
enter pipeline name: chess-dot-com
enter cron schedule: 0 * * * *
scheduled chess-dot-com with: 0 * * * *
```

## Stop a Pipeline

Removes the cron job for the given pipeline. The pipeline will no longer run on its schedule, but existing logs are preserved.

```bash
./scheduler.sh stop
```

```
enter pipeline name: chess-dot-com
stopped chess-dot-com
```

## List Scheduled Pipelines

Shows all pipelines currently registered in cron with their schedules.

```bash
./scheduler.sh list
```

```
chess-dot-com | 0 * * * *
```

## View Latest Logs

Prints the contents of the most recent log file for a given pipeline. Each cron run writes to a separate file in `logs/` named `<pipeline-name>_<yyyymmddhhmm>.txt`.

```bash
./scheduler.sh logs
```

```
enter pipeline name: chess-dot-com
=== logs/chess-dot-com_202602271400.txt ===
...
```

## How It Works

When cron fires, it calls `scheduler.sh run <pipeline-name>` which:

1. Exports `$HOME/.local/bin` to `PATH` so cron can find the `bruin` binary.
2. Runs `git pull` to fetch the latest pipeline definitions.
3. Runs `bruin run <pipeline>/pipeline.yml --force --no-log-file`.
4. Writes all output to `logs/<pipeline-name>_<yyyymmddhhmm>.txt`.

## Common Cron Schedules

| Schedule                | Expression       |
|-------------------------|------------------|
| Every hour              | `0 * * * *`      |
| Every 15 minutes        | `*/15 * * * *`   |
| Daily at 3 AM           | `0 3 * * *`      |
| Weekly Monday 8 AM      | `0 8 * * 1`      |
| Monthly 1st at midnight | `0 0 1 * *`      |
