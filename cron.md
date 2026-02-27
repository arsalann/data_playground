# Manual Cron Setup for Bruin Pipelines

Reference: [Deploying Bruin on Ubuntu VMs](https://getbruin.com/docs/bruin/deployment/vm-deployment.html)

## Steps

1. Find where bruin is installed by running `which bruin`.
2. Create a `logs/` folder in your project to store run output.
3. Open your crontab with `crontab -e` and add a line for each pipeline you want to schedule.
4. Check it worked with `crontab -l`.
5. To stop a pipeline, open `crontab -e` again and delete the line.

## Crontab Line Format

```
<schedule> /absolute/path/to/bruin run /absolute/path/to/project/<pipeline-name> >> /absolute/path/to/project/logs/<pipeline-name>.log 2>&1
```

Example — run chess-dot-com every hour:

```
0 * * * * /Users/bear/.local/bin/bruin run /Users/bear/Github/data_playground/chess-dot-com >> /Users/bear/Github/data_playground/logs/chess-dot-com.log 2>&1
```

## Common Schedules

| Schedule                | Expression       |
|-------------------------|------------------|
| Every hour              | `0 * * * *`      |
| Every 15 minutes        | `*/15 * * * *`   |
| Daily at 3 AM           | `0 3 * * *`      |
| Weekly Monday 8 AM      | `0 8 * * 1`      |
| Monthly 1st at midnight | `0 0 1 * *`      |
