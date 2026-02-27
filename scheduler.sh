#!/bin/bash
# ============================================================
# Bruin Pipeline Scheduler
#
# Manages cron jobs for Bruin pipelines.
#
# Usage:
#   ./scheduler.sh start   — schedule a pipeline with a cron expression
#   ./scheduler.sh stop    — remove a scheduled pipeline
#   ./scheduler.sh list    — show all scheduled pipelines
#   ./scheduler.sh logs    — tail the latest log for a pipeline
# ============================================================

set -e

# Resolve the project root to the directory this script lives in
PROJECT_PATH="$(cd "$(dirname "$0")" && pwd)"
SCRIPT_PATH="${PROJECT_PATH}/scheduler.sh"
LOG_DIR="${PROJECT_PATH}/logs"

# Tag used to identify our cron entries (appended as a comment to each line)
MARKER="# bruin-scheduler"

# ----------------------------------------------------------
# run <pipeline-name>
#   Called by cron — not meant to be run manually.
#   Pulls latest code, runs the pipeline, writes a timestamped log.
# ----------------------------------------------------------
do_run() {
    PIPELINE_NAME="$1"
    PIPELINE="${PIPELINE_NAME}/pipeline.yml"

    # Cron has a minimal PATH — include where bruin is installed
    export PATH="$HOME/.local/bin:$PATH"

    LOG_FILE="${LOG_DIR}/${PIPELINE_NAME}_$(date +%Y%m%d%H%M).txt"
    mkdir -p "$LOG_DIR"
    cd "$PROJECT_PATH"

    echo "=== Starting ${PIPELINE_NAME} at $(date) ===" >> "$LOG_FILE"

    # Pull latest pipeline definitions before running
    git pull origin main >> "$LOG_FILE" 2>&1

    # --force:       skip confirmation prompts (required for non-interactive cron)
    # --no-log-file: prevent bruin from creating its own log files
    if ! bruin run "$PIPELINE" --force --no-log-file >> "$LOG_FILE" 2>&1; then
        echo "=== FAILED at $(date) ===" >> "$LOG_FILE"
        exit 1
    fi

    echo "=== Completed at $(date) ===" >> "$LOG_FILE"
}

# ----------------------------------------------------------
# start
#   Prompts for a pipeline name and cron schedule, then
#   appends a new entry to the user's crontab.
# ----------------------------------------------------------
do_start() {
    printf "enter pipeline name: "
    read -r PIPELINE_NAME

    printf "enter cron schedule: "
    read -r SCHEDULE

    # Check if this pipeline is already scheduled
    if crontab -l 2>/dev/null | grep -q "${MARKER}:${PIPELINE_NAME}$"; then
        echo "error: ${PIPELINE_NAME} is already scheduled — stop it first"
        exit 1
    fi

    # Append a new cron entry tagged with the pipeline name
    CRON_LINE="${SCHEDULE} ${SCRIPT_PATH} run ${PIPELINE_NAME} ${MARKER}:${PIPELINE_NAME}"
    { crontab -l 2>/dev/null || true; echo "$CRON_LINE"; } | crontab -

    echo "scheduled ${PIPELINE_NAME} with: ${SCHEDULE}"
}

# ----------------------------------------------------------
# stop
#   Prompts for a pipeline name, then removes its cron entry.
# ----------------------------------------------------------
do_stop() {
    printf "enter pipeline name: "
    read -r PIPELINE_NAME

    if ! crontab -l 2>/dev/null | grep -q "${MARKER}:${PIPELINE_NAME}$"; then
        echo "error: ${PIPELINE_NAME} is not scheduled"
        exit 1
    fi

    # Remove the matching line from crontab
    crontab -l | grep -v "${MARKER}:${PIPELINE_NAME}$" | crontab -

    echo "stopped ${PIPELINE_NAME}"
}

# ----------------------------------------------------------
# list
#   Prints all scheduled pipelines and their cron expressions.
# ----------------------------------------------------------
do_list() {
    # Filter crontab to only our tagged lines
    ENTRIES=$(crontab -l 2>/dev/null | grep "${MARKER}:" || true)

    if [ -z "$ENTRIES" ]; then
        echo "no pipelines scheduled"
        return
    fi

    # Parse each line: first 5 fields are the schedule, pipeline name is in the marker
    echo "$ENTRIES" | while read -r line; do
        SCHEDULE=$(echo "$line" | awk '{print $1, $2, $3, $4, $5}')
        NAME=$(echo "$line" | sed "s/.*${MARKER}://")
        echo "${NAME} | ${SCHEDULE}"
    done
}

# ----------------------------------------------------------
# logs
#   Prompts for a pipeline name, then tails the latest log.
# ----------------------------------------------------------
do_logs() {
    printf "enter pipeline name: "
    read -r PIPELINE_NAME

    LATEST=$(ls -t "${LOG_DIR}/${PIPELINE_NAME}_"*.txt 2>/dev/null | head -1)

    if [ -z "$LATEST" ]; then
        echo "no logs found for ${PIPELINE_NAME}"
        exit 1
    fi

    echo "=== ${LATEST} ==="
    cat "$LATEST"
}

# ----------------------------------------------------------
# Route the subcommand
# ----------------------------------------------------------
case "${1:-}" in
    start) do_start ;;
    stop)  do_stop ;;
    list)  do_list ;;
    logs)  do_logs ;;
    run)   do_run "$2" ;;
    *)
        echo "usage: ./scheduler.sh {start|stop|list|logs}"
        exit 1
        ;;
esac
