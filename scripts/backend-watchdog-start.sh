#!/usr/bin/env bash
set -euo pipefail

WATCHDOG="/home/briyad/mzad-db-ui/scripts/backend-watchdog.sh"
WATCHDOG_PID_FILE="/tmp/mzad-backend-watchdog.pid"
WATCHDOG_LOG="/tmp/mzad-backend-watchdog.log"

if [[ -f "$WATCHDOG_PID_FILE" ]]; then
  pid="$(cat "$WATCHDOG_PID_FILE" || true)"
  if [[ -n "${pid}" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "watchdog already running (pid=$pid)"
    exit 0
  fi
fi

nohup bash "$WATCHDOG" >> "$WATCHDOG_LOG" 2>&1 < /dev/null &
pid="$!"
echo "$pid" > "$WATCHDOG_PID_FILE"
echo "watchdog started (pid=$pid)"
