#!/usr/bin/env bash
set -euo pipefail

WATCHDOG_PID_FILE="/tmp/mzad-backend-watchdog.pid"

if [[ -f "$WATCHDOG_PID_FILE" ]]; then
  pid="$(cat "$WATCHDOG_PID_FILE" || true)"
  if [[ -n "${pid}" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "watchdog: running (pid=$pid)"
  else
    echo "watchdog: not running (stale pid file)"
  fi
else
  echo "watchdog: not running"
fi

if curl -fsS "http://127.0.0.1:8090/api/summary" > /dev/null; then
  echo "backend: healthy on http://127.0.0.1:8090"
else
  echo "backend: not healthy on http://127.0.0.1:8090"
fi
