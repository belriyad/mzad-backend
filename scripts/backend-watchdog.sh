#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/briyad/mzad-db-ui"
SERVER="$APP_DIR/server.py"
HOST="0.0.0.0"
PORT="8090"

WATCHDOG_LOG="/tmp/mzad-backend-watchdog.log"
SERVER_LOG="/tmp/mzad-server.log"
SERVER_PID_FILE="/tmp/mzad-server.pid"

timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log() {
  printf "[%s] %s\n" "$(timestamp)" "$1" >> "$WATCHDOG_LOG"
}

is_backend_healthy() {
  curl -fsS "http://127.0.0.1:${PORT}/api/summary" > /dev/null
}

start_server() {
  log "Starting backend server on ${HOST}:${PORT}"
  nohup python3 "$SERVER" --host "$HOST" --port "$PORT" >> "$SERVER_LOG" 2>&1 < /dev/null &
  echo "$!" > "$SERVER_PID_FILE"
  sleep 2
}

log "Watchdog started"
while true; do
  if is_backend_healthy; then
    sleep 20
    continue
  fi
  start_server
done
