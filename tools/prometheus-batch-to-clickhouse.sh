#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

iso_hours_ago() {
  hours="$1"
  if date -u -v-"$hours"H '+%Y-%m-%dT%H:%M:%SZ' >/dev/null 2>&1; then
    date -u -v-"$hours"H '+%Y-%m-%dT%H:%M:%SZ'
  else
    date -u -d "$hours hours ago" '+%Y-%m-%dT%H:%M:%SZ'
  fi
}

iso_now() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

PROMETHEUS_BACKFILL_START=${PROMETHEUS_BACKFILL_START:-$(iso_hours_ago 72)}
PROMETHEUS_BACKFILL_END=${PROMETHEUS_BACKFILL_END:-$(iso_now)}
PROMETHEUS_BACKFILL_STEP=${PROMETHEUS_BACKFILL_STEP:-60s}

cd "$ROOT_DIR"

docker compose up -d --build clickhouse prometheus-connector mcp-server

curl -fsS http://localhost:3355/backfill \
  -H 'Content-Type: application/json' \
  -d "{
    \"queries\": [
      \"synthetic_service_up\",
      \"synthetic_incident_active\",
      \"synthetic_log_events_total\",
      \"synthetic_http_requests_total\",
      \"synthetic_http_request_duration_seconds_p95\",
      \"synthetic_db_connections\",
      \"synthetic_db_query_duration_seconds_p95\",
      \"synthetic_db_replication_lag_seconds\",
      \"synthetic_db_disk_usage_ratio\",
      \"synthetic_process_restarts_total\",
      \"up\"
    ],
    \"start\": \"$PROMETHEUS_BACKFILL_START\",
    \"end\": \"$PROMETHEUS_BACKFILL_END\",
    \"step\": \"$PROMETHEUS_BACKFILL_STEP\"
  }"
echo
