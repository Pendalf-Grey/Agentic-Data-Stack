#!/bin/sh
set -eu

# Скрипт выполняет пакетную загрузку исторических Prometheus-метрик в ClickHouse.
# Он вызывает prometheus-connector /backfill, который читает Prometheus query_range.

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

iso_hours_ago() {
  # Кроссплатформенная дата: macOS использует -v, GNU date использует -d.
  hours="$1"
  if date -u -v-"$hours"H '+%Y-%m-%dT%H:%M:%SZ' >/dev/null 2>&1; then
    date -u -v-"$hours"H '+%Y-%m-%dT%H:%M:%SZ'
  else
    date -u -d "$hours hours ago" '+%Y-%m-%dT%H:%M:%SZ'
  fi
}

iso_now() {
  # Текущий UTC timestamp в ISO-формате для Prometheus API.
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

# По умолчанию забираем последние 72 часа с шагом 60 секунд.
PROMETHEUS_BACKFILL_START=${PROMETHEUS_BACKFILL_START:-$(iso_hours_ago 72)}
PROMETHEUS_BACKFILL_END=${PROMETHEUS_BACKFILL_END:-$(iso_now)}
PROMETHEUS_BACKFILL_STEP=${PROMETHEUS_BACKFILL_STEP:-60s}
PROMETHEUS_BASE_URL=${PROMETHEUS_BASE_URL:-http://host.docker.internal:9095}
PROMETHEUS_IMPORT_SYNTHETIC_HISTORY=${PROMETHEUS_IMPORT_SYNTHETIC_HISTORY:-true}

cd "$ROOT_DIR"

# Если рядом есть prometheus-synthetic-lab, поднимаем его и при необходимости импортируем history.
# Это делает локальный demo красивее: в ClickHouse попадают не пустые/плоские метрики, а история.
if [ -f "$ROOT_DIR/prometheus-synthetic-lab/docker-compose.yml" ]; then
  (
    cd "$ROOT_DIR/prometheus-synthetic-lab"
    if [ "$PROMETHEUS_BASE_URL" = "http://host.docker.internal:9095" ] && [ "$PROMETHEUS_IMPORT_SYNTHETIC_HISTORY" = "true" ]; then
      docker compose down
      HISTORY_HOURS=${PROMETHEUS_HISTORY_HOURS:-72} \
        HISTORY_STEP_SECONDS=${PROMETHEUS_HISTORY_STEP_SECONDS:-60} \
        sh scripts/import-history.sh
    fi
    docker compose up -d --build
  )
fi

export PROMETHEUS_BASE_URL

# Поднимаем ClickHouse, MCP и сам prometheus-connector.
docker compose up -d --build clickhouse mcp-server
docker compose up -d --build --force-recreate prometheus-connector

# Ждем health endpoint connector'а, чтобы не отправить /backfill слишком рано.
for attempt in 1 2 3 4 5 6 7 8 9 10; do
  if curl -fsS http://localhost:3355/health >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 10 ]; then
    echo "prometheus-connector did not become healthy at http://localhost:3355/health" >&2
    exit 1
  fi
  sleep 2
done

# Набор queries подобран под synthetic monitoring lab:
# availability, incidents, HTTP latency/traffic, DB health и scrape health.
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
