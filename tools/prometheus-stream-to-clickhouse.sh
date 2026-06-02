#!/bin/sh
set -eu

# Скрипт включает потоковый путь Prometheus remote_write -> prometheus-connector -> ClickHouse.
# Он не делает исторический backfill; для истории есть prometheus-batch-to-clickhouse.sh.

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PROMETHEUS_CONFIG_FILE=${PROMETHEUS_CONFIG_FILE:-"$ROOT_DIR/prometheus-synthetic-lab/prometheus/prometheus.yml"}
PROMETHEUS_REMOTE_WRITE_URL=${PROMETHEUS_REMOTE_WRITE_URL:-"http://host.docker.internal:3355/api/v1/write"}

cd "$ROOT_DIR"

# Поднимаем минимальные сервисы, нужные для приема remote_write и дальнейшего анализа.
docker compose up -d clickhouse mcp-clickhouse
docker compose up -d --build prometheus-connector

# Если задан prometheus.yml, проверяем, что в нем есть remote_write.
# Скрипт не переписывает чужой Prometheus config автоматически, чтобы не сломать внешний мониторинг.
if [ -f "$PROMETHEUS_CONFIG_FILE" ]; then
  if ! grep -q "remote_write:" "$PROMETHEUS_CONFIG_FILE"; then
    echo "No remote_write block found in $PROMETHEUS_CONFIG_FILE" >&2
    echo "Add remote_write url $PROMETHEUS_REMOTE_WRITE_URL or set PROMETHEUS_CONFIG_FILE to the active Prometheus config." >&2
    exit 1
  elif ! grep -Fq "$PROMETHEUS_REMOTE_WRITE_URL" "$PROMETHEUS_CONFIG_FILE"; then
    echo "remote_write exists in $PROMETHEUS_CONFIG_FILE; verify that it points to $PROMETHEUS_REMOTE_WRITE_URL"
  fi
fi

# Для локальной synthetic lab дополнительно поднимаем Prometheus и перезапускаем его,
# чтобы он перечитал remote_write configuration.
if [ -f "$ROOT_DIR/prometheus-synthetic-lab/docker-compose.yml" ]; then
  (
    cd "$ROOT_DIR/prometheus-synthetic-lab"
    docker compose up -d --build
    docker compose restart prometheus
  )
fi

# Healthcheck показывает, что connector жив и куда пишет данные.
curl -fsS http://localhost:3355/health
echo
echo "Streaming path is active: Prometheus remote_write -> prometheus-connector -> ClickHouse."
