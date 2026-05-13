#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PROMETHEUS_CONFIG_FILE=${PROMETHEUS_CONFIG_FILE:-"$ROOT_DIR/prometheus-synthetic-lab/prometheus/prometheus.yml"}
PROMETHEUS_REMOTE_WRITE_URL=${PROMETHEUS_REMOTE_WRITE_URL:-"http://host.docker.internal:3355/api/v1/write"}

cd "$ROOT_DIR"

docker compose up -d --build clickhouse prometheus-connector mcp-server

if [ -f "$PROMETHEUS_CONFIG_FILE" ]; then
  if ! grep -q "remote_write:" "$PROMETHEUS_CONFIG_FILE"; then
    echo "No remote_write block found in $PROMETHEUS_CONFIG_FILE" >&2
    echo "Add remote_write url $PROMETHEUS_REMOTE_WRITE_URL or set PROMETHEUS_CONFIG_FILE to the active Prometheus config." >&2
    exit 1
  elif ! grep -Fq "$PROMETHEUS_REMOTE_WRITE_URL" "$PROMETHEUS_CONFIG_FILE"; then
    echo "remote_write exists in $PROMETHEUS_CONFIG_FILE; verify that it points to $PROMETHEUS_REMOTE_WRITE_URL"
  fi
fi

if [ -f "$ROOT_DIR/prometheus-synthetic-lab/docker-compose.yml" ]; then
  (
    cd "$ROOT_DIR/prometheus-synthetic-lab"
    docker compose up -d --build
    docker compose restart prometheus
  )
fi

curl -fsS http://localhost:3355/health
echo
echo "Streaming path is active: Prometheus remote_write -> prometheus-connector -> ClickHouse."
