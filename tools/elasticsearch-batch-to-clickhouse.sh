#!/bin/sh
set -eu

# Скрипт выполняет batch-миграцию документов из Elasticsearch в ClickHouse.
# Он поднимает elasticsearch-connector и вызывает endpoint /batch за заданный интервал.

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
  # Текущий UTC timestamp в ISO-формате для Elasticsearch range query.
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

ELASTICSEARCH_BATCH_START=${ELASTICSEARCH_BATCH_START:-$(iso_hours_ago 24)}
ELASTICSEARCH_BATCH_END=${ELASTICSEARCH_BATCH_END:-$(iso_now)}
ELASTICSEARCH_INDEX_PATTERN=${ELASTICSEARCH_INDEX_PATTERN:-logs-*}
ELASTICSEARCH_BATCH_SIZE=${ELASTICSEARCH_BATCH_SIZE:-1000}

cd "$ROOT_DIR"

# Поднимаем ClickHouse, официальный ClickHouse MCP и HTTP-режим elasticsearch-connector.
docker compose up -d clickhouse mcp-clickhouse

# На чистом volume эти SQL уже выполняет ClickHouse init. На старом volume init
# пропускается, поэтому перед импортом явно доприменяем нужную demo-схему.
sh tools/run_sql.sh clickhouse/init/001_schema.sql
sh tools/run_sql.sh clickhouse/init/003_ads2_mapreduce.sql
sh tools/run_sql.sh clickhouse/init/004_drop_legacy_llm_refinement.sql
sh tools/run_sql.sh clickhouse/init/005_compressed_batch_cursor.sql

docker compose up -d --build --force-recreate elasticsearch-connector

# Ждем health endpoint connector'а, чтобы /batch не ушел слишком рано.
for attempt in 1 2 3 4 5 6 7 8 9 10; do
  if curl -fsS http://localhost:3366/health >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 10 ]; then
    echo "elasticsearch-connector did not become healthy at http://localhost:3366/health" >&2
    exit 1
  fi
  sleep 2
done

# Запускаем batch-миграцию за интервал.
curl -fsS http://localhost:3366/batch \
  -H 'Content-Type: application/json' \
  -d "{
    \"index_pattern\": \"$ELASTICSEARCH_INDEX_PATTERN\",
    \"start\": \"$ELASTICSEARCH_BATCH_START\",
    \"end\": \"$ELASTICSEARCH_BATCH_END\",
    \"batch_size\": $ELASTICSEARCH_BATCH_SIZE
  }"
echo
