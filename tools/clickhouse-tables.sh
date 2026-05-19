#!/bin/sh
set -eu

# Скрипт показывает таблицы выбранной БД ClickHouse: engine, rows и размер.
# Он нужен как быстрая проверка "что сейчас лежит в ClickHouse".

# Переходим в корень репозитория, чтобы docker compose работал из любой текущей директории.
ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

env_value() {
  # Читает значение KEY из .env, если оно не передано через окружение.
  key="$1"
  if [ -f .env ]; then
    grep -E "^${key}=" .env | tail -n 1 | cut -d '=' -f 2-
  fi
}

CLICKHOUSE_DB=${CLICKHOUSE_DB:-$(env_value CLICKHOUSE_DB)}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-$(env_value CLICKHOUSE_USER)}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-$(env_value CLICKHOUSE_PASSWORD)}

# Fallback-значения для локального demo-окружения.
CLICKHOUSE_DB=${CLICKHOUSE_DB:-analytics}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-analytics}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-analytics_password}

# Читаем metadata из system.tables. Это быстрее и безопаснее, чем SELECT count() по каждой таблице.
docker compose exec -T clickhouse clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "
    SELECT
      database,
      name AS table,
      engine,
      total_rows AS rows,
      formatReadableSize(total_bytes) AS bytes
    FROM system.tables
    WHERE database = '$CLICKHOUSE_DB'
    ORDER BY database, name
    FORMAT PrettyCompact
  "
