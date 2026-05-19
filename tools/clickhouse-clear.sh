#!/bin/sh
set -eu

# Скрипт очищает все обычные таблицы в выбранной БД ClickHouse.
# View не трогаются, потому что они не хранят данные сами по себе.

# Переходим в корень репозитория, чтобы docker compose всегда видел правильный compose-файл.
ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

env_value() {
  # Читает значение KEY из локального .env, если переменная не передана через окружение.
  key="$1"
  if [ -f .env ]; then
    grep -E "^${key}=" .env | tail -n 1 | cut -d '=' -f 2-
  fi
}

# CLICKHOUSE_CLEAR_DATABASE позволяет явно очистить не основную CLICKHOUSE_DB.
CLICKHOUSE_DB=${CLICKHOUSE_CLEAR_DATABASE:-${CLICKHOUSE_DB:-$(env_value CLICKHOUSE_DB)}}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-$(env_value CLICKHOUSE_USER)}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-$(env_value CLICKHOUSE_PASSWORD)}

# Fallback-значения совпадают с локальным demo-проектом.
CLICKHOUSE_DB=${CLICKHOUSE_DB:-analytics}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-analytics}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-analytics_password}

# Защита от случайной очистки системных БД ClickHouse.
case "$CLICKHOUSE_DB" in
  system|INFORMATION_SCHEMA|information_schema)
    echo "Refusing to clear protected ClickHouse database: $CLICKHOUSE_DB" >&2
    exit 1
    ;;
esac

# Забираем список mutable-таблиц. View исключаем, потому что TRUNCATE к ним неприменим.
tables=$(docker compose exec -T clickhouse clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "
    SELECT name
    FROM system.tables
    WHERE database = '$CLICKHOUSE_DB'
      AND engine NOT LIKE '%View'
    ORDER BY name
    FORMAT TSV
  ")

if [ -z "$tables" ]; then
  echo "No mutable tables found in ClickHouse database $CLICKHOUSE_DB."
  exit 0
fi

# Очищаем каждую найденную таблицу отдельной TRUNCATE-командой.
for table in $tables; do
  docker compose exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --query "TRUNCATE TABLE \`$CLICKHOUSE_DB\`.\`$table\`"
  echo "Truncated $CLICKHOUSE_DB.$table"
done
