#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

CLICKHOUSE_DB=${CLICKHOUSE_CLEAR_DATABASE:-${CLICKHOUSE_DB:-analytics}}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-analytics}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-analytics_password}

case "$CLICKHOUSE_DB" in
  system|INFORMATION_SCHEMA|information_schema)
    echo "Refusing to clear protected ClickHouse database: $CLICKHOUSE_DB" >&2
    exit 1
    ;;
esac

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

for table in $tables; do
  docker compose exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --query "TRUNCATE TABLE \`$CLICKHOUSE_DB\`.\`$table\`"
  echo "Truncated $CLICKHOUSE_DB.$table"
done
