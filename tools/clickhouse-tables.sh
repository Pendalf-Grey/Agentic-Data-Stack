#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

CLICKHOUSE_DB=${CLICKHOUSE_DB:-analytics}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-analytics}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-analytics_password}

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
