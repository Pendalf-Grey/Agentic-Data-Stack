#!/bin/sh
set -eu

if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

python3 tools/import_raw_logs.py "${LOGS_FILE:-/Users/subbotaevgenij/ES_raw_logs_json}" \
  --input-format "${LOGS_INPUT_FORMAT:-auto}" \
  --clickhouse-url "${CLICKHOUSE_URL:-http://localhost:8123}" \
  --database "${CLICKHOUSE_DB:-analytics}" \
  --user "${CLICKHOUSE_USER:-analytics}" \
  --password "${CLICKHOUSE_PASSWORD:-analytics_password}" \
  --source-name "${LOGS_SOURCE_NAME:-elasticsearch-synthetic}" \
  --index-name "${LOGS_INDEX_NAME:-synthetic-logs}" \
  --batch-size "${LOGS_BATCH_SIZE:-10000}" \
  --progress-every "${LOGS_PROGRESS_EVERY:-100000}"
