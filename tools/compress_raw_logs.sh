#!/bin/sh
set -eu

if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

python3 tools/compress_raw_logs.py \
  --clickhouse-url "${CLICKHOUSE_URL:-http://localhost:8123}" \
  --database "${CLICKHOUSE_DB:-analytics}" \
  --user "${CLICKHOUSE_USER:-analytics}" \
  --password "${CLICKHOUSE_PASSWORD:-analytics_password}" \
  --source-name "${LOGS_SOURCE_NAME:-${ELASTICSEARCH_SOURCE_NAME:-elasticsearch-demo}}" \
  --index-like "${LOGS_INDEX_LIKE:-${ADS_LLM_LOG_INDEX_LIKE:-nginx-logs-%}}" \
  --batch-rows "${LOG_COMPRESS_BATCH_ROWS:-5000}" \
  --progress-every "${LOG_COMPRESS_PROGRESS_EVERY:-100}" \
  "$@"
