#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

read_env_value() {
  awk -v key="$1" '
    /^[[:space:]]*($|#)/ { next }
    {
      line = $0
      sub(/^[[:space:]]+/, "", line)
      if (index(line, key "=") == 1) {
        value = substr(line, length(key) + 2)
        sub(/[[:space:]]+#.*$/, "", value)
        if ((value ~ /^".*"$/) || (value ~ /^\047.*\047$/)) {
          value = substr(value, 2, length(value) - 2)
        }
        print value
        exit
      }
    }
  ' .env
}

load_env_default() {
  key="$1"
  eval "current=\${$key:-}"
  if [ -z "$current" ] && [ -f .env ]; then
    value="$(read_env_value "$key")"
    if [ -n "$value" ]; then
      export "$key=$value"
    fi
  fi
}

for key in \
  CLICKHOUSE_URL \
  CLICKHOUSE_DB \
  CLICKHOUSE_USER \
  CLICKHOUSE_PASSWORD \
  LOGS_SOURCE_NAME \
  ELASTICSEARCH_SOURCE_NAME \
  LOGS_INDEX_LIKE \
  ADS_LLM_LOG_INDEX_LIKE \
  LOG_COMPRESS_BATCH_ROWS \
  LOG_COMPRESS_PROGRESS_EVERY
do
  load_env_default "$key"
done

sh tools/run_sql.sh clickhouse/init/005_compressed_batch_cursor.sql >/dev/null
sh tools/run_sql.sh clickhouse/init/006_create_map_batch_inputs.sql >/dev/null

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
