#!/bin/sh
set -eu

if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

SQL_FILE="${1:?Usage: tools/run_sql.sh path/to/query.sql}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-analytics}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-analytics}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-analytics_password}"

QUERY_STRING="$(
  python3 - <<'PY'
import os
from pathlib import Path
from urllib.parse import urlencode

params = {"database": os.getenv("CLICKHOUSE_DB", "analytics")}
defaults = {
    "MAP_PROMPT_NAME": "map_compressed_logs_en",
}

def clickhouse_param(value):
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\t", "\\t")

env_to_param = {
    "INVESTIGATION_ID": "param_investigation_id",
    "USER_QUESTION": "param_user_question",
    "TIME_FROM": "param_time_from",
    "TIME_TO": "param_time_to",
    "LOGS_SOURCE_NAME": "param_source_name",
    "LOGS_INDEX_LIKE": "param_index_like",
    "MAP_PROMPT_NAME": "param_map_prompt_name",
}
for env_key, param_key in env_to_param.items():
    value = os.getenv(env_key) or defaults.get(env_key)
    if value:
        params[param_key] = clickhouse_param(value)
map_system_prompt = os.getenv("MAP_SYSTEM_PROMPT")
if not map_system_prompt:
    prompt_file = Path(os.getenv("MAP_PROMPT_FILE", "prompts/map_compressed_logs.en.txt"))
    if prompt_file.exists():
        map_system_prompt = prompt_file.read_text(encoding="utf-8").strip()
if map_system_prompt:
    params["param_map_system_prompt"] = clickhouse_param(map_system_prompt)
print(urlencode(params))
PY
)"

curl -fsS "$CLICKHOUSE_URL/?$QUERY_STRING" \
  -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
  --data-binary @"$SQL_FILE"
