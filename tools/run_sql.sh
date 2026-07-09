#!/bin/sh
set -eu

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
  if [ -z "$current" ]; then
    value="$(read_env_value "$key")"
    if [ -n "$value" ]; then
      export "$key=$value"
    fi
  fi
}

if [ -f .env ]; then
  for key in \
    CLICKHOUSE_URL \
    CLICKHOUSE_DB \
    CLICKHOUSE_USER \
    CLICKHOUSE_PASSWORD \
    INVESTIGATION_ID \
    USER_QUESTION \
    TIME_FROM \
    TIME_TO \
    LOGS_SOURCE_NAME \
    LOGS_INDEX_LIKE \
    MAP_PROMPT_NAME \
    MAP_SYSTEM_PROMPT \
    MAP_PROMPT_FILE
  do
    load_env_default "$key"
  done
fi

SQL_FILE="${1:?Usage: tools/run_sql.sh path/to/query.sql}"
export SQL_FILE
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
sql_text = Path(os.environ["SQL_FILE"]).read_text(encoding="utf-8")
defaults = {
    "MAP_PROMPT_NAME": "map_compressed_logs_en",
}

def needs_param(name):
    return "{" + name + ":" in sql_text

def clickhouse_param(value):
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\t", "\\t")

env_to_param = {
    "INVESTIGATION_ID": ("investigation_id", "param_investigation_id"),
    "USER_QUESTION": ("user_question", "param_user_question"),
    "TIME_FROM": ("time_from", "param_time_from"),
    "TIME_TO": ("time_to", "param_time_to"),
    "LOGS_SOURCE_NAME": ("source_name", "param_source_name"),
    "LOGS_INDEX_LIKE": ("index_like", "param_index_like"),
    "MAP_PROMPT_NAME": ("map_prompt_name", "param_map_prompt_name"),
}
for env_key, (placeholder_name, param_key) in env_to_param.items():
    if not needs_param(placeholder_name):
        continue
    value = os.getenv(env_key) or defaults.get(env_key)
    if value:
        params[param_key] = clickhouse_param(value)
if needs_param("map_system_prompt"):
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

export QUERY_STRING CLICKHOUSE_URL CLICKHOUSE_USER CLICKHOUSE_PASSWORD
python3 - <<'PY'
import base64
import os
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

def split_sql(script):
    statements = []
    buffer = []
    quote = None
    escaped = False
    for char in script:
        buffer.append(char)
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in ("'", '"', "`"):
            quote = char
        elif char == ";":
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)
    return statements

sql_file = Path(os.environ["SQL_FILE"])
statements = split_sql(sql_file.read_text(encoding="utf-8"))
if not statements:
    sys.exit(0)

base_url = os.environ["CLICKHOUSE_URL"].rstrip("/")
url = f"{base_url}/?{os.environ['QUERY_STRING']}"
credentials = f"{os.environ['CLICKHOUSE_USER']}:{os.environ['CLICKHOUSE_PASSWORD']}"
auth = base64.b64encode(credentials.encode("utf-8")).decode("ascii")

for statement in statements:
    request = Request(
        url,
        data=(statement.rstrip() + "\n").encode("utf-8"),
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "text/plain; charset=utf-8",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=60) as response:
            body = response.read()
            if body:
                sys.stdout.buffer.write(body)
    except HTTPError as error:
        sys.stderr.write(error.read().decode("utf-8", errors="replace"))
        raise SystemExit(error.code)
    except URLError as error:
        raise SystemExit(f"ClickHouse request failed: {error.reason}")
PY
