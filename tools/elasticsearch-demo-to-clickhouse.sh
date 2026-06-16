#!/bin/sh
set -eu

# Скрипт поднимает локальный Elasticsearch demo, загружает synthetic logs
# и переносит их в ClickHouse через elasticsearch-connector batch endpoint.

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
LAB_DIR="$ROOT_DIR/elasticsearch-synthetic-lab"
BULK_FILE=${ELASTICSEARCH_DEMO_BULK_FILE:-"$LAB_DIR/data/synthetic-logs.bulk.ndjson"}
META_FILE=${ELASTICSEARCH_DEMO_META_FILE:-"$LAB_DIR/data/synthetic-logs.meta.json"}
ES_PUBLIC_URL=${ELASTICSEARCH_PUBLIC_URL:-http://localhost:9200}
ES_CONTAINER_URL=${ELASTICSEARCH_BASE_URL:-http://elasticsearch:9200}
ELASTICSEARCH_DEMO_INDEX_PREFIX=${ELASTICSEARCH_DEMO_INDEX_PREFIX:-nginx-logs}
ELASTICSEARCH_DEMO_CLEAR=${ELASTICSEARCH_DEMO_CLEAR:-true}
ELASTICSEARCH_DEMO_REGENERATE=${ELASTICSEARCH_DEMO_REGENERATE:-true}
ELASTICSEARCH_BATCH_SIZE=${ELASTICSEARCH_BATCH_SIZE:-1000}

cd "$ROOT_DIR"

mkdir -p "$LAB_DIR/data/elasticsearch"

# Локальный Elasticsearch живет в compose profile, чтобы не грузить стек без необходимости.
docker compose --profile elasticsearch up -d elasticsearch

for attempt in $(seq 1 60); do
  if curl -fsS "$ES_PUBLIC_URL/_cluster/health" >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 60 ]; then
    echo "Elasticsearch did not become healthy at $ES_PUBLIC_URL" >&2
    exit 1
  fi
  sleep 2
done

if [ "$ELASTICSEARCH_DEMO_REGENERATE" = "true" ]; then
  mkdir -p "$(dirname "$BULK_FILE")"
  OUTPUT="$BULK_FILE" \
  ELASTICSEARCH_DEMO_INDEX_PREFIX="$ELASTICSEARCH_DEMO_INDEX_PREFIX" \
  python3 "$LAB_DIR/scripts/generate_bulk.py" > "$META_FILE"
fi

if [ ! -f "$BULK_FILE" ] || [ ! -f "$META_FILE" ]; then
  echo "Static Elasticsearch fixture is missing: $BULK_FILE / $META_FILE" >&2
  echo "Run ELASTICSEARCH_DEMO_REGENERATE=true sh tools/elasticsearch-demo-to-clickhouse.sh to rebuild it." >&2
  exit 1
fi

if [ "$ELASTICSEARCH_DEMO_CLEAR" = "true" ]; then
  for existing_index in $(curl -fsS "$ES_PUBLIC_URL/_cat/indices/$ELASTICSEARCH_DEMO_INDEX_PREFIX-*?h=index&s=index" 2>/dev/null || true); do
    curl -fsS -X DELETE "$ES_PUBLIC_URL/$existing_index" >/dev/null 2>&1 || true
  done
  for index_name in $(python3 - "$META_FILE" <<'PY'
import json
import sys

meta = json.load(open(sys.argv[1], encoding="utf-8"))
for index_name in sorted((meta.get("indices") or {}).keys()):
    print(index_name)
PY
); do
    curl -fsS -X DELETE "$ES_PUBLIC_URL/$index_name" >/dev/null 2>&1 || true
  done
fi

bulk_part_dir="$LAB_DIR/data/elasticsearch/bulk-parts"
rm -rf "$bulk_part_dir"
mkdir -p "$bulk_part_dir"
split -l "$((ELASTICSEARCH_BATCH_SIZE * 2))" -d -a 6 "$BULK_FILE" "$bulk_part_dir/part-"

bulk_items=0
bulk_response="$LAB_DIR/data/elasticsearch/bulk-response.json"
for bulk_part in "$bulk_part_dir"/part-*; do
  curl -fsS \
    -H 'Content-Type: application/x-ndjson' \
    --data-binary "@$bulk_part" \
    "$ES_PUBLIC_URL/_bulk?refresh=false" \
    -o "$LAB_DIR/data/elasticsearch/bulk-response-part.json"

  part_items=$(python3 - "$LAB_DIR/data/elasticsearch/bulk-response-part.json" <<'PY'
import json
import sys

path = sys.argv[1]
payload = json.load(open(path, encoding="utf-8"))
if payload.get("errors"):
    errors = []
    for item in payload.get("items", []):
        action = item.get("index") or item.get("create") or item.get("update") or item.get("delete") or {}
        if "error" in action:
            errors.append(action["error"])
        if len(errors) >= 3:
            break
    raise SystemExit(json.dumps({"bulkErrors": errors}, ensure_ascii=False, indent=2))
print(len(payload.get("items", [])))
PY
)
  bulk_items=$((bulk_items + part_items))
done
curl -fsS -X POST "$ES_PUBLIC_URL/$ELASTICSEARCH_DEMO_INDEX_PREFIX-*/_refresh" >/dev/null
printf '{"bulkErrors":false,"items":%s}\n' "$bulk_items" | tee "$bulk_response"

ELASTICSEARCH_BATCH_START=$(python3 - "$META_FILE" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1], encoding="utf-8"))["start"])
PY
)
ELASTICSEARCH_BATCH_END=$(python3 - "$META_FILE" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1], encoding="utf-8"))["end"])
PY
)
ELASTICSEARCH_INDEX_PATTERN="$ELASTICSEARCH_DEMO_INDEX_PREFIX-*"

export ELASTICSEARCH_BASE_URL="$ES_CONTAINER_URL"
export ELASTICSEARCH_USER=
export ELASTICSEARCH_PASSWORD=
export ELASTICSEARCH_BEARER_TOKEN=
export ELASTICSEARCH_INDEX_PATTERN
export ELASTICSEARCH_BATCH_START
export ELASTICSEARCH_BATCH_END
export ELASTICSEARCH_BATCH_SIZE
export ELASTICSEARCH_SOURCE_NAME=${ELASTICSEARCH_SOURCE_NAME:-elasticsearch-demo}

CLICKHOUSE_USER=${CLICKHOUSE_USER:-analytics}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-analytics_password}
CLICKHOUSE_DB=${CLICKHOUSE_DB:-analytics}
CLICKHOUSE_ELASTICSEARCH_TABLE=${CLICKHOUSE_ELASTICSEARCH_TABLE:-elasticsearch_events_raw}

docker compose up -d clickhouse mcp-clickhouse

if [ "$ELASTICSEARCH_DEMO_CLEAR" = "true" ]; then
  docker compose exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --query "
      ALTER TABLE \`$CLICKHOUSE_DB\`.\`$CLICKHOUSE_ELASTICSEARCH_TABLE\`
      DELETE WHERE source_name = '$ELASTICSEARCH_SOURCE_NAME'
        AND index_name LIKE '$ELASTICSEARCH_DEMO_INDEX_PREFIX-%'
      SETTINGS mutations_sync = 1
    "
fi

sh "$ROOT_DIR/tools/elasticsearch-batch-to-clickhouse.sh"

docker compose exec -T clickhouse clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "
    SELECT
      index_name,
      count() AS documents,
      min(event_time) AS first_event_time,
      max(event_time) AS last_event_time
    FROM \`$CLICKHOUSE_DB\`.\`$CLICKHOUSE_ELASTICSEARCH_TABLE\`
    WHERE source_name = '$ELASTICSEARCH_SOURCE_NAME'
      AND index_name LIKE '$ELASTICSEARCH_DEMO_INDEX_PREFIX-%'
    GROUP BY index_name
    ORDER BY index_name
    FORMAT PrettyCompact
  "
