#!/bin/sh
set -eu

connect_url="${CONNECT_URL:-http://debezium:8083}"

echo "Waiting for Kafka Connect at ${connect_url}..."
until curl -fsS "${connect_url}/connectors" >/dev/null; do
  sleep 5
done

for file in /connectors/postgres-source.json /connectors/clickhouse-sink.json; do
  name="$(sed -n 's/.*"name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$file" | head -1)"

  if [ -z "$name" ]; then
    echo "Cannot read connector name from ${file}" >&2
    exit 1
  fi

  if curl -fsS "${connect_url}/connectors/${name}" >/dev/null; then
    echo "Connector ${name} already exists"
    continue
  fi

  echo "Registering connector ${name}"
  curl -fsS \
    -X POST "${connect_url}/connectors" \
    -H "Content-Type: application/json" \
    --data-binary "@${file}" >/dev/null
done

echo "Debezium connectors are ready"
