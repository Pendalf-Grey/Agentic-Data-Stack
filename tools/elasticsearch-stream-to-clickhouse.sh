#!/bin/sh
set -eu

# Скрипт запускает streaming/micro-batch загрузку документов из Elasticsearch в ClickHouse.
# Это не настоящий CDC: контейнер периодически читает новые документы по timestamp и checkpoint.

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

cd "$ROOT_DIR"

# Поднимаем ClickHouse и отдельный long-running stream connector.
docker compose --profile elasticsearch up -d --build clickhouse
docker compose --profile elasticsearch up -d --build --force-recreate elasticsearch-stream-connector

echo "elasticsearch-stream-connector started"
echo "Check logs:"
echo "docker compose --profile elasticsearch logs -f elasticsearch-stream-connector"
