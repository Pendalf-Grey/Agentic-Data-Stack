# Quickstart

Эта ветка использует упрощенный demo-стек ADS-2: внешний Elasticsearch, ClickHouse, LibreChat/Kimi, `ads-log-workflow` MCP, ClickHouse MCP и Grafana.

Короткий запуск:

```bash
cp .env.example .env
# заполните KIMI_API_KEY и URL внешнего Elasticsearch
docker compose up -d
sh tools/elasticsearch-batch-to-clickhouse.sh
sh tools/compress_raw_logs.sh
```

Если во внешнем Elasticsearch еще нет тестовых логов:

```bash
sh tools/elasticsearch-demo-to-clickhouse.sh
```

Подробное описание архитектуры и потока данных находится в [README.md](README.md).
