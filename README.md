# Agentic Data Stack: PostgreSQL + Debezium + ClickHouse + Airflow + LangFuse + LibreChat + MCP

Этот проект — локальный scaffold для связки из статьи LangFuse Agentic Data Stack.

## Инструкции запуска

- `QUICKSTART.md` — краткая инструкция с основными командами, проверками и частыми ошибками.
- `RUNBOOK_DETAILED.md` — максимально подробная инструкция с полным сценарием запуска, UI, Debezium, ClickHouse, LibreChat, Airflow и troubleshooting.
- `LLM_LANGFUSE_SETUP.md` — ревизия локальных/облачных LLM providers и схема подключения LangFuse tracing.
- `UBUNTU_BARE_DEPLOYMENT.md` — развертывание минимального Agentic Data Stack на трёх bare Ubuntu servers через Tailscale.

## Конфигурация локальных LLM

LibreChat ходит в локальные/облачные модели через `agent-proxy`.

Основные параметры задаются в `.env`:

- `LIBRECHAT_MODELS` — comma-separated список моделей для UI.
- `LIBRECHAT_TITLE_MODEL` — модель для заголовков.
- `LIBRECHAT_SUMMARY_MODEL` — модель для summaries.
- `AGENT_PROXY_BASE_URL` — внутренний URL proxy для LibreChat.
- `UPSTREAM_OPENAI_BASE_URL` — реальный OpenAI-compatible endpoint, например Ollama на macOS.

`librechat/librechat.yaml` вручную менять не нужно: при старте контейнера он генерируется из `librechat/librechat.yaml.template` через `librechat/render-config.sh`.

## Grafana dashboards

Grafana доступна на `http://localhost:3001`.

Доступ:

```text
admin / admin
```

Dashboard `Agentic Data Stack Events` создаётся автоматически:

```text
http://localhost:3001/d/agentic-data-stack-events/agentic-data-stack-events
```

Datasource `ClickHouse Analytics` подключён к ClickHouse и использует таблицу `analytics.app_events_raw`.

## Архитектура

```text
PostgreSQL app_logs
  └─ seeded table public.app_events
      └─ Debezium PostgreSQL Source Connector
          └─ Redpanda/Kafka topic pg.public.app_events
              └─ ClickHouse Kafka Connect Sink Connector
                  └─ ClickHouse analytics.app_events_raw
                      └─ MCP Server tools over ClickHouse
                          └─ LibreChat + AI models

LangFuse работает рядом как observability layer для LLM traces.
Airflow по расписанию регистрирует/restarts Debezium connector и запускает snapshot signal.
```

## Сервисы

- `postgres` — PostgreSQL 16 с logical replication и тестовыми логами.
- `clickhouse` — аналитическая БД.
- `redpanda` — Kafka-compatible broker для Debezium.
- `debezium` — Kafka Connect runtime.
- `debezium-ui` — UI для Connect.
- `airflow-webserver` / `airflow-scheduler` — расписание Debezium jobs.
- `langfuse` — LLM observability.
- `librechat` — чат-интерфейс.
- `agent-proxy` — OpenAI-compatible proxy для локальных/облачных моделей и LangFuse traces.
- `grafana` — BI/UI dashboards поверх ClickHouse.
- `mcp-server` — MCP HTTP server для запросов в ClickHouse.

## Важное ограничение

Debezium PostgreSQL connector является source connector: он переносит изменения из PostgreSQL в Kafka/Redpanda.

Чтобы данные физически попадали в ClickHouse, нужен ClickHouse Kafka Connect sink plugin.
Конфиг уже подготовлен в `debezium/connectors/clickhouse-sink.json`, но JAR-плагин нужно положить в папку:

```text
debezium/plugins
```

После добавления плагина контейнер `debezium` должен увидеть connector class:

```text
com.clickhouse.kafka.connect.ClickHouseSinkConnector
```

## Запуск

```bash
docker compose up -d --build
```

## UI и endpoints

- LibreChat: http://localhost:3080
- LangFuse: http://localhost:3000
- Airflow: http://localhost:8081
  - login: `admin`
  - password: `admin`
- Debezium UI: http://localhost:8080
- Debezium Connect REST: http://localhost:8083
- ClickHouse HTTP: http://localhost:8123
- MCP server health: http://localhost:3333/health
- Agent proxy health: http://localhost:3344/health
- Grafana: http://localhost:3001
- Grafana dashboard: http://localhost:3001/d/agentic-data-stack-events/agentic-data-stack-events

## Ручная регистрация connectors

Source connector:

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @debezium/connectors/postgres-source.json
```

Sink connector после установки ClickHouse sink plugin:

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @debezium/connectors/clickhouse-sink.json
```

## Проверка PostgreSQL

```bash
docker compose exec postgres psql -U app -d app_logs -c "SELECT count(*) FROM app_events;"
```

## Проверка ClickHouse

```bash
docker compose exec clickhouse clickhouse-client \
  --user analytics \
  --password analytics_password \
  --query "SELECT * FROM analytics.v_event_summary LIMIT 10"
```

## LibreChat + ClickHouse analysis

LibreChat is configured with the `clickhouse-analytics` MCP server:

```text
http://mcp-server:3333/mcp
```

In LibreChat, use the `Local OpenAI-compatible` endpoint and enable/select the `clickhouse-analytics` MCP tools in the chat or add them to an Agent. Then the selected model can inspect the ClickHouse schema, read migrated event samples, use purpose-built analytics tools, return Grafana panel links, run read-only ClickHouse `SELECT` queries, and summarize the results.

Grafana links returned to the user must use the host-published URL:

```text
http://localhost:3001
```

The MCP server creates Grafana short URLs through the Grafana API and rewrites them to this host-published URL. Do not rewrite these links to Docker-internal `grafana:3000`, `grafana-server:3000`, or `localhost:3000`, and do not invent `d-solo` links.

Example prompts:

```text
Analyze the migrated ClickHouse events and show the busiest routes by event count.
```

```text
Use ClickHouse to find error-rate trends by hour and explain the likely hotspots.
```

```text
Compare model usage, token usage, and total_cost_usd by model_name.
```

```text
Построй график количества логов по времени с разбивкой по event_type.
```

```text
Визуализируй error rate по routes и объясни, какие endpoints проблемные.
```

## MCP tools

MCP server предоставляет инструменты:

- `describe_analytics_schema` — показывает схему аналитических таблиц и views.
- `sample_app_events` — возвращает примеры строк, мигрированных из PostgreSQL в ClickHouse через Debezium.
- `event_summary` — возвращает агрегаты из `analytics.v_event_summary`.
- `route_performance` — анализирует трафик, пользователей, ошибки, error rate и latency по route.
- `model_usage` — анализирует использование моделей, токены, completions и стоимость по `model_name`.
- `error_trends` — показывает почасовые ошибки по route и status code.
- `visualize_event_volume` — возвращает ссылку на Grafana time-series panel объема логов по времени в разрезе `event_type`.
- `visualize_route_performance` — возвращает ссылку на Grafana panel по route для `events`, `error_rate`, `avg_latency_ms` или `p95_latency_ms`.
- `visualize_model_usage` — возвращает ссылку на Grafana panel по `model_name` для `events`, `total_tokens`, `total_cost_usd` или `avg_latency_ms`.
- `run_readonly_query` — выполняет только `SELECT`-запросы в ClickHouse.

LibreChat настроен на MCP endpoint:

```text
http://mcp-server:3333/mcp
```

## LangFuse

LangFuse поднимается отдельно на `localhost:3000`.
Для production-like запуска нужно заменить секреты в `.env`:

- `NEXTAUTH_SECRET`
- `SALT`

LLM traces отправляет `agent-proxy`, используя ключи `LANGFUSE_PUBLIC_KEY` и `LANGFUSE_SECRET_KEY` из `.env`.

## Что еще можно доделать для production-ready схемы

- Разделить PostgreSQL для app logs и metadata DB Airflow.
- Заменить dev-секреты в `.env` на безопасные значения.
- Добавить генератор новых логов для realtime CDC demo.
- Расширить MCP tools под частые аналитические сценарии.
- Добавить больше Grafana panels: latency percentiles, errors by route, model usage, status distribution.
