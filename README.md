# Agentic Data Stack: PostgreSQL + Debezium + ClickHouse + Airflow + LangFuse + LibreChat + MCP

Этот проект — локальный scaffold для связки из статьи LangFuse Agentic Data Stack.

## Инструкции запуска

- `QUICKSTART.md` — краткая инструкция с основными командами, проверками и частыми ошибками.
- `RUNBOOK_DETAILED.md` — максимально подробная инструкция с полным сценарием запуска, UI, Debezium, ClickHouse, LibreChat, Airflow и troubleshooting.
- `LLM_LANGFUSE_SETUP.md` — ревизия локальных/облачных LLM providers и схема подключения LangFuse tracing.

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

## MCP tools

MCP server предоставляет два инструмента:

- `event_summary` — возвращает агрегаты из `analytics.v_event_summary`.
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
