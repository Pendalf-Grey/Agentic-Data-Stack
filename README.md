# Agentic Data Stack

Минимальный локальный стек для аналитики логов из PostgreSQL, мигрированных через Debezium в ClickHouse, с LibreChat, MCP tools и Grafana-визуализациями.

## Что осталось в проекте

- `postgres` — источник логов `public.app_events` с logical replication.
- `redpanda` — Kafka-compatible broker для Debezium.
- `debezium` — Kafka Connect runtime.
- `connectors-init` — одноразовая регистрация Debezium source/sink connectors.
- `clickhouse` — аналитическое хранилище `analytics`.
- `grafana` — dashboards поверх ClickHouse, опубликована на `http://localhost:3001`.
- `mcp-server` — MCP tools для LibreChat: аналитика ClickHouse и ссылки на Grafana.
- `agent-proxy` — OpenAI-compatible proxy к локальной/облачной модели.
- `librechat` и `librechat-db` — чат-интерфейс и MongoDB.

Из стека убраны Airflow, Langfuse, Adminer и Debezium UI, потому что для текущего сценария они не обязательны.

## Запуск

```bash
cd /Users/subbotaevgenij/PycharmProjects/Clicker/CascadeProjects/Agentic-Data-Stack
cp .env.example .env
docker compose up -d --build
```

Если старый проект `2048` уже запущен на тех же портах, сначала остановите его:

```bash
cd /Users/subbotaevgenij/PycharmProjects/Clicker/CascadeProjects/2048
docker compose down
```

Потом запускайте новый клон из `Agentic-Data-Stack`.

## Локальная модель

По умолчанию LibreChat ходит через `agent-proxy` в OpenAI-compatible endpoint:

```env
UPSTREAM_OPENAI_BASE_URL=http://host.docker.internal:11434/v1
UPSTREAM_OPENAI_API_KEY=local-dev-key
LIBRECHAT_MODELS=qwen2.5:7b,qwen2.5:14b,llama3.2-vision:latest
OPENAI_MODEL=qwen2.5:7b
OPENAI_MODEL_SMART=qwen2.5:14b
```

Если используете Ollama, убедитесь, что нужные модели скачаны на macOS:

```bash
ollama pull qwen2.5:14b
ollama pull nomic-embed-text
```

## Адреса

- LibreChat: `http://localhost:3080`
- Grafana: `http://localhost:3001`
- Grafana dashboard: `http://localhost:3001/d/agentic-data-stack-events/agentic-data-stack-events`
- MCP health: `http://localhost:3333/health`
- Agent proxy health: `http://localhost:3344/health`
- Debezium REST: `http://localhost:8083`
- ClickHouse HTTP/UI: `http://localhost:8123/play`
- PostgreSQL: `localhost:5432`

Grafana внутри Docker работает на `grafana:3000`, но пользовательские ссылки должны быть только с внешним портом `localhost:3001`. MCP server уже настроен так, чтобы отдавать ссылки именно на `http://localhost:3001`.

## Проверка

```bash
docker compose ps
curl http://localhost:3333/health
curl http://localhost:3344/health
```

Проверить, что Debezium connectors зарегистрированы:

```bash
curl http://localhost:8083/connectors
```

Ожидаемо:

```json
["postgres-app-events-source","clickhouse-app-events-sink"]
```

Проверить миграцию данных в ClickHouse:

```bash
curl 'http://localhost:8123/?user=analytics&password=analytics_password' \
  --data-binary 'SELECT count() FROM analytics.app_events_raw'
```

Ожидаемо после initial snapshot:

```text
1000
```

## LibreChat + ClickHouse

LibreChat настроен на MCP endpoint:

```text
http://mcp-server:3333/mcp
```

Регистрация локальных пользователей включена через `.env`:

```env
ALLOW_EMAIL_LOGIN=true
ALLOW_REGISTRATION=true
ALLOW_UNVERIFIED_EMAIL_LOGIN=true
```

Первый зарегистрированный пользователь становится администратором LibreChat.

Порядок первого входа:

1. Откройте `http://localhost:3080/register`.
2. Зарегистрируйте локального пользователя.
3. После регистрации откройте `http://localhost:3080/login`.
4. Войдите с email и паролем, которые указали при регистрации.

В LibreChat выберите endpoint `Local OpenAI-compatible` и включите MCP tools `clickhouse-analytics`.

Примеры запросов:

```text
Проанализируй данные, мигрированные в ClickHouse через Debezium: какие routes самые проблемные по error rate и latency?
```

```text
Построй график количества логов по времени с разбивкой по event_type.
```

```text
Визуализируй error rate по routes и дай ссылку на Grafana.
```

## MCP tools

- `describe_analytics_schema`
- `sample_app_events`
- `event_summary`
- `route_performance`
- `model_usage`
- `error_trends`
- `visualize_event_volume`
- `visualize_route_performance`
- `visualize_model_usage`
- `run_readonly_query`
