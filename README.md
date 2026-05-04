# Agentic Data Stack

Минимальный локальный стек для аналитики логов, мигрированных через Debezium в ClickHouse, с LibreChat, MCP tools и Grafana-визуализациями.

## Что осталось в проекте

- `postgres` — локальный demo-источник логов `public.app_events` с logical replication. Используется только когда включен профиль `postgres-source`.
- `redpanda` — Kafka-compatible broker для Debezium.
- `debezium` — Kafka Connect runtime.
- `connectors-init` — одноразовая регистрация Debezium source/sink connectors.
- `clickhouse` — аналитическое хранилище `analytics`.
- `grafana` — dashboards поверх ClickHouse, опубликована на `http://localhost:3001`.
- `mcp-server` — MCP tools для LibreChat: аналитика ClickHouse и ссылки на Grafana.
- `agent-proxy` — OpenAI-compatible proxy к локальной/облачной модели.
- `librechat` и `librechat-db` — чат-интерфейс и MongoDB.

Из стека убраны Airflow, Langfuse, Adminer и Debezium UI, потому что для текущего сценария они не обязательны.

## Выбор source-БД для Debezium

Активный source connector выбирается через `.env`. В блоке ниже должна быть раскомментирована ровно одна строка:

```env
ACTIVE_SOURCE_DB=postgres
# ACTIVE_SOURCE_DB=mysql
# ACTIVE_SOURCE_DB=mongodb
```

Для локальной demo-миграции из PostgreSQL оставьте профиль:

```env
COMPOSE_PROFILES=postgres-source
```

Если источник внешний и это не локальный PostgreSQL, закомментируйте профиль:

```env
# COMPOSE_PROFILES=postgres-source
```

Дальше заполните только блок переменных для активной БД. Остальные блоки должны оставаться закомментированными:

```env
POSTGRES_SOURCE_HOST=postgres
POSTGRES_SOURCE_PORT=5432
POSTGRES_SOURCE_USER=app
POSTGRES_SOURCE_PASSWORD=app_password
POSTGRES_SOURCE_DB=app_logs
POSTGRES_SOURCE_TOPIC_PREFIX=pg_flat
POSTGRES_SOURCE_SCHEMA=public
POSTGRES_SOURCE_TABLE=app_events
POSTGRES_SOURCE_TOPIC=pg_flat.public.app_events

# MYSQL_SOURCE_HOST=host.docker.internal
# MYSQL_SOURCE_PORT=3306
# MYSQL_SOURCE_USER=app
# MYSQL_SOURCE_PASSWORD=app_password
# MYSQL_SOURCE_DB=app_logs
# MYSQL_SOURCE_TOPIC_PREFIX=mysql_flat
# MYSQL_SOURCE_TABLE=app_events
# MYSQL_SOURCE_SERVER_ID=184054
# MYSQL_SOURCE_TOPIC=mysql_flat.app_logs.app_events

# MONGODB_SOURCE_CONNECTION_STRING=mongodb://host.docker.internal:27017
# MONGODB_SOURCE_DB=app_logs
# MONGODB_SOURCE_COLLECTION=app_events
# MONGODB_SOURCE_TOPIC_PREFIX=mongo_flat
# MONGODB_SOURCE_TOPIC=mongo_flat.app_logs.app_events
```

`connectors-init` при старте делает три вещи:

1. Смотрит `ACTIVE_SOURCE_DB`.
2. Рендерит нужный шаблон из `debezium/connectors/<db>-source.json`.
3. Создает или обновляет активный source connector и ClickHouse sink connector, а неактивные source connectors удаляет.

ClickHouse sink использует topic из переменной активной БД, например `POSTGRES_SOURCE_TOPIC`, и пишет его в таблицу:

```env
CLICKHOUSE_SINK_TABLE=app_events_raw
```

Важно: текущая аналитика, Grafana dashboard и MCP tools рассчитаны на структуру `analytics.app_events_raw`. При переключении на MySQL или MongoDB источник должен отдавать поля, совместимые с этой таблицей, либо нужно обновить ClickHouse schema, `CLICKHOUSE_SINK_TABLE`, Grafana panels и MCP-запросы под новую модель данных.

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

## Endpoints

- `http://localhost:3080` — LibreChat Web UI.
- `http://localhost:3080/register` — регистрация первого локального пользователя.
- `http://localhost:3080/login` — вход в LibreChat после регистрации.
- `http://localhost:3001` — Grafana UI.
- `http://localhost:3001/d/agentic-data-stack-events/agentic-data-stack-events` — dashboard `Agentic Data Stack Events`.
- `http://localhost:3333/health` — healthcheck MCP server.
- `http://localhost:3333/mcp` — MCP endpoint, который LibreChat использует внутри Docker как `http://mcp-server:3333/mcp`.
- `http://localhost:3344/health` — healthcheck OpenAI-compatible `agent-proxy`.
- `http://localhost:3344/v1/models` — внешний debug endpoint списка моделей через `agent-proxy`.
- `http://agent-proxy:3344/v1` — внутренний Docker endpoint для LibreChat, задается как `AGENT_PROXY_BASE_URL`.
- `http://host.docker.internal:11434/v1` — OpenAI-compatible endpoint Ollama на macOS, задается как `UPSTREAM_OPENAI_BASE_URL`.
- `http://localhost:8083` — Debezium Kafka Connect REST API.
- `http://localhost:8083/connectors` — список зарегистрированных Debezium connectors.
- `http://localhost:8123/play` — ClickHouse Web UI.
- `http://localhost:8123` — ClickHouse HTTP API.
- `localhost:9000` — ClickHouse native TCP port.
- `localhost:9092` — Redpanda Kafka API.
- `localhost:9644` — Redpanda admin API.
- `localhost:5432` — demo PostgreSQL, только при `COMPOSE_PROFILES=postgres-source`.

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
