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

## Подключение к внешней source-БД

Основной сценарий проекта — подключение к внешней БД, которая не принадлежит этому compose-стеку. Локальный PostgreSQL оставлен только как отключаемый demo-пример.

В `.env` выберите режим:

```env
SOURCE_MODE=external
# SOURCE_MODE=demo
```

Внешняя БД не запускается Docker Compose. Debezium внутри контейнера подключается к ней по host/port/credentials из `.env`.

Активный source connector выбирается отдельно. Должна быть раскомментирована ровно одна строка:

```env
ACTIVE_SOURCE_DB=postgres
# ACTIVE_SOURCE_DB=mysql
# ACTIVE_SOURCE_DB=mongodb
```

Для внешней БД профиль локального PostgreSQL должен быть выключен:

```env
# COMPOSE_PROFILES=postgres-source
```

Для локального demo-PostgreSQL включите профиль:

```env
SOURCE_MODE=demo
ACTIVE_SOURCE_DB=postgres
COMPOSE_PROFILES=postgres-source
```

### External PostgreSQL

Пример подключения к чужой PostgreSQL:

```env
SOURCE_MODE=external
ACTIVE_SOURCE_DB=postgres
# COMPOSE_PROFILES=postgres-source

POSTGRES_SOURCE_HOST=customer-postgres.example.com
POSTGRES_SOURCE_PORT=5432
POSTGRES_SOURCE_USER=debezium_user
POSTGRES_SOURCE_PASSWORD=change-me-source-password
POSTGRES_SOURCE_DB=customer_app
POSTGRES_SOURCE_TOPIC_PREFIX=customer_pg
POSTGRES_SOURCE_SCHEMA=public
POSTGRES_SOURCE_TABLE=app_events
POSTGRES_SOURCE_SLOT=agentic_data_stack_slot
POSTGRES_SOURCE_PUBLICATION=agentic_data_stack_publication
POSTGRES_SOURCE_SSL_MODE=require
POSTGRES_SOURCE_TOPIC=customer_pg.public.app_events
```

Если PostgreSQL запущен на macOS-хосте рядом с Docker Desktop, используйте:

```env
POSTGRES_SOURCE_HOST=host.docker.internal
```

Для PostgreSQL source-БД должны быть выполнены условия:

- `wal_level=logical` — режим PostgreSQL, который разрешает читать журнал изменений не только для восстановления БД, но и для внешних CDC-инструментов.
- Доступ к host/port из Docker-контейнеров `connectors-init` и `debezium`.
- Пользователь Debezium имеет права на чтение таблицы, создание/использование replication slot и publication.
- Таблица должна иметь primary key или replica identity, подходящую для CDC.
- `POSTGRES_SOURCE_SLOT` должен быть уникальным для этого Debezium-подключения.

### External MySQL

Пример подключения к чужой MySQL:

```env
SOURCE_MODE=external
ACTIVE_SOURCE_DB=mysql
# COMPOSE_PROFILES=postgres-source

MYSQL_SOURCE_HOST=customer-mysql.example.com
MYSQL_SOURCE_PORT=3306
MYSQL_SOURCE_USER=debezium_user
MYSQL_SOURCE_PASSWORD=change-me-source-password
MYSQL_SOURCE_DB=customer_app
MYSQL_SOURCE_TOPIC_PREFIX=customer_mysql
MYSQL_SOURCE_TABLE=app_events
MYSQL_SOURCE_SERVER_ID=184054
MYSQL_SOURCE_SSL_MODE=preferred
MYSQL_SOURCE_TOPIC=customer_mysql.customer_app.app_events
```

Для MySQL source-БД должны быть выполнены условия:

- Включен binary log — журнал изменений MySQL, из которого Debezium читает события.
- `binlog_format=ROW` — Debezium нужны изменения на уровне строк, а не только SQL-команды.
- `binlog_row_image=FULL` — в binlog должны попадать все поля измененной строки.
- `MYSQL_SOURCE_SERVER_ID` уникален среди replica/CDC clients.
- Пользователь Debezium имеет права чтения и replication client/slave.

### External MongoDB

Пример подключения к чужой MongoDB:

```env
SOURCE_MODE=external
ACTIVE_SOURCE_DB=mongodb
# COMPOSE_PROFILES=postgres-source

MONGODB_SOURCE_CONNECTION_STRING=mongodb://user:password@customer-mongo.example.com:27017/?replicaSet=rs0&authSource=admin
MONGODB_SOURCE_DB=customer_app
MONGODB_SOURCE_COLLECTION=app_events
MONGODB_SOURCE_TOPIC_PREFIX=customer_mongo
MONGODB_SOURCE_TOPIC=customer_mongo.customer_app.app_events
```

Для MongoDB source-БД должны быть выполнены условия:

- MongoDB работает как replica set или sharded cluster с change streams.
- Пользователь имеет права читать collection и change stream.
- Connection string должен включать нужный `authSource`, replica set и TLS-параметры, если они требуются.

### Local demo PostgreSQL

Demo-настройки нужны только для проверки проекта без внешней БД:

```env
SOURCE_MODE=demo
ACTIVE_SOURCE_DB=postgres
COMPOSE_PROFILES=postgres-source

POSTGRES_DB=app_logs
POSTGRES_USER=app
POSTGRES_PASSWORD=app_password

POSTGRES_SOURCE_HOST=postgres
POSTGRES_SOURCE_PORT=5432
POSTGRES_SOURCE_USER=app
POSTGRES_SOURCE_PASSWORD=app_password
POSTGRES_SOURCE_DB=app_logs
POSTGRES_SOURCE_TOPIC_PREFIX=pg_flat
POSTGRES_SOURCE_SCHEMA=public
POSTGRES_SOURCE_TABLE=app_events
POSTGRES_SOURCE_SLOT=app_events_slot
POSTGRES_SOURCE_PUBLICATION=app_events_publication
POSTGRES_SOURCE_SSL_MODE=disable
POSTGRES_SOURCE_TOPIC=pg_flat.public.app_events
```

### Как применяется переключение

`connectors-init` при старте делает три вещи:

1. Смотрит `ACTIVE_SOURCE_DB`.
2. Рендерит нужный шаблон из `debezium/connectors/<db>-source.json`.
3. Создает или обновляет активный source connector и ClickHouse sink connector, а неактивные source connectors удаляет.

ClickHouse sink использует topic из переменной активной БД, например `POSTGRES_SOURCE_TOPIC`, и пишет его в таблицу:

```env
CLICKHOUSE_SINK_TABLE=app_events_raw
```

Важно: текущая аналитика, Grafana dashboard и MCP tools рассчитаны на структуру `analytics.app_events_raw`.

При переключении на MySQL или MongoDB источник должен отдавать поля, совместимые с этой таблицей.

Если структура данных другая, нужно обновить ClickHouse schema, `CLICKHOUSE_SINK_TABLE`, Grafana panels и MCP-запросы под новую модель данных.

### Термины

CDC означает Change Data Capture.

Это подход, при котором система читает поток изменений из исходной БД: insert, update, delete. Debezium использует CDC, чтобы переносить не только начальный snapshot, но и последующие изменения.

Snapshot — это первичная выгрузка текущего состояния таблицы или collection.

После snapshot Debezium переходит в потоковый режим и начинает читать новые изменения из журнала БД.

Replication slot в PostgreSQL — это именованная позиция чтения WAL.

WAL означает Write-Ahead Log. Это журнал PostgreSQL, куда сначала записываются изменения, а уже потом они считаются надежно сохраненными в таблицах.

Publication в PostgreSQL — это список таблиц, изменения которых разрешено публиковать для logical replication.

Logical replication — это репликация на уровне строк и таблиц, а не побайтовая копия файлов БД.

Binlog в MySQL — это binary log, журнал изменений MySQL.

Change stream в MongoDB — это API, через который можно подписаться на изменения документов.

Topic в Kafka/Redpanda — это именованный поток сообщений.

Debezium пишет изменения из source-БД в topic, а ClickHouse sink читает этот topic и пишет строки в таблицу ClickHouse.

Sink connector — это коннектор, который забирает данные из Kafka/Redpanda и пишет их в целевую систему.

Source connector — это коннектор, который читает данные из исходной системы и отправляет их в Kafka/Redpanda.

### Ограничения и безопасность

- Debezium не может читать любую чужую БД только по обычному read-only login.
- Для CDC нужны специальные настройки сервера и права replication/change stream.
- Network path должен быть открыт от Docker Desktop до внешней БД.
- VPN, firewall, allowlist IP, DNS и TLS должны быть настроены отдельно.
- Не коммитьте реальные пароли в Git.
- Файл `.env` находится в `.gitignore`, а `.env.example` должен содержать только placeholders.
- Если внешний источник использует self-signed TLS certificates, потребуется добавить доверенные CA/certs в Debezium container или настроить connection string/SSL mode под конкретную БД.
- Текущий ClickHouse sink настроен на одну таблицу `app_events_raw`.
- Для нескольких таблиц или другой схемы данных нужно добавить новые ClickHouse tables и расширить `topic2TableMap`.

## Запуск

```bash
cd /Users/subbotaevgenij/PycharmProjects/Clicker/CascadeProjects/Agentic-Data-Stack
cp .env.example .env
```

Перед запуском отредактируйте `.env`.

Если подключаетесь к внешней БД, заполните host, port, user, password и остальные переменные активного блока `POSTGRES_SOURCE_*`, `MYSQL_SOURCE_*` или `MONGODB_SOURCE_*`.

Если внешней БД пока нет и нужно проверить проект на demo-данных, включите `SOURCE_MODE=demo` и `COMPOSE_PROFILES=postgres-source`.

После этого запускайте стек:

```bash
docker compose up -d --build
```

Если нужные порты уже заняты другим локальным стеком, остановите тот стек или поменяйте published ports в `docker-compose.yml`.

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
