# Agentic Data Stack

Это локальный стек для аналитики данных, которые приходят из внешней БД через **Debezium** и попадают в **ClickHouse**.

Идея простая: подключаемся к чужой source-БД, забираем изменения, складываем их в аналитическое хранилище, смотрим графики в **Grafana**, задаем вопросы данным через **LibreChat** + **MCP** и отслеживаем работу LLM через **Langfuse**.

Локальный PostgreSQL в проекте оставлен только как demo-пример. В реальной работе чаще используется внешняя БД: чужой **host**, внешний **IP**, отдельный **user**, отдельный **password**, свои правила firewall/VPN/TLS.

Для развертывания системы с нуля на нескольких машинах используйте подробный документ:

```text
docs/JUNIOR_DEVOPS_DEPLOYMENT_GUIDE.md
```

## Что Делают Сервисы

**Airflow** — планировщик.

Он нужен, когда миграцию надо запускать не сразу, а в определенное **время**, **день недели** или по регулярному расписанию. В этом проекте Airflow запускает DAG `scheduled_debezium_migration`, который регистрирует или обновляет Debezium connectors.

В других проектах Airflow чаще всего используют для ETL/ELT-процессов: загрузить данные, преобразовать, проверить качество, запустить отчет, отправить уведомление.

**Debezium** — CDC-инструмент.

CDC означает Change Data Capture. Это способ читать изменения из БД: новые строки, обновления и удаления. Debezium читает журнал изменений source-БД и отправляет события дальше.

В других проектах Debezium часто используют для репликации данных, аудита, realtime-аналитики и синхронизации микросервисов.

**Redpanda** — Kafka-compatible брокер сообщений.

Здесь он работает как транспорт между Debezium и ClickHouse sink connector. Debezium пишет изменения в **topic**, а ClickHouse sink читает этот **topic**.

В других проектах Redpanda или Kafka обычно используют как надежную “шину событий” между сервисами.

**ClickHouse** — аналитическая БД.

Она хранит данные в формате, удобном для быстрых агрегатов: count, group by, latency, error rate, временные ряды.

В других проектах ClickHouse часто используют для логов, продуктовой аналитики, метрик, observability и дешевых быстрых отчетов по большим объемам данных.

**Grafana** — интерфейс для графиков.

Она читает данные из ClickHouse и показывает dashboards. В этом проекте MCP tools возвращают ссылки на Grafana, чтобы модель не пыталась рисовать SVG-картинки внутри LibreChat.

В других проектах Grafana обычно используют для мониторинга, алертов, метрик и операционных dashboards.

**LibreChat** — web UI для общения с моделью.

В этом проекте LibreChat подключен к локальной или облачной OpenAI-compatible модели через `agent-proxy`. Также LibreChat видит MCP tools и может просить их анализировать ClickHouse.

В других проектах LibreChat часто используют как единый чат-интерфейс к нескольким LLM providers.

**Langfuse** — observability-платформа для LLM.

**Observability** означает наблюдаемость: мы видим не только итоговый ответ модели, но и trace запроса, latency, model name, input, output, usage tokens и ошибки.

В этом проекте Langfuse получает traces от `agent-proxy`. LibreChat отправляет запрос в `agent-proxy`, `agent-proxy` вызывает локальную или облачную модель и параллельно отправляет trace в Langfuse.

В других проектах Langfuse часто используют для debugging LLM-приложений, оценки качества ответов, анализа стоимости, prompt management и поиска “почему модель ответила именно так”.

**MCP server** — мост между моделью и инструментами.

MCP означает Model Context Protocol. Это способ дать модели безопасные tools: посмотреть схему ClickHouse, выполнить read-only запрос, получить ссылку на Grafana panel.

В других проектах MCP используют, когда модели нужно не просто отвечать текстом, а работать с внешними системами: БД, API, файлами, задачами, dashboards.

## Подключение К Внешней БД

Основной сценарий проекта — внешняя source-БД.

Это значит, что база не запускается внутри Docker Compose. Она уже существует где-то снаружи: на сервере клиента, в облаке, в корпоративной сети, за VPN или firewall.

В `.env` выберите режим:

```env
SOURCE_MODE=external
# SOURCE_MODE=demo
```

Затем выберите тип active source-БД. Должна быть раскомментирована ровно одна строка:

```env
ACTIVE_SOURCE_DB=postgres
# ACTIVE_SOURCE_DB=mysql
# ACTIVE_SOURCE_DB=mongodb
```

Для внешней БД локальный demo PostgreSQL должен быть выключен:

```env
# COMPOSE_PROFILES=postgres-source
```

Ключевые параметры почти всегда одни и те же:

**host** — DNS-имя или **IP** сервера БД.

**port** — сетевой порт БД, например `5432` для PostgreSQL или `3306` для MySQL.

**user** — пользователь, под которым Debezium подключается к source-БД.

**password** — пароль этого пользователя.

**database** — имя БД.

**table** или **collection** — что именно читаем.

**topic** — поток сообщений в Redpanda/Kafka, куда Debezium пишет изменения.

## Source-БД Первична

В этой архитектуре первична внешняя **source-БД**.

ClickHouse не диктует структуру данных.

ClickHouse хранит аналитическую копию, которая должна быть построена вокруг реальной source schema: реальных **tables**, **columns**, **types** и бизнес-смысла данных.

`analytics.app_events_raw` — это только demo-таблица для локального примера.

В production есть два подхода.

**Manual schema mode**: DevOps заранее создает ClickHouse tables SQL-скриптами.

Это надежнее, когда source schema известна и согласована.

**Auto schema bootstrap mode**: отдельный bootstrap job сначала читает metadata source-БД, генерирует `CREATE TABLE` для ClickHouse, а уже потом запускается ClickHouse sink connector.

Так можно вообще не создавать ClickHouse structure руками.

Важно: это лучше делать отдельным шагом, а не считать обязанностью ClickHouse sink.

Официальный ClickHouse Kafka Connect sink обычно ожидает, что target table уже существует. Поэтому auto-create в этой системе должен быть отдельным pre-step: `schema-bootstrap`.

## External PostgreSQL

Пример `.env` для чужой PostgreSQL:

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

Если PostgreSQL запущен прямо на Mac рядом с Docker Desktop, используйте:

```env
POSTGRES_SOURCE_HOST=host.docker.internal
```

Для PostgreSQL нужны не только **host**, **user** и **password**.

Debezium читает не обычный SQL-дамп, а поток изменений. Поэтому в PostgreSQL должен быть включен `wal_level=logical`.

**WAL** означает Write-Ahead Log. Это журнал PostgreSQL, куда сначала попадают изменения, и уже потом они считаются надежно сохраненными в таблицах.

**Replication slot** — именованная позиция чтения WAL. Она нужна, чтобы PostgreSQL понимал, какие изменения Debezium уже прочитал.

**Publication** — список таблиц, изменения которых PostgreSQL разрешает отдавать наружу.

Пользователь Debezium должен иметь права читать нужную таблицу, использовать logical replication, replication slot и publication.

## External MySQL

Пример `.env` для чужой MySQL:

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

Для MySQL должен быть включен **binlog**.

**Binlog** — binary log, журнал изменений MySQL. Debezium читает его так же, как PostgreSQL connector читает WAL.

Нужные настройки MySQL:

- `binlog_format=ROW`
- `binlog_row_image=FULL`
- уникальный `MYSQL_SOURCE_SERVER_ID`
- права чтения и replication client/slave для пользователя Debezium

## External MongoDB

Пример `.env` для чужой MongoDB:

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

MongoDB должна работать как replica set или sharded cluster.

Debezium использует **change stream**. Это API MongoDB, через который можно подписаться на изменения документов.

Пользователь должен иметь права читать нужную **collection** и ее change stream.

## Local Demo PostgreSQL

Demo-режим нужен только для проверки проекта без внешней БД.

Он запускает локальный PostgreSQL с тестовыми логами `public.app_events`.

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

## Как Работает Миграция

`connectors-init` запускается один раз при старте compose.

Он смотрит на `ACTIVE_SOURCE_DB`, берет нужный шаблон из `debezium/connectors/<db>-source.json`, подставляет значения из `.env` и регистрирует Debezium connector.

ClickHouse sink connector тоже создается автоматически.

Он читает **topic** активной source-БД и пишет строки в таблицу ClickHouse:

```env
CLICKHOUSE_SINK_TABLE=app_events_raw
```

Текущие Grafana dashboards и MCP tools рассчитаны на таблицу `analytics.app_events_raw`.

Если внешняя БД имеет другую структуру, нужно адаптировать ClickHouse schema, `CLICKHOUSE_SINK_TABLE`, Grafana panels и MCP-запросы.

### Ремарка Про ClickHouse Sink

**ClickHouse sink** в этом проекте — это отдельный Kafka Connect connector.

Он не читает внешнюю БД сам.

Он читает сообщения из **Redpanda/Kafka topic** и записывает их в **ClickHouse**.

Вся цепочка выглядит так:

```text
External DB / demo PostgreSQL
  -> Debezium source connector
  -> Redpanda topic
  -> ClickHouse sink connector
  -> ClickHouse table analytics.app_events_raw
```

То есть **Debezium source connector** отвечает за чтение source-БД.

**Redpanda** хранит поток изменений в topic.

**ClickHouse sink connector** забирает этот поток и вставляет строки в ClickHouse.

Конфиг ClickHouse sink находится здесь:

```text
debezium/connectors/clickhouse-sink.json
```

Ключевые настройки:

```json
"topics": "${ACTIVE_SOURCE_TOPIC}",
"database": "${CLICKHOUSE_DB}",
"topic2TableMap": "${ACTIVE_SOURCE_TOPIC}=${CLICKHOUSE_SINK_TABLE}"
```

Это читается так:

```text
читать ACTIVE_SOURCE_TOPIC
писать в CLICKHOUSE_SINK_TABLE
```

В demo-режиме это превращается примерно в такую связь:

```text
pg_flat.public.app_events -> analytics.app_events_raw
```

Поэтому фраза “текущий ClickHouse sink настроен на одну таблицу `app_events_raw`” верна.

Сейчас один source topic складывается в одну ClickHouse table.

Если нужно мигрировать несколько таблиц, например:

```text
public.users
public.orders
public.payments
```

нужно сделать несколько вещей:

1. Добавить эти таблицы в source connector, например через `table.include.list`.
2. Создать соответствующие tables в ClickHouse.
3. Расширить `topic2TableMap`.

Пример:

```text
pg.public.users=users_raw,pg.public.orders=orders_raw,pg.public.payments=payments_raw
```

Без такого mapping sink connector не будет понимать, в какие ClickHouse tables складывать разные topics.

## Airflow: Запуск Миграции По Расписанию

Airflow доступен здесь:

```text
http://localhost:8081
```

Логин и пароль задаются в `.env`:

```env
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com
```

При первом запуске `airflow-init` создает локального пользователя Airflow.

После входа в Airflow найдите DAG:

```text
scheduled_debezium_migration
```

Этот DAG делает то же, что `connectors-init`, но по расписанию: регистрирует или обновляет active Debezium source connector и ClickHouse sink connector.

Расписание задается через **cron** в `.env`:

```env
AIRFLOW_MIGRATION_CRON=0 2 * * *
```

Cron — это короткая запись расписания.

Формат такой:

```text
минута час день_месяца месяц день_недели
```

Примеры:

```env
# Каждый день в 02:00
AIRFLOW_MIGRATION_CRON=0 2 * * *

# Каждый понедельник в 03:30
AIRFLOW_MIGRATION_CRON=30 3 * * 1

# Первого числа каждого месяца в 01:00
AIRFLOW_MIGRATION_CRON=0 1 1 * *
```

По умолчанию DAG создается в paused-состоянии:

```env
AIRFLOW_DAG_PAUSED=true
```

Это сделано специально, чтобы миграция из внешней БД не стартовала случайно.

Чтобы включить расписание:

1. Откройте `http://localhost:8081`.
2. Войдите под `AIRFLOW_ADMIN_USER` и `AIRFLOW_ADMIN_PASSWORD`.
3. Найдите DAG `scheduled_debezium_migration`.
4. Нажмите toggle, чтобы снять DAG с паузы.

Чтобы запустить миграцию вручную, нажмите кнопку Trigger DAG в Airflow UI.

Если меняете `AIRFLOW_MIGRATION_CRON`, перезапустите Airflow scheduler:

```bash
docker compose up -d airflow-scheduler airflow-webserver
```

Важно: Debezium обычно работает как непрерывный CDC-процесс.

Airflow в этом проекте отвечает за момент регистрации или обновления connectors. Если нужен строгий “миграционный интервал”, например запускать в 02:00 и останавливать в 03:00, нужно добавить отдельный DAG для pause/resume или delete connectors.

## Запуск

```bash
cd /Users/subbotaevgenij/PycharmProjects/Clicker/CascadeProjects/Agentic-Data-Stack
cp .env.example .env
```

Перед запуском отредактируйте `.env`.

Если подключаетесь к внешней БД, заполните **host**, **port**, **user**, **password** и остальные переменные активного блока `POSTGRES_SOURCE_*`, `MYSQL_SOURCE_*` или `MONGODB_SOURCE_*`.

Если внешней БД пока нет и нужно проверить проект на demo-данных, включите `SOURCE_MODE=demo` и `COMPOSE_PROFILES=postgres-source`.

После этого запускайте стек:

```bash
docker compose up -d --build
```

Если нужные **ports** уже заняты другим локальным стеком, остановите тот стек или поменяйте published ports в `docker-compose.yml`.

## LibreChat

LibreChat доступен здесь:

```text
http://localhost:3080
```

Сначала нужно зарегистрироваться:

```text
http://localhost:3080/register
```

Первый зарегистрированный пользователь становится администратором LibreChat.

После регистрации откройте:

```text
http://localhost:3080/login
```

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

## Langfuse

Langfuse доступен здесь:

```text
http://localhost:3002
```

При первом запуске проект автоматически создает локального пользователя, organization, project и API keys.

Значения для demo-режима находятся в `.env`:

```env
LANGFUSE_INIT_USER_EMAIL=admin@example.com
LANGFUSE_INIT_USER_PASSWORD=admin123456
LANGFUSE_INIT_ORG_NAME=Agentic Data Stack
LANGFUSE_INIT_PROJECT_NAME=Agentic Data Stack LLM
LANGFUSE_PUBLIC_KEY=pk-lf-agentic-data-stack-local
LANGFUSE_SECRET_KEY=sk-lf-agentic-data-stack-local
```

Для production замените `LANGFUSE_NEXTAUTH_SECRET`, `LANGFUSE_SALT`, `LANGFUSE_ENCRYPTION_KEY`, `LANGFUSE_INIT_USER_PASSWORD`, `LANGFUSE_PUBLIC_KEY` и `LANGFUSE_SECRET_KEY`.

`LANGFUSE_INIT_USER_PASSWORD` должен быть не короче 8 символов.

Сгенерировать секреты можно так:

```bash
openssl rand -base64 32
openssl rand -hex 32
```

Чтобы увидеть traces:

1. Откройте `http://localhost:3002`.
2. Войдите под пользователем из `LANGFUSE_INIT_USER_EMAIL`.
3. Откройте project `Agentic Data Stack LLM`.
4. В LibreChat задайте любой вопрос модели.
5. Вернитесь в Langfuse и откройте раздел `Traces`.

Если traces не появляются, проверьте:

```bash
curl http://localhost:3002/api/public/health
docker compose logs agent-proxy
docker compose logs langfuse-web
docker compose logs langfuse-worker
```

## Локальная Или Облачная Модель

LibreChat ходит в модель через `agent-proxy`.

`agent-proxy` также отправляет traces в Langfuse, если включено:

```env
LANGFUSE_ENABLED=true
LANGFUSE_INTERNAL_URL=http://langfuse-web:3000
```

Для Ollama на macOS обычно используется:

```env
UPSTREAM_OPENAI_BASE_URL=http://host.docker.internal:11434/v1
UPSTREAM_OPENAI_API_KEY=local-dev-key
```

Список моделей для UI:

```env
LIBRECHAT_MODELS=qwen2.5:7b,qwen2.5:14b,llama3.2-vision:latest
OPENAI_MODEL=qwen2.5:7b
OPENAI_MODEL_SMART=qwen2.5:14b
```

## Endpoints

- `http://localhost:3080` — LibreChat Web UI.
- `http://localhost:3080/register` — регистрация локального пользователя LibreChat.
- `http://localhost:3080/login` — вход в LibreChat.
- `http://localhost:8081` — Airflow Web UI.
- `http://localhost:3001` — Grafana UI.
- `http://localhost:3001/d/agentic-data-stack-events/agentic-data-stack-events` — dashboard `Agentic Data Stack Events`.
- `http://localhost:3002` — Langfuse Web UI.
- `http://localhost:3002/api/public/health` — healthcheck Langfuse Web.
- `http://localhost:9090` — MinIO S3 API для Langfuse events/media.
- `http://localhost:9091` — MinIO console.
- `http://localhost:3333/health` — healthcheck MCP server.
- `http://localhost:3333/mcp` — MCP endpoint. Внутри Docker LibreChat использует `http://mcp-server:3333/mcp`.
- `http://localhost:3344/health` — healthcheck `agent-proxy`.
- `http://localhost:3344/v1/models` — debug endpoint списка моделей через `agent-proxy`.
- `http://agent-proxy:3344/v1` — внутренний Docker endpoint для LibreChat.
- `http://host.docker.internal:11434/v1` — OpenAI-compatible endpoint Ollama на macOS.
- `http://localhost:8083` — Debezium Kafka Connect REST API.
- `http://localhost:8083/connectors` — список зарегистрированных Debezium connectors.
- `http://localhost:8123/play` — ClickHouse Web UI.
- `http://localhost:8123` — ClickHouse HTTP API.
- `localhost:9000` — ClickHouse native TCP port.
- `localhost:9092` — Redpanda Kafka API.
- `localhost:9644` — Redpanda admin API.
- `localhost:5432` — demo PostgreSQL, только при `COMPOSE_PROFILES=postgres-source`.

Grafana внутри Docker работает на `grafana:3000`.

Пользовательские ссылки должны использовать внешний адрес `http://localhost:3001`.

## Проверка

```bash
docker compose ps
curl http://localhost:3333/health
curl http://localhost:3344/health
curl http://localhost:3002/api/public/health
curl http://localhost:8083/connectors
```

Проверить строки в ClickHouse:

```bash
curl 'http://localhost:8123/?user=analytics&password=analytics_password' \
  --data-binary 'SELECT count() FROM analytics.app_events_raw'
```

Для demo-режима после initial snapshot ожидается:

```text
1000
```

## Ограничения И Безопасность

Debezium не может читать любую чужую БД только по обычному read-only login.

Для CDC нужны специальные права и настройки source-сервера.

Нужно заранее проверить сетевой путь от Docker Desktop до внешней БД: **VPN**, **firewall**, **allowlist IP**, **DNS**, **TLS**.

Не коммитьте реальные пароли в Git.

Файл `.env` находится в `.gitignore`. В `.env.example` должны быть только placeholders.

Если внешняя БД использует self-signed TLS certificates, потребуется добавить доверенные CA/certs в Debezium container или настроить SSL-параметры под конкретную БД.

Langfuse сохраняет LLM inputs и outputs.

Если в prompts могут попадать персональные данные, токены, коммерческая тайна или данные клиента, нужно заранее определить правила masking/redaction.

Для production Langfuse лучше закрывать за VPN, reverse proxy или corporate SSO.

Текущий ClickHouse sink настроен на одну таблицу `app_events_raw`.

Для нескольких таблиц или другой схемы данных нужно добавить новые ClickHouse tables и расширить `topic2TableMap`.
