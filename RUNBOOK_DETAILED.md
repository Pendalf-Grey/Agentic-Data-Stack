# Detailed Runbook: Agentic Data Stack

Подробная инструкция для запуска и проверки полного локального стека:

```text
PostgreSQL -> Debezium Source -> Redpanda/Kafka -> ClickHouse Sink -> ClickHouse
                                                   -> MCP Server -> LibreChat
LibreChat -> agent-proxy -> Ollama/OpenAI-compatible provider -> LangFuse traces
ClickHouse -> Grafana dashboards
Airflow рядом как scheduler/orchestrator
Adminer как PostgreSQL UI
ClickHouse built-in UI для ClickHouse
```

## 1. Что входит в стек

### Data pipeline

- `postgres` — PostgreSQL 16, база `app_logs`, таблица `public.app_events`, seeded logs.
- `redpanda` — Kafka-compatible broker.
- `debezium` — Kafka Connect runtime.
- `debezium-ui` — UI для Debezium Connect.
- `clickhouse` — аналитическое хранилище.
- `mcp-server` — MCP server, который выполняет read-only запросы к ClickHouse.
- `agent-proxy` — OpenAI-compatible proxy между LibreChat и локальными/облачными моделями, пишет traces в LangFuse.

### Interfaces and apps

- `librechat` — UI для общения с AI-моделями и MCP tools.
- `langfuse` — observability/tracing для LLM.
- `grafana` — BI/UI dashboards поверх ClickHouse.
- `airflow-webserver` и `airflow-scheduler` — scheduler/orchestrator.
- `adminer` — web UI для PostgreSQL.

## 2. Требования

Проверьте версии:

```bash
docker --version
docker compose version
```

Нужен Docker Desktop с поддержкой Docker Compose v2.

На Apple Silicon часть образов может запускаться через `platform: linux/amd64`. В compose это уже учтено для `debezium-ui`.

## 3. Порты

Перед запуском убедитесь, что порты свободны:

```text
3000  LangFuse
3001  Grafana
3080  LibreChat
3333  MCP server
3344  Agent proxy
5432  PostgreSQL
8080  Debezium UI
8081  Airflow UI
8082  Adminer PostgreSQL UI
8083  Debezium Connect REST
8123  ClickHouse HTTP/UI
9000  ClickHouse native
9092  Redpanda/Kafka
9644  Redpanda admin
```

Если порт занят, либо остановите конфликтующий сервис, либо измените mapping в `docker-compose.yml`.

## 4. Важная заметка про VPN и Docker Hub

Если Docker Hub недоступен без VPN, могут быть ошибки:

```text
failed to authorize
failed to fetch oauth token
unexpected EOF
no matching manifest for linux/arm64/v8
```

Решение:

```bash
docker compose pull
```

с включенным VPN.

Если скачивание прервалось, повторите команду. Docker обычно докачивает уже полученные layers.

## 5. Первый запуск

Из корня проекта:

Если `.env` отсутствует:

```bash
cp .env.example .env
```

Если у разработчика другие локальные модели, нужно поменять в `.env`:

```env
LIBRECHAT_MODELS=your-fast-model:latest,your-smart-model:latest
LIBRECHAT_TITLE_MODEL=your-fast-model:latest
LIBRECHAT_SUMMARY_MODEL=your-fast-model:latest
OPENAI_MODEL=your-fast-model:latest
UPSTREAM_OPENAI_BASE_URL=http://host.docker.internal:11434/v1
```

`librechat/librechat.yaml` вручную менять не нужно. При старте LibreChat генерирует `/app/librechat.yaml` из `librechat/librechat.yaml.template` через `librechat/render-config.sh`.

```bash
docker compose pull
```

Затем:

```bash
docker compose up -d --build
```

Разница команд:

```bash
docker compose up -d
```

запускает стек и собирает локальные images только при необходимости.

```bash
docker compose up -d --build
```

принудительно пересобирает сервисы с `build:`. В этом проекте это `mcp-server`.
Также пересобирается `agent-proxy`, если его код менялся.

Неправильно:

```bash
docker compose up -d build
```

Так Docker воспримет `build` как имя сервиса.

## 6. Проверка контейнеров

```bash
docker compose ps
```

Ожидаемо ключевые контейнеры должны быть `Up`, а для некоторых — `healthy`:

```text
ads_postgres          Up / healthy
ads_clickhouse        Up / healthy
ads_redpanda          Up / healthy
ads_debezium          Up
ads_debezium_ui       Up
ads_mcp_server        Up
ads_agent_proxy       Up
ads_librechat         Up
ads_langfuse          Up
ads_grafana           Up
ads_airflow_webserver Up
ads_airflow_scheduler Up
ads_adminer           Up
```

Если контейнер не поднялся:

```bash
docker compose logs --tail=120 <service-name>
```

Пример:

```bash
docker compose logs --tail=120 librechat
```

## 7. Проверка PostgreSQL

PostgreSQL app logs:

```bash
docker compose exec postgres psql -U app -d app_logs -c "SELECT count(*) FROM app_events;"
```

Ожидаемо:

```text
1000
```

Посмотреть первые строки:

```bash
docker compose exec postgres psql -U app -d app_logs -c "SELECT id, event_time, user_id, event_type FROM app_events ORDER BY id LIMIT 10;"
```

Проверить logical replication settings:

```bash
docker compose exec postgres psql -U app -d app_logs -c "SHOW wal_level;"
```

Ожидаемо:

```text
logical
```

## 8. PostgreSQL UI через Adminer

Откройте:

```text
http://localhost:8082
```

Для основной базы логов:

```text
System: PostgreSQL
Server: postgres
Username: app
Password: app_password
Database: app_logs
```

Для LangFuse database:

```text
System: PostgreSQL
Server: langfuse-db
Username: langfuse
Password: langfuse_password
Database: langfuse
```

После входа откройте таблицу:

```text
public.app_events
```

## 9. Проверка ClickHouse

Быстрая проверка:

```bash
curl 'http://localhost:8123/?user=analytics&password=analytics_password' \
  --data-binary 'SELECT 1'
```

Ожидаемо:

```text
1
```

Проверить таблицы:

```bash
curl 'http://localhost:8123/?user=analytics&password=analytics_password' \
  --data-binary "SELECT database, name FROM system.tables WHERE database = 'analytics' ORDER BY name FORMAT PrettyCompact"
```

Ожидаемо:

```text
analytics.app_events_raw
analytics.v_event_summary
```

Проверить данные:

```bash
curl 'http://localhost:8123/?user=analytics&password=analytics_password' \
  --data-binary 'SELECT count() FROM analytics.app_events_raw'
```

После успешной работы Debezium ожидаемо:

```text
1000
```

Агрегация:

```bash
curl 'http://localhost:8123/?user=analytics&password=analytics_password' \
  --data-binary 'SELECT event_type, count() FROM analytics.app_events_raw GROUP BY event_type ORDER BY count() DESC FORMAT PrettyCompact'
```

Ожидаемо примерно:

```text
error             200
chat_message      200
page_view         200
tool_call         200
model_completion  200
```

## 10. ClickHouse UI

Откройте:

```text
http://localhost:8123/play
```

Credentials:

```text
User: analytics
Password: analytics_password
Database: analytics
```

Примеры запросов:

```sql
SELECT count()
FROM analytics.app_events_raw;
```

```sql
SELECT *
FROM analytics.app_events_raw
LIMIT 20;
```

```sql
SELECT *
FROM analytics.v_event_summary
LIMIT 20;
```

## 11. Проверка Debezium Connect REST

Проверить REST API:

```bash
curl http://localhost:8083/connectors
```

Если connectors ещё не созданы, будет:

```json
[]
```

Проверить plugins:

```bash
curl http://localhost:8083/connector-plugins
```

Должны присутствовать:

```text
io.debezium.connector.postgresql.PostgresConnector
com.clickhouse.kafka.connect.ClickHouseSinkConnector
```

Если ClickHouse plugin отсутствует, проверьте JAR:

```bash
find debezium/plugins -type f -name '*.jar'
```

Ожидаемо:

```text
debezium/plugins/clickhouse-kafka-connect/.../clickhouse-kafka-connect-v1.3.7-confluent.jar
```

После добавления JAR перезапустите Debezium:

```bash
docker compose up -d --force-recreate debezium
```

## 12. Регистрация Debezium connectors

### Source connector: PostgreSQL -> Redpanda/Kafka

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @debezium/connectors/postgres-source.json
```

### Sink connector: Redpanda/Kafka -> ClickHouse

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @debezium/connectors/clickhouse-sink.json
```

Если connector уже существует, ответ может быть:

```text
409 Conflict
```

Это нормально. Проверить список:

```bash
curl http://localhost:8083/connectors
```

Ожидаемо:

```json
["postgres-app-events-source","clickhouse-app-events-sink"]
```

## 13. Проверка статуса Debezium connectors

Source:

```bash
curl http://localhost:8083/connectors/postgres-app-events-source/status
```

Sink:

```bash
curl http://localhost:8083/connectors/clickhouse-app-events-sink/status
```

Ожидаемо:

```text
connector.state = RUNNING
tasks[0].state = RUNNING
```

Если task `FAILED`, смотрите `trace` в JSON status и логи:

```bash
docker compose logs --tail=200 debezium
```

## 14. Debezium UI

Откройте:

```text
http://localhost:8080
```

Важно: если Debezium UI показывает:

```text
Server API problem
```

или не показывает connectors, проверяйте состояние через REST API:

```bash
curl http://localhost:8083/connectors
```

REST API является источником правды. В этом проекте Debezium UI может выдавать 404 на endpoint проверки topic creation, но connectors при этом работают.

## 15. Проверка Redpanda/Kafka topics

Список topics:

```bash
docker compose exec redpanda rpk topic list
```

Ожидаемый data topic:

```text
pg_flat.public.app_events
```

Посмотреть consumer groups:

```bash
docker compose exec redpanda rpk group list
```

## 16. MCP server

Health:

```bash
curl http://localhost:3333/health
```

Ожидаемо:

```json
{"ok":true}
```

MCP tools доступны LibreChat:

```text
event_summary
run_readonly_query
```

Если LibreChat пишет:

```text
Domain "http://mcp-server:3333" is not allowed
```

Проверьте `librechat/librechat.yaml`:

```yaml
mcpSettings:
  allowedDomains:
    - "mcp-server"
    - "http://mcp-server:3333"
```

Пересоздать LibreChat:

```bash
docker compose up -d --force-recreate librechat
```

## 17. LibreChat

Откройте:

```text
http://localhost:3080
```

### Создание пользователя

Если UI registration выдаёт:

```text
Registration is not allowed
```

создайте пользователя через script:

```bash
docker compose exec librechat npm run create-user
```

Введите:

```text
Email
Name
Username
Password
Email verified: y
```

После этого войдите в UI с указанными email/password.

Если браузер продолжает показывать проблемы с токеном:

- откройте LibreChat в incognito;
- или очистите cookies/localStorage для `localhost:3080`.

### Модели LibreChat

Модели для UI задаются в `.env`, а не напрямую в YAML:

```env
LIBRECHAT_MODELS=qwen2.5:7b,qwen2.5:14b,llama3.2-vision:latest
LIBRECHAT_TITLE_MODEL=qwen2.5:7b
LIBRECHAT_SUMMARY_MODEL=qwen2.5:7b
```

Для другого разработчика нужно заменить эти значения на модели из его `ollama list` или OpenAI-compatible runtime.

Проверить сгенерированный config внутри контейнера:

```bash
docker compose exec librechat sh -lc "sed -n '10,30p' /app/librechat.yaml"
```

## 18. Agent proxy и LLM tracing

Health:

```bash
curl http://localhost:3344/health
```

Ожидаемо:

```json
{"ok":true,"upstreamBaseUrl":"http://host.docker.internal:11434/v1","langfuseEnabled":true}
```

Проверить модели upstream provider:

```bash
curl http://localhost:3344/v1/models
```

Проверить non-streaming completion:

```bash
curl -fsS http://localhost:3344/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer local-dev-key' \
  --data '{"model":"qwen2.5:7b","messages":[{"role":"user","content":"Ответь одним словом: OK"}],"stream":false}'
```

Проверить streaming completion:

```bash
curl -fsS http://localhost:3344/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer local-dev-key' \
  --data '{"model":"qwen2.5:7b","messages":[{"role":"user","content":"Ответь одним словом: OK"}],"stream":true}'
```

Если в логах LibreChat есть `Failed to fetch models from openAI API` или `401`, проверьте, что в контейнер LibreChat не попадают `OPENAI_API_KEY` и `OPENAI_BASE_URL`:

```bash
docker compose exec librechat sh -lc 'env | grep -E "^(OPENAI_API_KEY|OPENAI_BASE_URL|AGENT_PROXY_)" | sort'
```

Ожидаемо должны быть только:

```text
AGENT_PROXY_API_KEY=...
AGENT_PROXY_BASE_URL=...
```

## 19. LangFuse

Откройте:

```text
http://localhost:3000
```

LangFuse использует отдельную PostgreSQL database:

```text
Server: langfuse-db
Database: langfuse
User: langfuse
Password: langfuse_password
```

Для production-like использования замените секреты в `docker-compose.yml`:

```text
NEXTAUTH_SECRET
SALT
```

LLM traces пишет `agent-proxy`, используя:

```env
LANGFUSE_PUBLIC_KEY
LANGFUSE_SECRET_KEY
```

После LLM-запроса из LibreChat проверьте traces/generations в LangFuse UI.

## 20. Grafana dashboards

Откройте:

```text
http://localhost:3001
```

Credentials:

```text
Login: admin
Password: admin
```

Datasource создаётся автоматически:

```text
ClickHouse Analytics
```

Dashboard:

```text
http://localhost:3001/d/agentic-data-stack-events/agentic-data-stack-events
```

Проверить Grafana health:

```bash
curl http://admin:admin@localhost:3001/api/health
```

Проверить dashboard:

```bash
curl -fsS 'http://admin:admin@localhost:3001/api/search?query=Agentic%20Data%20Stack%20Events'
```

Проверить запрос к ClickHouse через Grafana datasource:

```bash
curl -fsS http://admin:admin@localhost:3001/api/ds/query \
  -H 'Content-Type: application/json' \
  --data '{"queries":[{"refId":"A","datasource":{"type":"grafana-clickhouse-datasource","uid":"clickhouse-analytics"},"rawSql":"SELECT count() AS total_events FROM analytics.app_events_raw","format":1}],"from":"now-24h","to":"now"}'
```

Ожидаемо query вернёт `1000` для seeded dataset.

## 21. Airflow

Откройте:

```text
http://localhost:8081
```

Credentials:

```text
Login: admin
Password: admin
```

DAG:

```text
debezium_postgres_to_clickhouse
```

Airflow init контейнер завершает работу после миграции БД и создания пользователя. Это нормально:

```text
ads_airflow_init Exited
```

Проверить scheduler logs:

```bash
docker compose logs --tail=120 airflow-scheduler
```

## 22. Полная smoke-check последовательность

Выполнить после запуска:

```bash
docker compose ps
```

```bash
curl http://localhost:3333/health
```

```bash
curl http://localhost:3344/health
```

```bash
curl http://admin:admin@localhost:3001/api/health
```

```bash
curl http://localhost:8083/connectors
```

```bash
curl http://localhost:8083/connectors/postgres-app-events-source/status
```

```bash
curl http://localhost:8083/connectors/clickhouse-app-events-sink/status
```

```bash
curl 'http://localhost:8123/?user=analytics&password=analytics_password' \
  --data-binary 'SELECT count() FROM analytics.app_events_raw'
```

```bash
docker compose exec postgres psql -U app -d app_logs -c "SELECT count(*) FROM app_events;"
```

Если оба count равны `1000`, pipeline работает.

Проверить, что Grafana видит dashboard:

```bash
curl -fsS 'http://admin:admin@localhost:3001/api/search?query=Agentic%20Data%20Stack%20Events'
```

## 23. Пересоздание connectors

Если нужно пересоздать source:

```bash
curl -X DELETE http://localhost:8083/connectors/postgres-app-events-source
sleep 3
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @debezium/connectors/postgres-source.json
```

Если нужно пересоздать sink:

```bash
curl -X DELETE http://localhost:8083/connectors/clickhouse-app-events-sink
sleep 3
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @debezium/connectors/clickhouse-sink.json
```

## 24. Очистка ClickHouse таблицы

```bash
curl 'http://localhost:8123/?user=analytics&password=analytics_password' \
  --data-binary 'TRUNCATE TABLE analytics.app_events_raw'
```

После truncate старые Kafka offsets могут не переиграться автоматически. Для полного clean replay проще удалить volumes и поднять стек заново.

## 25. Полный сброс окружения

Осторожно: удалит volumes и все данные.

```bash
docker compose down -v
```

Затем:

```bash
docker compose up -d --build
```

И снова зарегистрировать connectors.

## 26. Частые ошибки и решения

### Ошибка: no matching manifest for linux/arm64/v8

Симптом:

```text
no matching manifest for linux/arm64/v8
```

Причина: образ не поддерживает ARM64.

Решение: добавить для сервиса:

```yaml
platform: linux/amd64
```

В проекте это уже сделано для `debezium-ui`.

### Ошибка: failed to fetch oauth token / unexpected EOF

Симптом:

```text
failed to authorize
failed to fetch oauth token
unexpected EOF
```

Причина: проблемы доступа к Docker Hub без VPN.

Решение:

```bash
docker compose pull
```

с включенным VPN.

### Ошибка: ClickHouseSinkConnector не найден

Проверка:

```bash
curl http://localhost:8083/connector-plugins
```

Если нет:

```text
com.clickhouse.kafka.connect.ClickHouseSinkConnector
```

проверьте JAR:

```bash
find debezium/plugins -type f -name '*.jar'
```

Перезапустите Debezium:

```bash
docker compose up -d --force-recreate debezium
```

### Ошибка: Table analytics.pg.public.app_events does not exist

Причина: ClickHouse sink по умолчанию ищет таблицу с именем Kafka topic.

Решение: в `debezium/connectors/clickhouse-sink.json` должен быть mapping:

```json
"topic2TableMap": "pg_flat.public.app_events=app_events_raw"
```

### Ошибка: Cannot parse input while reading event_time

Причина: Debezium отдаёт timestamp в ISO format, ClickHouse sink может не распарсить в `DateTime64`.

Решение в проекте: raw table хранит:

```sql
event_time String
```

а view парсит:

```sql
parseDateTimeBestEffortOrNull(event_time)
```

### Ошибка: LibreChat Registration is not allowed

Решение:

```bash
docker compose exec librechat npm run create-user
```

### Ошибка: LibreChat Invalid refresh token

Решение:

- открыть в incognito;
- очистить cookies/localStorage для `localhost:3080`;
- перелогиниться.

### Ошибка: LibreChat показывает `${...}` вместо имён моделей

Причина: LibreChat не подставляет env-переменные внутри YAML list напрямую.

Решение: не редактируйте `/app/librechat.yaml` вручную. Используйте:

```text
librechat/librechat.yaml.template
librechat/render-config.sh
LIBRECHAT_MODELS=...
```

Перезапуск:

```bash
docker compose up -d --force-recreate librechat
```

Проверка:

```bash
docker compose exec librechat sh -lc "sed -n '10,30p' /app/librechat.yaml"
```

### Ошибка: LibreChat получает 401 от OpenAI API

Причина: в контейнер LibreChat попали `OPENAI_API_KEY`/`OPENAI_BASE_URL`, и LibreChat активировал официальный OpenAI endpoint.

Решение: используйте только:

```env
AGENT_PROXY_API_KEY=local-dev-key
AGENT_PROXY_BASE_URL=http://agent-proxy:3344/v1
```

Проверка:

```bash
docker compose exec librechat sh -lc 'env | grep -E "^(OPENAI_API_KEY|OPENAI_BASE_URL|AGENT_PROXY_)" | sort'
```

Ожидаемо: `OPENAI_API_KEY` и `OPENAI_BASE_URL` отсутствуют.

### Ошибка: agent-proxy не видит Ollama

Проверьте Ollama на macOS host:

```bash
curl http://localhost:11434/v1/models
```

Проверьте proxy:

```bash
curl http://localhost:3344/health
```

В `.env` для Docker Desktop на macOS обычно нужно:

```env
UPSTREAM_OPENAI_BASE_URL=http://host.docker.internal:11434/v1
```

### Ошибка: Grafana не видит dashboard

Проверьте mount:

```bash
docker compose exec grafana ls -lah /var/lib/grafana/dashboards
```

Проверьте provider:

```bash
docker compose logs --tail=120 grafana
```

Dashboard должен быть доступен:

```text
http://localhost:3001/d/agentic-data-stack-events/agentic-data-stack-events
```

### Ошибка: Grafana datasource не подключается к ClickHouse

Проверьте datasource:

```bash
curl -fsS http://admin:admin@localhost:3001/api/datasources/name/ClickHouse%20Analytics
```

Проверьте запрос через datasource API:

```bash
curl -fsS http://admin:admin@localhost:3001/api/ds/query \
  -H 'Content-Type: application/json' \
  --data '{"queries":[{"refId":"A","datasource":{"type":"grafana-clickhouse-datasource","uid":"clickhouse-analytics"},"rawSql":"SELECT count() AS total_events FROM analytics.app_events_raw","format":1}],"from":"now-24h","to":"now"}'
```

### Ошибка: MCP Domain is not allowed

Решение: проверьте `librechat/librechat.yaml`:

```yaml
mcpSettings:
  allowedDomains:
    - "mcp-server"
    - "http://mcp-server:3333"
```

Перезапуск:

```bash
docker compose up -d --force-recreate librechat
```

### Debezium UI не показывает connectors

Проверяйте REST:

```bash
curl http://localhost:8083/connectors
```

Если REST показывает connectors и statuses `RUNNING`, pipeline работает независимо от UI.

## 27. Useful logs

```bash
docker compose logs --tail=120 postgres
```

```bash
docker compose logs --tail=120 clickhouse
```

```bash
docker compose logs --tail=200 debezium
```

```bash
docker compose logs --tail=120 debezium-ui
```

```bash
docker compose logs --tail=120 librechat
```

```bash
docker compose logs --tail=120 mcp-server
```

```bash
docker compose logs --tail=120 agent-proxy
```

```bash
docker compose logs --tail=120 grafana
```

```bash
docker compose logs --tail=120 airflow-webserver
```

```bash
docker compose logs --tail=120 airflow-scheduler
```

## 28. Итоговая проверка успешности

Проект считается успешно запущенным, если выполняются условия:

- `docker compose ps` показывает сервисы `Up`.
- `curl http://localhost:3333/health` возвращает `{"ok":true}`.
- `curl http://localhost:3344/health` возвращает `langfuseEnabled:true`.
- `curl http://admin:admin@localhost:3001/api/health` возвращает `database: ok`.
- `curl http://localhost:8083/connectors` возвращает оба connector’а.
- Status обоих Debezium connector’ов: `RUNNING`.
- PostgreSQL `public.app_events` содержит `1000` строк.
- ClickHouse `analytics.app_events_raw` содержит `1000` строк.
- ClickHouse UI открывается на `http://localhost:8123/play`.
- Adminer открывается на `http://localhost:8082`.
- Grafana dashboard открывается на `http://localhost:3001/d/agentic-data-stack-events/agentic-data-stack-events`.
- LibreChat открывается на `http://localhost:3080` и пользователь создан через `npm run create-user`.
- LibreChat logs показывают MCP tools:

```text
Tools: event_summary, run_readonly_query
```
