# Quickstart: Agentic Data Stack

Краткая инструкция для запуска проекта: PostgreSQL, Debezium, Redpanda, ClickHouse, Airflow, LangFuse, LibreChat, MCP server, UI для ClickHouse и PostgreSQL.

## 1. Требования

- Docker Desktop
- Docker Compose v2
- Свободные порты:
  - `3000` LangFuse
  - `3080` LibreChat
  - `3333` MCP server
  - `5432` PostgreSQL
  - `8080` Debezium UI
  - `8081` Airflow
  - `8082` Adminer PostgreSQL UI
  - `8083` Debezium Connect REST
  - `8123` ClickHouse HTTP/UI
  - `9000` ClickHouse native
  - `9092` Redpanda/Kafka
  - `9644` Redpanda admin

## 2. Если Docker Hub плохо качается

Если без VPN появляются ошибки вроде:

```text
failed to fetch oauth token
unexpected EOF
no matching manifest
```

Сначала включите VPN и скачайте образы:

```bash
docker compose pull
```

Потом запускайте стек.

## 3. Запуск

Если `.env` отсутствует:

```bash
cp .env.example .env
```

Если у вас другие локальные модели Ollama/HuggingFace-runtime, поменяйте в `.env`:

```env
LIBRECHAT_MODELS=your-fast-model:latest,your-smart-model:latest
LIBRECHAT_TITLE_MODEL=your-fast-model:latest
LIBRECHAT_SUMMARY_MODEL=your-fast-model:latest
OPENAI_MODEL=your-fast-model:latest
UPSTREAM_OPENAI_BASE_URL=http://host.docker.internal:11434/v1
```

`librechat/librechat.yaml` вручную менять не нужно: он генерируется из `librechat/librechat.yaml.template` при старте контейнера.

```bash
docker compose up -d --build
```

Если образы уже скачаны и код не менялся:

```bash
docker compose up -d
```

Проверить контейнеры:

```bash
docker compose ps
```

## 4. Проверка базовых сервисов

MCP server:

```bash
curl http://localhost:3333/health
```

Ожидаемо:

```json
{"ok":true}
```

ClickHouse:

```bash
curl 'http://localhost:8123/?user=analytics&password=analytics_password' \
  --data-binary 'SELECT 1'
```

PostgreSQL seed:

```bash
docker compose exec postgres psql -U app -d app_logs -c "SELECT count(*) FROM app_events;"
```

Ожидаемо: `1000` строк.

## 5. Регистрация Debezium connectors

Проверить доступные plugins:

```bash
curl http://localhost:8083/connector-plugins
```

Должны быть:

```text
io.debezium.connector.postgresql.PostgresConnector
com.clickhouse.kafka.connect.ClickHouseSinkConnector
```

Зарегистрировать source connector:

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @debezium/connectors/postgres-source.json
```

Зарегистрировать sink connector:

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @debezium/connectors/clickhouse-sink.json
```

Если получили `409 Conflict`, connector уже существует.

Проверить список:

```bash
curl http://localhost:8083/connectors
```

Ожидаемо:

```json
["postgres-app-events-source","clickhouse-app-events-sink"]
```

Проверить статусы:

```bash
curl http://localhost:8083/connectors/postgres-app-events-source/status
curl http://localhost:8083/connectors/clickhouse-app-events-sink/status
```

Ожидаемо: `RUNNING` у connector и task.

## 6. Проверка данных в ClickHouse

Количество строк:

```bash
curl 'http://localhost:8123/?user=analytics&password=analytics_password' \
  --data-binary 'SELECT count() FROM analytics.app_events_raw'
```

Ожидаемо:

```text
1000
```

Агрегация:

```bash
curl 'http://localhost:8123/?user=analytics&password=analytics_password' \
  --data-binary 'SELECT event_type, count() FROM analytics.app_events_raw GROUP BY event_type ORDER BY count() DESC FORMAT PrettyCompact'
```

## 7. UI адреса

- ClickHouse UI: http://localhost:8123/play
- PostgreSQL UI/Adminer: http://localhost:8082
- Grafana dashboards: http://localhost:3001
- Agent proxy: http://localhost:3344/health
- LibreChat: http://localhost:3080
- LangFuse: http://localhost:3000
- Airflow: http://localhost:8081
- Debezium UI: http://localhost:8080
- Debezium REST: http://localhost:8083

## 8. Доступы

ClickHouse:

```text
User: analytics
Password: analytics_password
Database: analytics
```

Adminer для PostgreSQL app logs:

```text
System: PostgreSQL
Server: postgres
Username: app
Password: app_password
Database: app_logs
```

Adminer для LangFuse DB:

```text
System: PostgreSQL
Server: langfuse-db
Username: langfuse
Password: langfuse_password
Database: langfuse
```

Airflow:

```text
Login: admin
Password: admin
```

Grafana:

```text
Login: admin
Password: admin
```

Grafana datasource `ClickHouse Analytics` создаётся автоматически.
Dashboard:

```text
http://localhost:3001/d/agentic-data-stack-events/agentic-data-stack-events
```

## 9. LibreChat пользователь

Если регистрация через UI запрещена, создайте пользователя командой:

```bash
docker compose exec librechat npm run create-user
```

Следуйте prompts и задайте email/password.

## 10. Проверка LLM proxy и LangFuse tracing

LibreChat ходит в локальные модели через `agent-proxy`:

```text
LibreChat -> agent-proxy -> Ollama
                       -> LangFuse
```

Health:

```bash
curl http://localhost:3344/health
```

Тестовый LLM-вызов:

```bash
curl -fsS http://localhost:3344/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer local-dev-key' \
  --data '{"model":"qwen2.5:7b","messages":[{"role":"user","content":"Ответь одним словом: OK"}],"stream":false}'
```

Streaming check:

```bash
curl -fsS http://localhost:3344/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer local-dev-key' \
  --data '{"model":"qwen2.5:7b","messages":[{"role":"user","content":"Ответь одним словом: OK"}],"stream":true}'
```

## 11. Частые ошибки

### Docker Hub скачивается только с VPN

Симптом:

```text
failed to fetch oauth token
unexpected EOF
```

Решение:

```bash
docker compose pull
```

с включенным VPN.

### Debezium UI показывает Server API problem

Проверяйте connectors через REST API:

```bash
curl http://localhost:8083/connectors
```

Debezium UI может некорректно отображать состояние, но REST API является источником правды.

### ClickHouse sink FAILED

Проверить статус:

```bash
curl http://localhost:8083/connectors/clickhouse-app-events-sink/status
```

Пересоздать sink:

```bash
curl -X DELETE http://localhost:8083/connectors/clickhouse-app-events-sink
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @debezium/connectors/clickhouse-sink.json
```

### LibreChat MCP domain not allowed

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

## 12. Остановка

Остановить контейнеры:

```bash
docker compose down
```

Остановить и удалить volumes:

```bash
docker compose down -v
```
