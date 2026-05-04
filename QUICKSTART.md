# Quickstart

## 1. Подготовка

```bash
cd /Users/subbotaevgenij/PycharmProjects/Clicker/CascadeProjects/Agentic-Data-Stack
cp .env.example .env
```

Если Ollama запущена на macOS, Docker-контейнеры увидят ее через:

```env
UPSTREAM_OPENAI_BASE_URL=http://host.docker.internal:11434/v1
```

## 2. Запуск

```bash
docker compose up -d --build
```

Если заняты порты `3001`, `3080`, `3333`, `3344`, `5432`, `8083`, `8123`, `9000`, `9092` или `9644`, остановите старый стек:

```bash
cd /Users/subbotaevgenij/PycharmProjects/Clicker/CascadeProjects/2048
docker compose down
```

## 3. Проверка

```bash
docker compose ps
curl http://localhost:3333/health
curl http://localhost:3344/health
curl http://localhost:8083/connectors
```

ClickHouse:

```bash
curl 'http://localhost:8123/?user=analytics&password=analytics_password' \
  --data-binary 'SELECT count() FROM analytics.app_events_raw'
```

Ожидаемо после initial snapshot: `1000`.

## 4. UI

- LibreChat: `http://localhost:3080`
- Grafana: `http://localhost:3001`
- Dashboard: `http://localhost:3001/d/agentic-data-stack-events/agentic-data-stack-events`
- ClickHouse UI: `http://localhost:8123/play`

Grafana login/password из `.env`:

```text
admin / admin
```

## 5. LibreChat

В LibreChat выберите `Local OpenAI-compatible` endpoint и включите MCP tools `clickhouse-analytics`.

Рабочие запросы:

```text
Проанализируй routes по error rate и p95 latency.
```

```text
Визуализируй error rate по routes и верни ссылку на Grafana.
```

```text
Построй график количества логов по времени с разбивкой по event_type.
```
