# Bare Ubuntu deployment: Agentic Data Stack на 3 серверах

Этот документ описывает минимальное развертывание Agentic Data Stack на трёх чистых Ubuntu Server, связанных приватной VPN-сетью Tailscale.

Документ содержит только обязательные компоненты. JS-код здесь не приводится: `mcp-server` и `agent-proxy` запускаются как готовые container images.

___
Задача Agentic Data Stack - анализ логов ClickHouse, анализ таблиц и данных с помощью LLM

В моей доке три машины
- 1 - БД Postgre
- 2 - Debezium + Airflow
- 3 - Вот этот Agentic Data Stack

Из Postge данные будут мигрировать в clickHouse с помощью debezium

Airfow здесь - это sheduler для Debezium.

Как только данные залетели в ClickHouse их можно анализировать в LibreChat локальными или облачными модельками

LangFuse даёт свою собственную аналитику не по ClickHouse а по тому, как отвечают модели

## Содержание

1. [Итоговая архитектура](#1-итоговая-архитектура)
2. [Имена, порты и переменные](#2-имена-порты-и-переменные)
3. [Общая подготовка всех серверов](#3-общая-подготовка-всех-серверов)
4. [Tailscale VPN](#4-tailscale-vpn)
5. [Machine 3: source database node](#5-machine-3-source-database-node)
6. [Machine 1: AI/data node](#6-machine-1-aidata-node)
7. [Machine 2: pipeline node](#7-machine-2-pipeline-node)
8. [Debezium migration scenarios](#8-debezium-migration-scenarios)
9. [Adminer и UI-доступы](#9-adminer-и-ui-доступы)
10. [End-to-end smoke check](#10-end-to-end-smoke-check)
11. [Troubleshooting](#11-troubleshooting)
12. [Production notes](#12-production-notes)

## Как читать команды

Каждый шаг оформлен одинаково:

- **Команда** — что выполнить.
- **Проверка** — чем проверить результат.
- **Ожидаемо** — что должно получиться.

Команды выполняются на конкретной машине, указанной в заголовке раздела.

---

## 1. Итоговая архитектура

### 1.1. Распределение по машинам

```text
Machine 1: ai-data-node
  - ClickHouse
  - MCP server
  - LibreChat
  - LibreChat MongoDB
  - LangFuse
  - LangFuse PostgreSQL
  - LLM runtime, например Ollama
  - agent-proxy для LangFuse tracing LLM-вызовов
  - Adminer для просмотра ClickHouse

Machine 2: pipeline-node
  - Redpanda или Kafka
  - Debezium Connect
  - Debezium UI
  - Airflow webserver
  - Airflow scheduler
  - Airflow metadata PostgreSQL

Machine 3: source-db-node
  - PostgreSQL source database
  - Adminer для просмотра PostgreSQL
```

### 1.2. Data flow

```text
PostgreSQL source DB
  -> Debezium PostgreSQL Source Connector
  -> Redpanda/Kafka topic
  -> ClickHouse Sink Connector
  -> ClickHouse
  -> MCP server
  -> LibreChat

LibreChat
  -> agent-proxy
  -> LLM runtime
  -> LangFuse traces
```

### 1.3. Почему нужен Redpanda/Kafka

Debezium Connect не пишет напрямую из PostgreSQL в ClickHouse. Он публикует CDC events в Kafka-compatible broker. Поэтому минимальный pipeline требует:

```text
PostgreSQL -> Debezium -> Kafka/Redpanda -> ClickHouse Sink -> ClickHouse
```

---

## 2. Имена, порты и переменные

### 2.1. Tailscale hostnames

В документе используются MagicDNS-имена:

```text
ai-data-node
pipeline-node
source-db-node
```

Если MagicDNS отключён, замените имена на Tailscale IP вида `100.x.x.x`.

### 2.2. Основные порты

| Машина | Порт | Сервис | Назначение |
|---|---:|---|---|
| `ai-data-node` | `8123` | ClickHouse HTTP/UI | SQL HTTP API и `/play` |
| `ai-data-node` | `9000` | ClickHouse native | Native protocol |
| `ai-data-node` | `3333` | MCP server | MCP endpoint/health |
| `ai-data-node` | `3344` | agent-proxy | OpenAI-compatible proxy |
| `ai-data-node` | `3000` | LangFuse | LangFuse UI/API |
| `ai-data-node` | `3080` | LibreChat | Chat UI |
| `ai-data-node` | `11434` | Ollama | LLM runtime |
| `ai-data-node` | `8082` | Adminer | ClickHouse/PostgreSQL UI |
| `pipeline-node` | `9092` | Redpanda/Kafka | Kafka API |
| `pipeline-node` | `9644` | Redpanda admin | Admin API |
| `pipeline-node` | `8083` | Debezium Connect | Connect REST API |
| `pipeline-node` | `8080` | Debezium UI | Connect UI |
| `pipeline-node` | `8081` | Airflow | Airflow UI |
| `source-db-node` | `5432` | PostgreSQL | Source DB |
| `source-db-node` | `8082` | Adminer | PostgreSQL UI |

### 2.3. Demo credentials

Для production обязательно заменить.

```text
PostgreSQL source:
  DB: app_logs
  User: app
  Password: app_password

ClickHouse:
  DB: analytics
  User: analytics
  Password: analytics_password

Airflow:
  User: admin
  Password: admin

LangFuse DB:
  DB: langfuse
  User: langfuse
  Password: langfuse_password
```

---

## 3. Общая подготовка всех серверов

Выполнить на каждой машине: `ai-data-node`, `pipeline-node`, `source-db-node`.

### 3.1. Обновить ОС

**Команда:**

```bash
sudo apt update && sudo apt upgrade -y
```

**Проверка:**

```bash
lsb_release -a && uname -a
```

**Ожидаемо:** Ubuntu Server и версия ядра выводятся без ошибок.

### 3.2. Установить базовые утилиты

**Команда:**

```bash
sudo apt install -y ca-certificates curl gnupg lsb-release jq net-tools htop unzip nano
```

**Проверка:**

```bash
curl --version && jq --version
```

**Ожидаемо:** обе команды выводят версии.

### 3.3. Установить Docker Engine

**Команда:**

```bash
sudo install -m 0755 -d /etc/apt/keyrings
```

**Проверка:**

```bash
ls -ld /etc/apt/keyrings
```

**Команда:**

```bash
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
```

**Проверка:**

```bash
ls -l /etc/apt/keyrings/docker.gpg
```

**Команда:**

```bash
sudo chmod a+r /etc/apt/keyrings/docker.gpg
```

**Проверка:**

```bash
stat -c '%a %n' /etc/apt/keyrings/docker.gpg
```

**Команда:**

```bash
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
```

**Проверка:**

```bash
cat /etc/apt/sources.list.d/docker.list
```

**Команда:**

```bash
sudo apt update
```

**Проверка:**

```bash
apt-cache policy docker-ce | sed -n '1,20p'
```

**Команда:**

```bash
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

**Проверка:**

```bash
docker --version && docker compose version
```

### 3.4. Разрешить текущему пользователю запускать Docker

**Команда:**

```bash
sudo usermod -aG docker $USER
```

**Проверка:**

```bash
id $USER
```

**Ожидаемо:** в списке групп есть `docker`.

После этого перелогиньтесь по SSH.

**Проверка после перелогина:**

```bash
docker run --rm hello-world
```

**Ожидаемо:** Docker выводит `Hello from Docker!`.

### 3.5. Создать рабочую директорию

**Команда:**

```bash
sudo mkdir -p /opt/agentic-data-stack && sudo chown -R $USER:$USER /opt/agentic-data-stack
```

**Проверка:**

```bash
stat -c '%U:%G %n' /opt/agentic-data-stack
```

**Ожидаемо:** владелец — текущий пользователь.

---

## 4. Tailscale VPN

Выполнить на каждой машине.

### 4.1. Установить Tailscale

**Команда:**

```bash
curl -fsSL https://tailscale.com/install.sh | sh
```

**Проверка:**

```bash
tailscale version
```

### 4.2. Подключить машины к tailnet

На `ai-data-node`:

```bash
sudo tailscale up --hostname=ai-data-node --ssh
```

Проверка:

```bash
tailscale status
```

На `pipeline-node`:

```bash
sudo tailscale up --hostname=pipeline-node --ssh
```

Проверка:

```bash
tailscale status
```

На `source-db-node`:

```bash
sudo tailscale up --hostname=source-db-node --ssh
```

Проверка:

```bash
tailscale status
```

**Ожидаемо:** все три машины видны в одном tailnet.

### 4.3. Проверить связность

На `pipeline-node`:

```bash
ping -c 3 source-db-node
ping -c 3 ai-data-node
```

Проверка: в обоих случаях `3 received`.

На `ai-data-node`:

```bash
ping -c 3 pipeline-node
```

Проверка: `3 received`.

### 4.4. Firewall только через Tailscale

На `source-db-node`:

```bash
sudo ufw allow in on tailscale0 to any port 5432 proto tcp
```

Проверка:

```bash
sudo ufw status verbose | grep 5432
```

На `ai-data-node`:

```bash
for port in 8123 9000 3333 3344 3000 3080 11434 8082; do sudo ufw allow in on tailscale0 to any port $port proto tcp; done
```

Проверка:

```bash
sudo ufw status verbose | grep -E '8123|9000|3333|3344|3000|3080|11434|8082'
```

На `pipeline-node`:

```bash
for port in 8083 8080 8081 9092 9644; do sudo ufw allow in on tailscale0 to any port $port proto tcp; done
```

Проверка:

```bash
sudo ufw status verbose | grep -E '8083|8080|8081|9092|9644'
```

---

## 5. Machine 3: source database node

Выполнять на `source-db-node`.

### 5.1. Цель раздела

Поднять PostgreSQL с logical replication и Adminer для просмотра source DB.

### 5.2. Создать директории и `.env`

```bash
mkdir -p /opt/agentic-data-stack/source-db/postgres-init
```

Проверка:

```bash
ls -ld /opt/agentic-data-stack/source-db/postgres-init
```

```bash
cat > /opt/agentic-data-stack/source-db/.env <<'ENV'
POSTGRES_DB=app_logs
POSTGRES_USER=app
POSTGRES_PASSWORD=app_password
ENV
```

Проверка:

```bash
grep -E 'POSTGRES_DB|POSTGRES_USER' /opt/agentic-data-stack/source-db/.env
```

### 5.3. Создать demo schema и seed data

```bash
cat > /opt/agentic-data-stack/source-db/postgres-init/001_schema.sql <<'SQL'
CREATE TABLE IF NOT EXISTS public.app_events
(
  id BIGSERIAL PRIMARY KEY,
  event_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  user_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  route TEXT,
  status_code INTEGER,
  latency_ms INTEGER,
  model_name TEXT,
  prompt_tokens INTEGER,
  completion_tokens INTEGER,
  total_cost_usd NUMERIC(12, 6),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

INSERT INTO public.app_events
  (user_id, session_id, event_type, route, status_code, latency_ms, model_name, prompt_tokens, completion_tokens, total_cost_usd, metadata)
SELECT
  'user_' || (g % 50),
  'session_' || (g % 200),
  CASE WHEN g % 10 = 0 THEN 'error' WHEN g % 3 = 0 THEN 'llm_call' ELSE 'page_view' END,
  '/api/' || (g % 20),
  CASE WHEN g % 10 = 0 THEN 500 ELSE 200 END,
  20 + (g % 500),
  CASE WHEN g % 3 = 0 THEN 'qwen2.5:7b' ELSE NULL END,
  CASE WHEN g % 3 = 0 THEN 100 + (g % 1000) ELSE NULL END,
  CASE WHEN g % 3 = 0 THEN 50 + (g % 500) ELSE NULL END,
  CASE WHEN g % 3 = 0 THEN ((g % 100)::numeric / 10000) ELSE NULL END,
  jsonb_build_object('source', 'seed', 'n', g)
FROM generate_series(1, 1000) AS g;
SQL
```

Проверка:

```bash
sed -n '1,30p' /opt/agentic-data-stack/source-db/postgres-init/001_schema.sql
```

### 5.4. Создать Compose file

```bash
cat > /opt/agentic-data-stack/source-db/docker-compose.yml <<'YAML'
services:
  postgres:
    image: postgres:16-alpine
    container_name: source_postgres
    env_file: .env
    command:
      - postgres
      - -c
      - wal_level=logical
      - -c
      - max_wal_senders=10
      - -c
      - max_replication_slots=10
      - -c
      - listen_addresses=*
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./postgres-init:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app -d app_logs"]
      interval: 10s
      timeout: 5s
      retries: 10

  adminer:
    image: adminer:4
    container_name: source_adminer
    depends_on:
      postgres:
        condition: service_healthy
    ports:
      - "8082:8080"

volumes:
  postgres_data:
YAML
```

Проверка:

```bash
docker compose -f /opt/agentic-data-stack/source-db/docker-compose.yml config >/dev/null && echo OK
```

### 5.5. Запустить PostgreSQL и Adminer

```bash
docker compose -f /opt/agentic-data-stack/source-db/docker-compose.yml up -d
```

Проверка:

```bash
docker compose -f /opt/agentic-data-stack/source-db/docker-compose.yml ps
```

Ожидаемо: `source_postgres` и `source_adminer` в состоянии `Up`.

### 5.6. Проверить PostgreSQL

```bash
docker exec source_postgres pg_isready -U app -d app_logs
```

Ожидаемо: `accepting connections`.

```bash
docker exec source_postgres psql -U app -d app_logs -c "SHOW wal_level;"
```

Ожидаемо: `logical`.

```bash
docker exec source_postgres psql -U app -d app_logs -c "SELECT count(*) FROM public.app_events;"
```

Ожидаемо: `1000`.

### 5.7. Проверить доступ с pipeline node

Выполнить на `pipeline-node`.

```bash
sudo apt install -y postgresql-client
```

Проверка:

```bash
psql --version
```

```bash
psql "postgresql://app:app_password@source-db-node:5432/app_logs" -c "SELECT count(*) FROM public.app_events;"
```

Ожидаемо: `1000`.

---

## 6. Machine 1: AI/data node

Выполнять на `ai-data-node`.

### 6.1. Цель раздела

Поднять ClickHouse, LLM runtime, LangFuse, MCP server, LibreChat и Adminer.

### 6.2. Создать директории и `.env`

```bash
mkdir -p /opt/agentic-data-stack/ai-data/{clickhouse-init,librechat}
```

Проверка:

```bash
find /opt/agentic-data-stack/ai-data -maxdepth 2 -type d | sort
```

```bash
cat > /opt/agentic-data-stack/ai-data/.env <<'ENV'
CLICKHOUSE_DB=analytics
CLICKHOUSE_USER=analytics
CLICKHOUSE_PASSWORD=analytics_password

LANGFUSE_DB=langfuse
LANGFUSE_DB_USER=langfuse
LANGFUSE_DB_PASSWORD=langfuse_password
LANGFUSE_NEXTAUTH_SECRET=replace-with-long-random-secret
LANGFUSE_SALT=replace-with-long-random-salt
LANGFUSE_HOST=http://ai-data-node:3000
LANGFUSE_PUBLIC_KEY=replace-after-langfuse-project-created
LANGFUSE_SECRET_KEY=replace-after-langfuse-project-created

LIBRECHAT_JWT_SECRET=replace-with-long-random-secret
LIBRECHAT_JWT_REFRESH_SECRET=replace-with-long-random-secret
AGENT_PROXY_API_KEY=local-dev-key
AGENT_PROXY_BASE_URL=http://agent-proxy:3344/v1

UPSTREAM_OPENAI_API_KEY=local-dev-key
UPSTREAM_OPENAI_BASE_URL=http://ollama:11434/v1
ENV
```

Проверка:

```bash
grep -E 'CLICKHOUSE_DB|LANGFUSE_HOST|AGENT_PROXY_BASE_URL|UPSTREAM_OPENAI_BASE_URL' /opt/agentic-data-stack/ai-data/.env
```

### 6.3. Подготовить ClickHouse schema для варианта B

Если выбираете Debezium вариант B, создайте schema заранее.

```bash
cat > /opt/agentic-data-stack/ai-data/clickhouse-init/001_schema.sql <<'SQL'
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.app_events_raw
(
  id UInt64,
  event_time String,
  user_id String,
  session_id String,
  event_type String,
  route Nullable(String),
  status_code Nullable(Int32),
  latency_ms Nullable(Int32),
  model_name Nullable(String),
  prompt_tokens Nullable(Int32),
  completion_tokens Nullable(Int32),
  total_cost_usd Nullable(Decimal(12, 6)),
  metadata String,
  ingest_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (event_time, id);

CREATE VIEW IF NOT EXISTS analytics.v_event_summary AS
SELECT
  toStartOfHour(parseDateTimeBestEffortOrNull(event_time)) AS hour,
  event_type,
  count() AS events,
  uniqExact(user_id) AS users,
  avgOrNull(latency_ms) AS avg_latency_ms,
  sumOrNull(total_cost_usd) AS total_cost_usd
FROM analytics.app_events_raw
GROUP BY hour, event_type
ORDER BY hour DESC, event_type;
SQL
```

Проверка:

```bash
sed -n '1,80p' /opt/agentic-data-stack/ai-data/clickhouse-init/001_schema.sql
```

Если выбираете вариант A, оставьте только:

```sql
CREATE DATABASE IF NOT EXISTS analytics;
```

### 6.4. Подготовить LibreChat config

```bash
cat > /opt/agentic-data-stack/ai-data/librechat/librechat.yaml <<'YAML'
version: 1.2.1

cache: true

mcpSettings:
  allowedDomains:
    - "mcp-server"
    - "http://mcp-server:3333"

endpoints:
  custom:
    - name: "Local OpenAI-compatible"
      apiKey: "${AGENT_PROXY_API_KEY}"
      baseURL: "${AGENT_PROXY_BASE_URL}"
      models:
        default:
          - "qwen2.5:7b"
        fetch: true
      titleConvo: true
      titleModel: "qwen2.5:7b"
      summarize: false
      summaryModel: "qwen2.5:7b"

mcpServers:
  clickhouse-analytics:
    type: streamable-http
    url: http://mcp-server:3333/mcp
YAML
```

Проверка:

```bash
sed -n '1,80p' /opt/agentic-data-stack/ai-data/librechat/librechat.yaml
```

### 6.5. Создать Compose file

`mcp-server` и `agent-proxy` указаны как готовые образы. Замените `your-registry/...` на реальные имена.

```bash
cat > /opt/agentic-data-stack/ai-data/docker-compose.yml <<'YAML'
services:
  clickhouse:
    image: clickhouse/clickhouse-server:24.8
    container_name: ai_clickhouse
    environment:
      CLICKHOUSE_DB: ${CLICKHOUSE_DB}
      CLICKHOUSE_USER: ${CLICKHOUSE_USER}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD}
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - ./clickhouse-init:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "clickhouse-client --query 'SELECT 1'"]
      interval: 10s
      timeout: 5s
      retries: 10

  langfuse-db:
    image: postgres:16-alpine
    container_name: ai_langfuse_db
    environment:
      POSTGRES_DB: ${LANGFUSE_DB}
      POSTGRES_USER: ${LANGFUSE_DB_USER}
      POSTGRES_PASSWORD: ${LANGFUSE_DB_PASSWORD}
    volumes:
      - langfuse_db_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${LANGFUSE_DB_USER} -d ${LANGFUSE_DB}"]
      interval: 10s
      timeout: 5s
      retries: 10

  langfuse:
    image: langfuse/langfuse:2
    container_name: ai_langfuse
    depends_on:
      langfuse-db:
        condition: service_healthy
    environment:
      DATABASE_URL: postgresql://${LANGFUSE_DB_USER}:${LANGFUSE_DB_PASSWORD}@langfuse-db:5432/${LANGFUSE_DB}
      NEXTAUTH_SECRET: ${LANGFUSE_NEXTAUTH_SECRET}
      SALT: ${LANGFUSE_SALT}
      NEXTAUTH_URL: ${LANGFUSE_HOST}
      TELEMETRY_ENABLED: "false"
    ports:
      - "3000:3000"

  ollama:
    image: ollama/ollama:latest
    container_name: ai_ollama
    ports:
      - "11434:11434"
    volumes:
      - ollama_data:/root/.ollama

  agent-proxy:
    image: your-registry/agent-proxy:latest
    container_name: ai_agent_proxy
    depends_on:
      - langfuse
      - ollama
    environment:
      PORT: 3344
      UPSTREAM_OPENAI_API_KEY: ${UPSTREAM_OPENAI_API_KEY}
      UPSTREAM_OPENAI_BASE_URL: ${UPSTREAM_OPENAI_BASE_URL}
      LANGFUSE_HOST: http://langfuse:3000
      LANGFUSE_PUBLIC_KEY: ${LANGFUSE_PUBLIC_KEY}
      LANGFUSE_SECRET_KEY: ${LANGFUSE_SECRET_KEY}
    ports:
      - "3344:3344"

  mcp-server:
    image: your-registry/clickhouse-mcp-server:latest
    container_name: ai_mcp_server
    depends_on:
      clickhouse:
        condition: service_healthy
    environment:
      CLICKHOUSE_HOST: http://clickhouse:8123
      CLICKHOUSE_USER: ${CLICKHOUSE_USER}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD}
      CLICKHOUSE_DATABASE: ${CLICKHOUSE_DB}
      PORT: 3333
    ports:
      - "3333:3333"

  librechat-db:
    image: mongo:7
    container_name: ai_librechat_mongo
    volumes:
      - librechat_mongo_data:/data/db

  librechat:
    image: ghcr.io/danny-avila/librechat:latest
    container_name: ai_librechat
    depends_on:
      - librechat-db
      - agent-proxy
      - mcp-server
    environment:
      HOST: 0.0.0.0
      MONGO_URI: mongodb://librechat-db:27017/LibreChat
      DOMAIN_CLIENT: http://ai-data-node:3080
      DOMAIN_SERVER: http://ai-data-node:3080
      JWT_SECRET: ${LIBRECHAT_JWT_SECRET}
      JWT_REFRESH_SECRET: ${LIBRECHAT_JWT_REFRESH_SECRET}
      AGENT_PROXY_API_KEY: ${AGENT_PROXY_API_KEY}
      AGENT_PROXY_BASE_URL: ${AGENT_PROXY_BASE_URL}
      LANGFUSE_HOST: ${LANGFUSE_HOST}
      LANGFUSE_PUBLIC_KEY: ${LANGFUSE_PUBLIC_KEY}
      LANGFUSE_SECRET_KEY: ${LANGFUSE_SECRET_KEY}
      NO_INDEX: "true"
    ports:
      - "3080:3080"
    volumes:
      - ./librechat/librechat.yaml:/app/librechat.yaml:ro

  adminer:
    image: adminer:4
    container_name: ai_adminer
    ports:
      - "8082:8080"

volumes:
  clickhouse_data:
  langfuse_db_data:
  librechat_mongo_data:
  ollama_data:
YAML
```

Проверка:

```bash
docker compose -f /opt/agentic-data-stack/ai-data/docker-compose.yml --env-file /opt/agentic-data-stack/ai-data/.env config >/dev/null && echo OK
```

### 6.6. Запустить и проверить Machine 1

```bash
docker compose -f /opt/agentic-data-stack/ai-data/docker-compose.yml --env-file /opt/agentic-data-stack/ai-data/.env up -d
```

Проверка:

```bash
docker compose -f /opt/agentic-data-stack/ai-data/docker-compose.yml --env-file /opt/agentic-data-stack/ai-data/.env ps
```

Проверки сервисов:

```bash
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'SELECT 1'
curl -I http://ai-data-node:3000
curl http://ai-data-node:11434/api/tags
curl http://ai-data-node:3344/health
curl http://ai-data-node:3333/health
curl -I http://ai-data-node:3080
curl -I http://ai-data-node:8082
```

Ожидаемо: ClickHouse возвращает `1`, остальные endpoints отвечают без network error.

Загрузить demo LLM модель:

```bash
docker exec ai_ollama ollama pull qwen2.5:7b
```

Проверка:

```bash
docker exec ai_ollama ollama list
```

---

## 7. Machine 2: pipeline node

Выполнять на `pipeline-node`.

### 7.1. Цель раздела

Поднять Kafka-compatible broker, Debezium Connect, Debezium UI и Airflow.

### 7.2. Создать директории и `.env`

```bash
mkdir -p /opt/agentic-data-stack/pipeline/{debezium/connectors,debezium/plugins,airflow/dags}
```

Проверка:

```bash
find /opt/agentic-data-stack/pipeline -maxdepth 3 -type d | sort
```

```bash
cat > /opt/agentic-data-stack/pipeline/.env <<'ENV'
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.local
AIRFLOW_DB=airflow
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow_password
ENV
```

Проверка:

```bash
grep -E 'AIRFLOW_ADMIN_USER|AIRFLOW_DB' /opt/agentic-data-stack/pipeline/.env
```

### 7.3. Установить ClickHouse Kafka Connect plugin

Положите ClickHouse Kafka Connect plugin JAR в:

```text
/opt/agentic-data-stack/pipeline/debezium/plugins
```

Проверка:

```bash
find /opt/agentic-data-stack/pipeline/debezium/plugins -type f -name '*.jar'
```

Ожидаемо: виден JAR `clickhouse-kafka-connect`.

### 7.4. Создать Compose file

```bash
cat > /opt/agentic-data-stack/pipeline/docker-compose.yml <<'YAML'
services:
  redpanda:
    image: redpandadata/redpanda:v24.2.8
    container_name: pipeline_redpanda
    command:
      - redpanda
      - start
      - --overprovisioned
      - --smp=1
      - --memory=1G
      - --reserve-memory=0M
      - --node-id=0
      - --check=false
      - --kafka-addr=PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr=PLAINTEXT://pipeline-node:9092
    ports:
      - "9092:9092"
      - "9644:9644"
    volumes:
      - redpanda_data:/var/lib/redpanda/data
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -E 'Healthy:.+true' || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 10

  debezium:
    image: debezium/connect:2.7.3.Final
    container_name: pipeline_debezium
    depends_on:
      redpanda:
        condition: service_healthy
    environment:
      BOOTSTRAP_SERVERS: pipeline-node:9092
      GROUP_ID: ads-connect
      CONFIG_STORAGE_TOPIC: ads_connect_configs
      OFFSET_STORAGE_TOPIC: ads_connect_offsets
      STATUS_STORAGE_TOPIC: ads_connect_statuses
      KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE: "false"
      CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
      CONNECT_PLUGIN_PATH: /kafka/connect,/debezium-plugins
    ports:
      - "8083:8083"
    volumes:
      - ./debezium/connectors:/connectors:ro
      - ./debezium/plugins:/debezium-plugins

  debezium-ui:
    image: debezium/debezium-ui:2.5
    container_name: pipeline_debezium_ui
    depends_on:
      - debezium
    environment:
      KAFKA_CONNECT_URIS: http://debezium:8083
    ports:
      - "8080:8080"

  airflow-db:
    image: postgres:16-alpine
    container_name: pipeline_airflow_db
    environment:
      POSTGRES_DB: ${AIRFLOW_DB}
      POSTGRES_USER: ${AIRFLOW_DB_USER}
      POSTGRES_PASSWORD: ${AIRFLOW_DB_PASSWORD}
    volumes:
      - airflow_db_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${AIRFLOW_DB_USER} -d ${AIRFLOW_DB}"]
      interval: 10s
      timeout: 5s
      retries: 10

  airflow-init:
    image: apache/airflow:2.10.2
    container_name: pipeline_airflow_init
    depends_on:
      airflow-db:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASSWORD}@airflow-db:5432/${AIRFLOW_DB}
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    volumes:
      - ./airflow/dags:/opt/airflow/dags
    command: bash -c "airflow db migrate && airflow users create --username ${AIRFLOW_ADMIN_USER} --password ${AIRFLOW_ADMIN_PASSWORD} --firstname Admin --lastname User --role Admin --email ${AIRFLOW_ADMIN_EMAIL} || true"

  airflow-webserver:
    image: apache/airflow:2.10.2
    container_name: pipeline_airflow_webserver
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASSWORD}@airflow-db:5432/${AIRFLOW_DB}
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    ports:
      - "8081:8080"
    volumes:
      - ./airflow/dags:/opt/airflow/dags
    command: bash -c "airflow webserver"

  airflow-scheduler:
    image: apache/airflow:2.10.2
    container_name: pipeline_airflow_scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASSWORD}@airflow-db:5432/${AIRFLOW_DB}
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    volumes:
      - ./airflow/dags:/opt/airflow/dags
    command: bash -c "airflow scheduler"

volumes:
  redpanda_data:
  airflow_db_data:
YAML
```

Проверка:

```bash
docker compose -f /opt/agentic-data-stack/pipeline/docker-compose.yml --env-file /opt/agentic-data-stack/pipeline/.env config >/dev/null && echo OK
```

### 7.5. Запустить и проверить Machine 2

```bash
docker compose -f /opt/agentic-data-stack/pipeline/docker-compose.yml --env-file /opt/agentic-data-stack/pipeline/.env up -d
```

Проверка:

```bash
docker compose -f /opt/agentic-data-stack/pipeline/docker-compose.yml --env-file /opt/agentic-data-stack/pipeline/.env ps
```

Проверки сервисов:

```bash
docker exec pipeline_redpanda rpk cluster health
curl http://pipeline-node:8083/connectors
curl http://pipeline-node:8083/connector-plugins | jq '.[].class' | grep ClickHouseSinkConnector
curl -I http://pipeline-node:8080
curl -I http://pipeline-node:8081
```

Ожидаемо: Redpanda healthy, Debezium REST отвечает, ClickHouse plugin найден, UI доступны.

---

## 8. Debezium migration scenarios

Выполнять на `pipeline-node`.

Общий source connector одинаковый для обоих вариантов:

```text
PostgreSQL -> Redpanda topic pg_flat.public.app_events
```

Sink connector отличается:

- **Вариант A** — ClickHouse пустой, connector пытается создать структуру автоматически.
- **Вариант B** — ClickHouse schema уже подготовлена, connector пишет в готовую таблицу.

### 8.1. Source connector: PostgreSQL -> Redpanda

```bash
cat > /opt/agentic-data-stack/pipeline/debezium/connectors/postgres-source.json <<'JSON'
{
  "name": "postgres-app-events-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "source-db-node",
    "database.port": "5432",
    "database.user": "app",
    "database.password": "app_password",
    "database.dbname": "app_logs",
    "topic.prefix": "pg_flat",
    "schema.include.list": "public",
    "table.include.list": "public.app_events",
    "slot.name": "app_events_slot",
    "publication.name": "app_events_publication",
    "publication.autocreate.mode": "filtered",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "false",
    "decimal.handling.mode": "string",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "true",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}
JSON
```

Проверка:

```bash
jq . /opt/agentic-data-stack/pipeline/debezium/connectors/postgres-source.json >/dev/null && echo OK
```

```bash
curl -X POST http://pipeline-node:8083/connectors -H 'Content-Type: application/json' --data @/opt/agentic-data-stack/pipeline/debezium/connectors/postgres-source.json
```

Проверка:

```bash
curl http://pipeline-node:8083/connectors/postgres-app-events-source/status | jq '.connector.state, .tasks[0].state'
docker exec pipeline_redpanda rpk topic list
```

Ожидаемо: connector `RUNNING`, topic `pg_flat.public.app_events` существует.

### 8.2. Вариант A: ClickHouse пустой, auto-create структуры

Выбирайте этот вариант, если хотите, чтобы ClickHouse sink connector сам создал таблицу.

Ограничение: параметры auto-create зависят от версии ClickHouse Kafka Connect Sink. Если connector не поддерживает `auto.create`/`auto.evolve`, используйте вариант B.

```bash
cat > /opt/agentic-data-stack/pipeline/debezium/connectors/clickhouse-sink-autocreate.json <<'JSON'
{
  "name": "clickhouse-app-events-sink-autocreate",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "tasks.max": "1",
    "topics": "pg_flat.public.app_events",
    "hostname": "ai-data-node",
    "port": "8123",
    "database": "analytics",
    "username": "analytics",
    "password": "analytics_password",
    "ssl": "false",
    "schemas.enable": "true",
    "auto.create": "true",
    "auto.evolve": "true",
    "exactlyOnce": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true"
  }
}
JSON
```

Проверка:

```bash
jq . /opt/agentic-data-stack/pipeline/debezium/connectors/clickhouse-sink-autocreate.json >/dev/null && echo OK
```

```bash
curl -X POST http://pipeline-node:8083/connectors -H 'Content-Type: application/json' --data @/opt/agentic-data-stack/pipeline/debezium/connectors/clickhouse-sink-autocreate.json
```

Проверка:

```bash
curl http://pipeline-node:8083/connectors/clickhouse-app-events-sink-autocreate/status | jq '.connector.state, .tasks[0].state'
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'SHOW TABLES FROM analytics'
```

Ожидаемо: sink `RUNNING`, в ClickHouse появилась таблица.

Проверка данных:

```bash
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'SELECT count() FROM analytics.app_events'
```

Если таблица создана под другим именем, сначала выполните `SHOW TABLES FROM analytics` и замените имя таблицы.

### 8.3. Вариант B: миграция в заранее подготовленную ClickHouse schema

Это рекомендуемый вариант для предсказуемой production-схемы.

Проверка таблицы:

```bash
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'DESCRIBE TABLE analytics.app_events_raw'
```

Ожидаемо: ClickHouse возвращает список колонок.

```bash
cat > /opt/agentic-data-stack/pipeline/debezium/connectors/clickhouse-sink-prepared.json <<'JSON'
{
  "name": "clickhouse-app-events-sink",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "tasks.max": "1",
    "topics": "pg_flat.public.app_events",
    "hostname": "ai-data-node",
    "port": "8123",
    "database": "analytics",
    "username": "analytics",
    "password": "analytics_password",
    "ssl": "false",
    "topic2TableMap": "pg_flat.public.app_events=app_events_raw",
    "dateTimeFormats": "event_time=yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'",
    "schemas.enable": "false",
    "exactlyOnce": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false"
  }
}
JSON
```

Проверка:

```bash
jq . /opt/agentic-data-stack/pipeline/debezium/connectors/clickhouse-sink-prepared.json >/dev/null && echo OK
```

```bash
curl -X POST http://pipeline-node:8083/connectors -H 'Content-Type: application/json' --data @/opt/agentic-data-stack/pipeline/debezium/connectors/clickhouse-sink-prepared.json
```

Проверка:

```bash
curl http://pipeline-node:8083/connectors/clickhouse-app-events-sink/status | jq '.connector.state, .tasks[0].state'
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'SELECT count() FROM analytics.app_events_raw'
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'SELECT * FROM analytics.v_event_summary LIMIT 10'
```

Ожидаемо: sink `RUNNING`, count приближается к PostgreSQL count, view возвращает агрегаты.

---

## 9. Adminer и UI-доступы

### 9.1. PostgreSQL через Adminer

URL:

```text
http://source-db-node:8082
```

Параметры:

```text
System: PostgreSQL
Server: postgres
Username: app
Password: app_password
Database: app_logs
```

Проверка:

```bash
curl -I http://source-db-node:8082
```

### 9.2. ClickHouse через Adminer

URL:

```text
http://ai-data-node:8082
```

Параметры:

```text
System: ClickHouse
Server: clickhouse
Username: analytics
Password: analytics_password
Database: analytics
```

Проверка:

```bash
curl -I http://ai-data-node:8082
```

Если Adminer image не показывает ClickHouse driver, используйте ClickHouse built-in UI:

```text
http://ai-data-node:8123/play
```

Проверка:

```bash
curl -I http://ai-data-node:8123/play
```

### 9.3. Остальные UI

| UI | URL | Credentials |
|---|---|---|
| LibreChat | `http://ai-data-node:3080` | создать пользователя в UI или через admin script |
| LangFuse | `http://ai-data-node:3000` | создать пользователя/проект в UI |
| Debezium UI | `http://pipeline-node:8080` | без логина по умолчанию |
| Airflow | `http://pipeline-node:8081` | `admin / admin` |

---

## 10. End-to-end smoke check

### 10.1. Сравнить counts

PostgreSQL:

```bash
psql "postgresql://app:app_password@source-db-node:5432/app_logs" -c "SELECT count(*) FROM public.app_events;"
```

ClickHouse для варианта B:

```bash
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'SELECT count() FROM analytics.app_events_raw'
```

Ожидаемо: counts совпадают после завершения snapshot.

### 10.2. Проверить realtime CDC

```bash
psql "postgresql://app:app_password@source-db-node:5432/app_logs" -c "INSERT INTO public.app_events (user_id, session_id, event_type, route, status_code, latency_ms, metadata) VALUES ('manual_user', 'manual_session', 'manual_test', '/manual', 200, 123, '{\"source\":\"manual\"}'::jsonb);"
```

Проверка PostgreSQL:

```bash
psql "postgresql://app:app_password@source-db-node:5432/app_logs" -c "SELECT count(*) FROM public.app_events WHERE event_type = 'manual_test';"
```

Проверка ClickHouse для варианта B:

```bash
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary "SELECT count() FROM analytics.app_events_raw WHERE event_type = 'manual_test'"
```

Ожидаемо: ClickHouse возвращает `1` после небольшой задержки.

### 10.3. Проверить connectors

Source:

```bash
curl http://pipeline-node:8083/connectors/postgres-app-events-source/status | jq '.connector.state, .tasks[0].state'
```

Sink вариант A:

```bash
curl http://pipeline-node:8083/connectors/clickhouse-app-events-sink-autocreate/status | jq '.connector.state, .tasks[0].state'
```

Sink вариант B:

```bash
curl http://pipeline-node:8083/connectors/clickhouse-app-events-sink/status | jq '.connector.state, .tasks[0].state'
```

Ожидаемо: `RUNNING` и `RUNNING`.

### 10.4. Проверить AI/MCP layer

```bash
curl http://ai-data-node:3333/health
curl http://ai-data-node:3344/health
curl http://ai-data-node:3344/v1/models
curl -I http://ai-data-node:3080
curl -I http://ai-data-node:3000
```

Ожидаемо: health endpoints возвращают `ok:true`, UI отвечают без network error.

---

## 11. Troubleshooting

### 11.1. Debezium не видит PostgreSQL

Проверка с `pipeline-node`:

```bash
psql "postgresql://app:app_password@source-db-node:5432/app_logs" -c "SELECT 1;"
```

Если не работает:

- проверьте Tailscale DNS/IP;
- проверьте firewall `tailscale0`;
- проверьте `wal_level=logical`;
- проверьте credentials.

### 11.2. ClickHouse Sink plugin не найден

Проверка:

```bash
curl http://pipeline-node:8083/connector-plugins | jq '.[].class' | grep ClickHouseSinkConnector
```

Если пусто:

```bash
find /opt/agentic-data-stack/pipeline/debezium/plugins -type f -name '*.jar'
```

После добавления plugin перезапустите Debezium:

```bash
docker compose -f /opt/agentic-data-stack/pipeline/docker-compose.yml --env-file /opt/agentic-data-stack/pipeline/.env up -d --force-recreate debezium
```

### 11.3. Sink connector упал

Статус:

```bash
curl http://pipeline-node:8083/connectors/clickhouse-app-events-sink/status | jq .
```

Логи:

```bash
docker logs pipeline_debezium --tail=200
```

Частые причины:

- ClickHouse table отсутствует в варианте B;
- `topic2TableMap` не совпадает с topic;
- типы колонок ClickHouse не совпадают с Debezium payload;
- ClickHouse недоступен по `ai-data-node:8123`.

### 11.4. Adminer не показывает ClickHouse

Некоторые Adminer images не содержат ClickHouse driver. Используйте:

```text
http://ai-data-node:8123/play
```

Проверка:

```bash
curl -I http://ai-data-node:8123/play
```

### 11.5. agent-proxy не видит LLM runtime

Проверка Ollama:

```bash
curl http://ai-data-node:11434/api/tags
```

Проверка proxy:

```bash
curl http://ai-data-node:3344/health
```

Проверьте `.env` на `ai-data-node`:

```text
UPSTREAM_OPENAI_BASE_URL=http://ollama:11434/v1
```

---

## 12. Production notes

- Замените все demo passwords и secrets.
- Не открывайте PostgreSQL, ClickHouse, Debezium Connect и Adminer в публичный интернет.
- Используйте Tailscale ACL, чтобы ограничить доступ между машинами.
- Для варианта A проверьте поддержку `auto.create`/`auto.evolve` в конкретной версии ClickHouse Kafka Connect Sink.
- Для production обычно предпочтительнее вариант B: schema ClickHouse создаётся и версионируется вручную.
- Для source DB кроме PostgreSQL нужен соответствующий Debezium source connector.
- Для MySQL/MariaDB включите binlog и используйте `io.debezium.connector.mysql.MySqlConnector`.
- Для Oracle/SQL Server нужны отдельные Debezium connectors и права на CDC.
- `mcp-server` и `agent-proxy` должны быть заранее собраны в container images и доступны с серверов.
