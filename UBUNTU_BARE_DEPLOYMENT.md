# Развертывание Agentic Data Stack на голых Ubuntu servers

Документ описывает минимальное production-like развертывание на трёх Ubuntu Server через приватную VPN-сеть Tailscale.

Цель: поднять только обязательные элементы, без которых стек не работает:

- PostgreSQL или другая source database.
- Debezium Connect.
- Kafka-compatible broker для Debezium, в примерах используется Redpanda.
- ClickHouse.
- MCP server.
- LibreChat.
- LangFuse.
- LLM runtime.
- Adminer для просмотра PostgreSQL и ClickHouse.
- Airflow для orchestration Debezium jobs.

В документе нет JS-кода приложения. MCP server и LLM proxy предполагаются как готовые контейнеры/образы или уже собранные сервисы. Если в вашей сборке MCP/agent proxy реализованы на Node.js, на сервере всё равно запускаются только контейнеры, без ручного написания JS-кода.

## 0. Схема машин

```text
Machine 1: ai-data-node
  Tailscale name/IP: ai-data-node / 100.x.x.1
  Services:
    - ClickHouse
    - MCP server
    - LibreChat
    - LibreChat MongoDB
    - LangFuse
    - LangFuse PostgreSQL
    - LLM runtime, например Ollama/vLLM/LM Studio-compatible endpoint
    - Adminer

Machine 2: pipeline-node
  Tailscale name/IP: pipeline-node / 100.x.x.2
  Services:
    - Redpanda
    - Debezium Connect
    - Debezium UI
    - Airflow webserver
    - Airflow scheduler
    - Airflow metadata PostgreSQL

Machine 3: source-db-node
  Tailscale name/IP: source-db-node / 100.x.x.3
  Services:
    - PostgreSQL source database
    - optional Adminer, если нужен локальный доступ к source DB
```

Дальше в командах используются DNS-имена Tailscale MagicDNS:

```text
ai-data-node
pipeline-node
source-db-node
```

Если MagicDNS выключен, используйте Tailscale IP вида `100.x.x.x`.

## 1. Подготовка всех трёх Ubuntu servers

Выполнить на каждой машине.

### 1.1. Обновить ОС

```bash
sudo apt update && sudo apt upgrade -y
```

Проверка:

```bash
lsb_release -a && uname -a
```

Ожидаемо: команда показывает Ubuntu Server и актуальное ядро.

### 1.2. Установить базовые утилиты

```bash
sudo apt install -y ca-certificates curl gnupg lsb-release jq net-tools htop unzip
```

Проверка:

```bash
curl --version && jq --version
```

Ожидаемо: обе команды выводят версии.

### 1.3. Установить Docker Engine и Docker Compose plugin

```bash
sudo install -m 0755 -d /etc/apt/keyrings
```

Проверка:

```bash
ls -ld /etc/apt/keyrings
```

```bash
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
```

Проверка:

```bash
ls -l /etc/apt/keyrings/docker.gpg
```

```bash
sudo chmod a+r /etc/apt/keyrings/docker.gpg
```

Проверка:

```bash
stat -c '%a %n' /etc/apt/keyrings/docker.gpg
```

```bash
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
```

Проверка:

```bash
cat /etc/apt/sources.list.d/docker.list
```

```bash
sudo apt update
```

Проверка:

```bash
apt-cache policy docker-ce | sed -n '1,20p'
```

```bash
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

Проверка:

```bash
docker --version && docker compose version
```

```bash
sudo usermod -aG docker $USER
```

Проверка:

```bash
id $USER
```

Важно: после добавления пользователя в группу Docker перелогиньтесь по SSH.

После перелогина проверить:

```bash
docker run --rm hello-world
```

Ожидаемо: Docker выводит `Hello from Docker!`.

### 1.4. Создать рабочую директорию

```bash
sudo mkdir -p /opt/agentic-data-stack
```

Проверка:

```bash
ls -ld /opt/agentic-data-stack
```

```bash
sudo chown -R $USER:$USER /opt/agentic-data-stack
```

Проверка:

```bash
stat -c '%U:%G %n' /opt/agentic-data-stack
```

## 2. Tailscale VPN для трёх машин

Выполнить на каждой машине.

### 2.1. Установить Tailscale

```bash
curl -fsSL https://tailscale.com/install.sh | sh
```

Проверка:

```bash
tailscale version
```

### 2.2. Подключить машину к tailnet

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

### 2.3. Проверить связность между машинами

С `pipeline-node` проверить source database node:

```bash
ping -c 3 source-db-node
```

Проверка успешности: есть ответы `3 received`.

С `pipeline-node` проверить ClickHouse node:

```bash
ping -c 3 ai-data-node
```

Проверка успешности: есть ответы `3 received`.

С `ai-data-node` проверить pipeline node:

```bash
ping -c 3 pipeline-node
```

Проверка успешности: есть ответы `3 received`.

### 2.4. Минимальные firewall правила

На `source-db-node` разрешить PostgreSQL только из Tailscale:

```bash
sudo ufw allow in on tailscale0 to any port 5432 proto tcp
```

Проверка:

```bash
sudo ufw status verbose
```

На `ai-data-node` разрешить ClickHouse, LibreChat, LangFuse, MCP, Adminer и LLM endpoint через Tailscale:

```bash
sudo ufw allow in on tailscale0 to any port 8123 proto tcp
```

Проверка:

```bash
sudo ufw status verbose | grep 8123
```

```bash
sudo ufw allow in on tailscale0 to any port 9000 proto tcp
```

Проверка:

```bash
sudo ufw status verbose | grep 9000
```

```bash
sudo ufw allow in on tailscale0 to any port 3333 proto tcp
```

Проверка:

```bash
sudo ufw status verbose | grep 3333
```

```bash
sudo ufw allow in on tailscale0 to any port 3000 proto tcp
```

Проверка:

```bash
sudo ufw status verbose | grep 3000
```

```bash
sudo ufw allow in on tailscale0 to any port 3080 proto tcp
```

Проверка:

```bash
sudo ufw status verbose | grep 3080
```

```bash
sudo ufw allow in on tailscale0 to any port 8082 proto tcp
```

Проверка:

```bash
sudo ufw status verbose | grep 8082
```

```bash
sudo ufw allow in on tailscale0 to any port 11434 proto tcp
```

Проверка:

```bash
sudo ufw status verbose | grep 11434
```

На `pipeline-node` разрешить Debezium, Debezium UI, Redpanda и Airflow через Tailscale:

```bash
sudo ufw allow in on tailscale0 to any port 8083 proto tcp
```

Проверка:

```bash
sudo ufw status verbose | grep 8083
```

```bash
sudo ufw allow in on tailscale0 to any port 8080 proto tcp
```

Проверка:

```bash
sudo ufw status verbose | grep 8080
```

```bash
sudo ufw allow in on tailscale0 to any port 8081 proto tcp
```

Проверка:

```bash
sudo ufw status verbose | grep 8081
```

```bash
sudo ufw allow in on tailscale0 to any port 9092 proto tcp
```

Проверка:

```bash
sudo ufw status verbose | grep 9092
```

## 3. Machine 3: PostgreSQL source database

Раздел выполняется на `source-db-node`.

### 3.1. Создать docker-compose для PostgreSQL и Adminer

```bash
mkdir -p /opt/agentic-data-stack/source-db/postgres-init
```

Проверка:

```bash
ls -ld /opt/agentic-data-stack/source-db/postgres-init
```

```bash
cat > /opt/agentic-data-stack/source-db/.env <<'EOF'
POSTGRES_DB=app_logs
POSTGRES_USER=app
POSTGRES_PASSWORD=app_password
EOF
```

Проверка:

```bash
grep -E 'POSTGRES_DB|POSTGRES_USER' /opt/agentic-data-stack/source-db/.env
```

```bash
cat > /opt/agentic-data-stack/source-db/postgres-init/001_schema.sql <<'EOF'
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
FROM generate_series(1, 1000) AS g
ON CONFLICT DO NOTHING;
EOF
```

Проверка:

```bash
sed -n '1,20p' /opt/agentic-data-stack/source-db/postgres-init/001_schema.sql
```

```bash
cat > /opt/agentic-data-stack/source-db/docker-compose.yml <<'EOF'
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
EOF
```

Проверка:

```bash
docker compose -f /opt/agentic-data-stack/source-db/docker-compose.yml config >/dev/null && echo OK
```

### 3.2. Запустить PostgreSQL source database

```bash
docker compose -f /opt/agentic-data-stack/source-db/docker-compose.yml up -d
```

Проверка:

```bash
docker compose -f /opt/agentic-data-stack/source-db/docker-compose.yml ps
```

Проверить readiness:

```bash
docker exec source_postgres pg_isready -U app -d app_logs
```

Ожидаемо: `accepting connections`.

Проверить logical replication:

```bash
docker exec source_postgres psql -U app -d app_logs -c "SHOW wal_level;"
```

Ожидаемо: `logical`.

Проверить данные:

```bash
docker exec source_postgres psql -U app -d app_logs -c "SELECT count(*) FROM public.app_events;"
```

Ожидаемо: `1000`.

Проверить доступ с `pipeline-node`:

```bash
psql "postgresql://app:app_password@source-db-node:5432/app_logs" -c "SELECT count(*) FROM public.app_events;"
```

Если `psql` не установлен на `pipeline-node`, установить:

```bash
sudo apt install -y postgresql-client
```

Проверка установки:

```bash
psql --version
```

Adminer source database:

```text
http://source-db-node:8082
```

Параметры входа:

```text
System: PostgreSQL
Server: postgres
Username: app
Password: app_password
Database: app_logs
```

Проверка Adminer:

```bash
curl -I http://source-db-node:8082
```

Ожидаемо: HTTP status `200` или `302`.

## 4. Machine 1: ClickHouse, Adminer, LangFuse, LibreChat, MCP, LLM

Раздел выполняется на `ai-data-node`.

### 4.1. Создать директории

```bash
mkdir -p /opt/agentic-data-stack/ai-data/clickhouse-init
```

Проверка:

```bash
ls -ld /opt/agentic-data-stack/ai-data/clickhouse-init
```

```bash
mkdir -p /opt/agentic-data-stack/ai-data/librechat
```

Проверка:

```bash
ls -ld /opt/agentic-data-stack/ai-data/librechat
```

### 4.2. Создать `.env`

```bash
cat > /opt/agentic-data-stack/ai-data/.env <<'EOF'
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
LIBRECHAT_MODELS=qwen2.5:7b
LIBRECHAT_TITLE_MODEL=qwen2.5:7b
LIBRECHAT_SUMMARY_MODEL=qwen2.5:7b
AGENT_PROXY_API_KEY=local-dev-key
AGENT_PROXY_BASE_URL=http://agent-proxy:3344/v1

UPSTREAM_OPENAI_API_KEY=local-dev-key
UPSTREAM_OPENAI_BASE_URL=http://ollama:11434/v1
EOF
```

Проверка:

```bash
grep -E 'CLICKHOUSE_DB|LANGFUSE_HOST|LIBRECHAT_MODELS|UPSTREAM_OPENAI_BASE_URL' /opt/agentic-data-stack/ai-data/.env
```

### 4.3. Вариант B: заранее подготовленная структура ClickHouse

Этот вариант нужен, если ClickHouse schema уже известна и должна контролироваться вручную.

```bash
cat > /opt/agentic-data-stack/ai-data/clickhouse-init/001_schema.sql <<'EOF'
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
EOF
```

Проверка:

```bash
sed -n '1,40p' /opt/agentic-data-stack/ai-data/clickhouse-init/001_schema.sql
```

Для варианта A, когда структура создаётся автоматически ClickHouse sink connector, этот файл можно не создавать или оставить только `CREATE DATABASE IF NOT EXISTS analytics;`. Подробности в разделе 6.

### 4.4. LibreChat config без ручного JS-кода

```bash
cat > /opt/agentic-data-stack/ai-data/librechat/librechat.yaml <<'EOF'
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
EOF
```

Проверка:

```bash
sed -n '1,80p' /opt/agentic-data-stack/ai-data/librechat/librechat.yaml
```

### 4.5. Создать docker-compose для Machine 1

Важно: `mcp-server` и `agent-proxy` ниже указаны как готовые container images. Замените `your-registry/...` на ваши реальные образы. Если вы не используете отдельный `agent-proxy`, LibreChat можно подключить напрямую к LLM OpenAI-compatible endpoint, но тогда LangFuse tracing нужно обеспечить другим способом.

```bash
cat > /opt/agentic-data-stack/ai-data/docker-compose.yml <<'EOF'
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
    depends_on:
      clickhouse:
        condition: service_healthy
      langfuse-db:
        condition: service_healthy
    ports:
      - "8082:8080"

volumes:
  clickhouse_data:
  langfuse_db_data:
  librechat_mongo_data:
  ollama_data:
EOF
```

Проверка:

```bash
docker compose -f /opt/agentic-data-stack/ai-data/docker-compose.yml config >/dev/null && echo OK
```

### 4.6. Запустить Machine 1 services

```bash
docker compose -f /opt/agentic-data-stack/ai-data/docker-compose.yml up -d
```

Проверка:

```bash
docker compose -f /opt/agentic-data-stack/ai-data/docker-compose.yml ps
```

Проверить ClickHouse:

```bash
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'SELECT 1'
```

Ожидаемо: `1`.

Проверить подготовленную таблицу, если используется вариант B:

```bash
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'EXISTS TABLE analytics.app_events_raw'
```

Ожидаемо: `1`.

Проверить LangFuse:

```bash
curl -I http://ai-data-node:3000
```

Ожидаемо: HTTP status `200`, `302` или другой web response без network error.

Проверить Ollama:

```bash
curl http://ai-data-node:11434/api/tags
```

Ожидаемо: JSON со списком моделей, возможно пустым.

Загрузить LLM модель, если используется Ollama:

```bash
docker exec ai_ollama ollama pull qwen2.5:7b
```

Проверка:

```bash
docker exec ai_ollama ollama list
```

Проверить agent-proxy:

```bash
curl http://ai-data-node:3344/health
```

Ожидаемо: JSON с `ok:true`.

Проверить MCP server:

```bash
curl http://ai-data-node:3333/health
```

Ожидаемо: JSON с `ok:true`.

Проверить LibreChat:

```bash
curl -I http://ai-data-node:3080
```

Ожидаемо: HTTP response от web UI.

Проверить Adminer:

```bash
curl -I http://ai-data-node:8082
```

Ожидаемо: HTTP status `200` или `302`.

Adminer для ClickHouse:

```text
http://ai-data-node:8082
```

Параметры входа:

```text
System: ClickHouse
Server: clickhouse
Username: analytics
Password: analytics_password
Database: analytics
```

Если текущий образ Adminer не содержит ClickHouse driver, используйте ClickHouse built-in UI:

```text
http://ai-data-node:8123/play
```

Проверка ClickHouse UI:

```bash
curl -I http://ai-data-node:8123/play
```

## 5. Machine 2: Redpanda, Debezium, Airflow

Раздел выполняется на `pipeline-node`.

### 5.1. Создать директории

```bash
mkdir -p /opt/agentic-data-stack/pipeline/debezium/connectors
```

Проверка:

```bash
ls -ld /opt/agentic-data-stack/pipeline/debezium/connectors
```

```bash
mkdir -p /opt/agentic-data-stack/pipeline/debezium/plugins
```

Проверка:

```bash
ls -ld /opt/agentic-data-stack/pipeline/debezium/plugins
```

```bash
mkdir -p /opt/agentic-data-stack/pipeline/airflow/dags
```

Проверка:

```bash
ls -ld /opt/agentic-data-stack/pipeline/airflow/dags
```

### 5.2. Установить ClickHouse Kafka Connect plugin

Скачайте ClickHouse Kafka Connect plugin и положите его в:

```text
/opt/agentic-data-stack/pipeline/debezium/plugins
```

Пример проверки после копирования:

```bash
find /opt/agentic-data-stack/pipeline/debezium/plugins -type f -name '*.jar'
```

Ожидаемо: виден JAR `clickhouse-kafka-connect`.

### 5.3. Создать `.env` для pipeline node

```bash
cat > /opt/agentic-data-stack/pipeline/.env <<'EOF'
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.local
AIRFLOW_DB=airflow
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow_password
EOF
```

Проверка:

```bash
grep -E 'AIRFLOW_ADMIN_USER|AIRFLOW_DB' /opt/agentic-data-stack/pipeline/.env
```

### 5.4. Создать docker-compose для pipeline node

```bash
cat > /opt/agentic-data-stack/pipeline/docker-compose.yml <<'EOF'
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
    env_file: .env
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
    env_file: .env
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
    env_file: .env
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
    env_file: .env
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
EOF
```

Проверка:

```bash
docker compose -f /opt/agentic-data-stack/pipeline/docker-compose.yml config >/dev/null && echo OK
```

### 5.5. Запустить pipeline services

```bash
docker compose -f /opt/agentic-data-stack/pipeline/docker-compose.yml up -d
```

Проверка:

```bash
docker compose -f /opt/agentic-data-stack/pipeline/docker-compose.yml ps
```

Проверить Redpanda:

```bash
docker exec pipeline_redpanda rpk cluster health
```

Ожидаемо: `Healthy: true`.

Проверить Debezium Connect:

```bash
curl http://pipeline-node:8083/connectors
```

Ожидаемо: пустой JSON-массив `[]`, если connectors ещё не зарегистрированы.

Проверить plugin ClickHouse Sink:

```bash
curl http://pipeline-node:8083/connector-plugins | jq '.[].class' | grep ClickHouseSinkConnector
```

Ожидаемо: вывод содержит `com.clickhouse.kafka.connect.ClickHouseSinkConnector`.

Проверить Debezium UI:

```bash
curl -I http://pipeline-node:8080
```

Ожидаемо: HTTP response от UI.

Проверить Airflow:

```bash
curl -I http://pipeline-node:8081
```

Ожидаемо: HTTP response от UI.

Airflow UI:

```text
http://pipeline-node:8081
```

Credentials:

```text
admin / admin
```

## 6. Debezium миграция: два варианта

В обоих вариантах source connector читает PostgreSQL changes и пишет их в Kafka topic `pg_flat.public.app_events`.

Разница находится в ClickHouse sink connector.

### 6.1. Source connector PostgreSQL -> Redpanda

Создать source connector config на `pipeline-node`:

```bash
cat > /opt/agentic-data-stack/pipeline/debezium/connectors/postgres-source.json <<'EOF'
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
EOF
```

Проверка:

```bash
jq . /opt/agentic-data-stack/pipeline/debezium/connectors/postgres-source.json >/dev/null && echo OK
```

Зарегистрировать source connector:

```bash
curl -X POST http://pipeline-node:8083/connectors -H 'Content-Type: application/json' --data @/opt/agentic-data-stack/pipeline/debezium/connectors/postgres-source.json
```

Проверка:

```bash
curl http://pipeline-node:8083/connectors/postgres-app-events-source/status | jq .
```

Ожидаемо: `connector.state` и `tasks[0].state` равны `RUNNING`.

Проверить Kafka topic:

```bash
docker exec pipeline_redpanda rpk topic list
```

Ожидаемо: есть topic `pg_flat.public.app_events`.

### 6.2. Вариант A: миграция в пустой ClickHouse с автоматическим созданием структуры

Используйте этот вариант, если ClickHouse должен получить таблицу автоматически на основании событий/схемы.

Требования:

- ClickHouse database `analytics` существует.
- Таблица `analytics.app_events` или `analytics.app_events_raw` заранее не создаётся вручную.
- В ClickHouse sink connector включается auto-create/evolution, если ваша версия ClickHouse Kafka Connect Sink поддерживает эти параметры.
- Debezium value converter должен передавать schemas, иначе sink connector может не знать типы колонок.

Создать sink config:

```bash
cat > /opt/agentic-data-stack/pipeline/debezium/connectors/clickhouse-sink-autocreate.json <<'EOF'
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
EOF
```

Проверка JSON:

```bash
jq . /opt/agentic-data-stack/pipeline/debezium/connectors/clickhouse-sink-autocreate.json >/dev/null && echo OK
```

Зарегистрировать sink connector:

```bash
curl -X POST http://pipeline-node:8083/connectors -H 'Content-Type: application/json' --data @/opt/agentic-data-stack/pipeline/debezium/connectors/clickhouse-sink-autocreate.json
```

Проверка статуса:

```bash
curl http://pipeline-node:8083/connectors/clickhouse-app-events-sink-autocreate/status | jq .
```

Ожидаемо: `connector.state` и `tasks[0].state` равны `RUNNING`.

Проверить, что таблица появилась в ClickHouse:

```bash
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'SHOW TABLES FROM analytics'
```

Ожидаемо: появилась таблица, соответствующая topic или настройкам connector.

Проверить количество строк:

```bash
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'SELECT count() FROM analytics.app_events'
```

Если connector создал таблицу с другим именем, сначала посмотрите `SHOW TABLES FROM analytics` и замените имя таблицы в запросе.

Проверка ошибок:

```bash
docker logs pipeline_debezium --tail=200
```

Если connector не поддерживает `auto.create`/`auto.evolve`, используйте вариант B.

### 6.3. Вариант B: миграция в заранее подготовленную структуру ClickHouse

Используйте этот вариант, если schema контролируется вручную, например таблица `analytics.app_events_raw` уже создана на `ai-data-node`.

Проверить таблицу:

```bash
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'DESCRIBE TABLE analytics.app_events_raw'
```

Ожидаемо: ClickHouse возвращает список колонок.

Создать sink config:

```bash
cat > /opt/agentic-data-stack/pipeline/debezium/connectors/clickhouse-sink-prepared.json <<'EOF'
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
EOF
```

Проверка JSON:

```bash
jq . /opt/agentic-data-stack/pipeline/debezium/connectors/clickhouse-sink-prepared.json >/dev/null && echo OK
```

Зарегистрировать sink connector:

```bash
curl -X POST http://pipeline-node:8083/connectors -H 'Content-Type: application/json' --data @/opt/agentic-data-stack/pipeline/debezium/connectors/clickhouse-sink-prepared.json
```

Проверка статуса:

```bash
curl http://pipeline-node:8083/connectors/clickhouse-app-events-sink/status | jq .
```

Ожидаемо: `connector.state` и `tasks[0].state` равны `RUNNING`.

Проверить данные в ClickHouse:

```bash
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'SELECT count() FROM analytics.app_events_raw'
```

Ожидаемо: после snapshot значение приближается к count в PostgreSQL, например `1000`.

Проверить summary view:

```bash
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'SELECT * FROM analytics.v_event_summary LIMIT 10'
```

Ожидаемо: ClickHouse возвращает агрегированные строки.

## 7. Проверка end-to-end потока

Сравнить PostgreSQL count:

```bash
psql "postgresql://app:app_password@source-db-node:5432/app_logs" -c "SELECT count(*) FROM public.app_events;"
```

Проверка: PostgreSQL возвращает count, например `1000`.

Сравнить ClickHouse count:

```bash
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'SELECT count() FROM analytics.app_events_raw'
```

Проверка: ClickHouse возвращает такой же count для варианта B.

Добавить новую строку в PostgreSQL:

```bash
psql "postgresql://app:app_password@source-db-node:5432/app_logs" -c "INSERT INTO public.app_events (user_id, session_id, event_type, route, status_code, latency_ms, metadata) VALUES ('manual_user', 'manual_session', 'manual_test', '/manual', 200, 123, '{\"source\":\"manual\"}'::jsonb);"
```

Проверка PostgreSQL:

```bash
psql "postgresql://app:app_password@source-db-node:5432/app_logs" -c "SELECT count(*) FROM public.app_events WHERE event_type = 'manual_test';"
```

Проверка ClickHouse:

```bash
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary "SELECT count() FROM analytics.app_events_raw WHERE event_type = 'manual_test'"
```

Ожидаемо: ClickHouse возвращает `1` после небольшой задержки.

Проверить source connector:

```bash
curl http://pipeline-node:8083/connectors/postgres-app-events-source/status | jq '.connector.state, .tasks[0].state'
```

Ожидаемо:

```text
"RUNNING"
"RUNNING"
```

Проверить sink connector для варианта B:

```bash
curl http://pipeline-node:8083/connectors/clickhouse-app-events-sink/status | jq '.connector.state, .tasks[0].state'
```

Ожидаемо:

```text
"RUNNING"
"RUNNING"
```

Проверить Redpanda topics:

```bash
docker exec pipeline_redpanda rpk topic list
```

Ожидаемо: есть `pg_flat.public.app_events`.

## 8. Adminer: просмотр PostgreSQL и ClickHouse

### 8.1. PostgreSQL через Adminer на source-db-node

Открыть:

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

Проверка с CLI:

```bash
curl -I http://source-db-node:8082
```

### 8.2. ClickHouse через Adminer на ai-data-node

Открыть:

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

Проверка с CLI:

```bash
curl -I http://ai-data-node:8082
```

Если Adminer image не показывает ClickHouse driver, используйте built-in ClickHouse UI:

```text
http://ai-data-node:8123/play
```

Проверка:

```bash
curl -I http://ai-data-node:8123/play
```

## 9. LibreChat, MCP и LangFuse smoke checks

Проверить LibreChat UI:

```bash
curl -I http://ai-data-node:3080
```

Проверка: HTTP response без network error.

Проверить MCP server:

```bash
curl http://ai-data-node:3333/health
```

Проверка: `ok:true`.

Проверить agent-proxy:

```bash
curl http://ai-data-node:3344/health
```

Проверка: `ok:true`.

Проверить список LLM моделей:

```bash
curl http://ai-data-node:3344/v1/models
```

Проверка: JSON со списком моделей.

Проверить LangFuse UI:

```bash
curl -I http://ai-data-node:3000
```

Проверка: HTTP response без network error.

После создания проекта в LangFuse обновите на `ai-data-node`:

```bash
nano /opt/agentic-data-stack/ai-data/.env
```

Проверка:

```bash
grep -E 'LANGFUSE_PUBLIC_KEY|LANGFUSE_SECRET_KEY' /opt/agentic-data-stack/ai-data/.env
```

Перезапустить сервисы, которым нужны ключи:

```bash
docker compose -f /opt/agentic-data-stack/ai-data/docker-compose.yml up -d --force-recreate agent-proxy librechat mcp-server
```

Проверка:

```bash
docker compose -f /opt/agentic-data-stack/ai-data/docker-compose.yml ps agent-proxy librechat mcp-server
```

## 10. Airflow smoke checks

Открыть:

```text
http://pipeline-node:8081
```

Проверка CLI:

```bash
curl -I http://pipeline-node:8081
```

Проверить scheduler logs:

```bash
docker logs pipeline_airflow_scheduler --tail=100
```

Проверка: нет циклических ошибок подключения к metadata DB.

Проверить webserver logs:

```bash
docker logs pipeline_airflow_webserver --tail=100
```

Проверка: webserver слушает порт `8080` внутри контейнера.

## 11. Полный checklist успешного развертывания

На `source-db-node`:

```bash
docker exec source_postgres psql -U app -d app_logs -c "SELECT count(*) FROM public.app_events;"
```

Ожидаемо: count больше `0`.

На `pipeline-node`:

```bash
curl http://pipeline-node:8083/connectors
```

Ожидаемо: source и sink connectors в списке.

На `pipeline-node`:

```bash
curl http://pipeline-node:8083/connectors/postgres-app-events-source/status | jq '.connector.state, .tasks[0].state'
```

Ожидаемо: `RUNNING` и `RUNNING`.

На `pipeline-node` для варианта B:

```bash
curl http://pipeline-node:8083/connectors/clickhouse-app-events-sink/status | jq '.connector.state, .tasks[0].state'
```

Ожидаемо: `RUNNING` и `RUNNING`.

На `ai-data-node`:

```bash
curl 'http://ai-data-node:8123/?user=analytics&password=analytics_password' --data-binary 'SELECT count() FROM analytics.app_events_raw'
```

Ожидаемо: count соответствует PostgreSQL.

На `ai-data-node`:

```bash
curl http://ai-data-node:3333/health
```

Ожидаемо: `ok:true`.

На `ai-data-node`:

```bash
curl http://ai-data-node:3344/health
```

Ожидаемо: `ok:true`.

На `ai-data-node`:

```bash
curl -I http://ai-data-node:3080
```

Ожидаемо: LibreChat UI отвечает.

На `ai-data-node`:

```bash
curl -I http://ai-data-node:3000
```

Ожидаемо: LangFuse UI отвечает.

На `pipeline-node`:

```bash
curl -I http://pipeline-node:8081
```

Ожидаемо: Airflow UI отвечает.

## 12. Важные замечания

- Для production замените все пароли и секреты в `.env`.
- Не открывайте PostgreSQL, ClickHouse, Debezium Connect и Adminer в публичный интернет.
- Используйте Tailscale ACL, чтобы ограничить доступ между машинами.
- Для варианта A auto-create параметры зависят от версии ClickHouse Kafka Connect Sink. Если auto-create не поддерживается, используйте вариант B.
- Для любой source DB кроме PostgreSQL нужен соответствующий Debezium source connector и отдельная настройка CDC/binlog/redo logs.
- Для MySQL/MariaDB нужно включить binlog и использовать `io.debezium.connector.mysql.MySqlConnector`.
- Для Oracle/SQL Server нужны отдельные Debezium connector images/plugins и права на CDC.
