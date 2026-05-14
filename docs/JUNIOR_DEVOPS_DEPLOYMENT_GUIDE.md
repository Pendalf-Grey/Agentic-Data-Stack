# Agentic Data Stack: Deployment Guide For Junior DevOps

Этот документ описывает, как поднять систему с нуля.

Он написан так, будто инфраструктуру разворачивает junior DevOps, который видит проект впервые.

Цель: развернуть отказоустойчивую систему, где компоненты находятся на разных машинах, данные из внешней БД попадают в ClickHouse через Debezium и Redpanda, а метрики Prometheus попадают в ClickHouse через отдельный Prometheus connector.

## 1. Что Мы Строим

Система нужна для аналитики данных, которые приходят из внешней БД.

Внешняя БД может быть не нашей. Например, PostgreSQL клиента, MySQL в другой сети или MongoDB в облаке.

Мы не забираем данные обычным SQL-скриптом.

Мы читаем поток изменений через **CDC**.

**CDC** означает **Change Data Capture**. Это подход, при котором система читает изменения из исходной БД: insert, update, delete.

Общая цепочка:

```text
External Database
  -> Debezium source connector
  -> Redpanda topic
  -> ClickHouse sink connector
  -> ClickHouse
  -> Grafana / MCP / LibreChat
  -> Langfuse traces for LLM requests
```

Для Prometheus цепочка другая:

```text
Prometheus
  -> remote_write или HTTP API query_range
  -> Prometheus connector
  -> ClickHouse
  -> MCP / LibreChat
```

По-человечески:

1. **Debezium** подключается к внешней БД и читает изменения.
2. **Redpanda** принимает эти изменения как поток сообщений.
3. **ClickHouse sink** читает поток из Redpanda и пишет строки в ClickHouse.
4. **Grafana** строит графики по ClickHouse.
5. **MCP server** дает LLM-модели инструменты для безопасной аналитики.
6. **LibreChat** дает человеку web UI для общения с моделью.
7. **Airflow** запускает регистрацию или обновление коннекторов по расписанию.
8. **Langfuse** показывает traces LLM-запросов: что отправили в модель, что получили, какая модель отвечала и сколько времени занял вызов.
9. **Prometheus connector** переносит метрики Prometheus в ClickHouse для анализа через LibreChat.

## 2. Что Делает Каждый Компонент
___
### Debezium

**Debezium** — это CDC-сервис.

Он не хранит данные как аналитическая БД, но подключается к source-БД и читает журнал изменений.

Для PostgreSQL это **WAL**.

- **WAL** означает **Write-Ahead Log**. Это журнал, куда PostgreSQL пишет изменения перед тем, как окончательно применить их в таблицах.

Для MySQL - **binlog**.

- **Binlog** означает **binary log**. Это журнал изменений MySQL.

Для MongoDB - **change stream**.

- **Change stream** — API MongoDB, который позволяет подписаться на изменения документов.

В этом проекте Debezium работает внутри Kafka Connect runtime.

**Важно**: Debezium не подходит для Prometheus.
Prometheus — не транзакционная БД с WAL/binlog/change stream.

У Prometheus другой способ интеграции, и в локальном проекте он сведен к двум командам:

- `sh tools/prometheus-stream-to-clickhouse.sh` — потоковая отправка новых samples;
- `sh tools/prometheus-batch-to-clickhouse.sh` — пакетная загрузка истории.

Поэтому для Prometheus в этом проекте используется отдельный `prometheus-connector`.
___

### Prometheus Connector

**Prometheus connector** — Node.js сервис, который переносит метрики Prometheus в ClickHouse.

В локальном compose-стеке его не нужно настраивать руками через длинные `curl` или ручное редактирование `prometheus.yml`: используйте две команды из `tools/`.

- **Batch command** — единоразовая или периодическая выгрузка истории.

Connector ходит в Prometheus HTTP API:

```text
/api/v1/query_range
```

- **Streaming command** — потоковая выгрузка почти в realtime.

Prometheus сам отправляет новые samples в endpoint:

```text
prometheus-connector:3355/api/v1/write
```

**Sample** — одно значение метрики в конкретный момент времени.

Например:

```text
up{job="api",instance="10.10.0.10:8080"} 1 1778500800000
```

В ClickHouse samples сохраняются в таблицу:

```text
analytics.prometheus_samples
```

Labels Prometheus сохраняются в поле `labels_json`.

___
### Redpanda

**Redpanda** — Kafka-compatible брокер сообщений.

Он хранит изменения в **topics**.

**Topic** — это именованный поток сообщений. Например:

```text
pg_flat.public.app_events
```

Debezium пишет события в topic.

ClickHouse sink читает этот topic.

Redpanda нужен, чтобы система не зависела от мгновенной доступности ClickHouse. Если ClickHouse временно недоступен, поток событий может ждать в Redpanda.
___
### ClickHouse

**ClickHouse** — аналитическая БД.

Она хорошо подходит для быстрых запросов по большим объемам логов и событий.

Например:

- сколько было ошибок по часам;
- какие routes медленные;
- какой error rate у endpoints;
- сколько токенов использовали модели;
- сколько стоили completions.

В этом проекте основная таблица demo-сценария:

```text
analytics.app_events_raw
```

**Важно**: ClickHouse не является первичной БД в этой архитектуре.

Первична та БД, из которой мы делаем миграцию: PostgreSQL, MySQL, MongoDB или другая source-БД.

ClickHouse — это аналитическая копия или аналитическая проекция.

Это значит, что структура ClickHouse должна подстраиваться под source-БД, а не наоборот.

Если у source-БД другая таблица, другие поля или другие типы данных, нельзя считать `app_events_raw` универсальной истиной.

Нужно либо создать ClickHouse tables под реальную source schema, либо автоматически сгенерировать их перед запуском sink connector.
___
### ClickHouse Sink

**ClickHouse sink** — это Kafka Connect connector.

Он читает данные из Redpanda topic и пишет их в ClickHouse table.

**Важно**: ClickHouse sink не подключается к внешней БД, а работает только после Debezium и Redpanda.

Конфиг лежит здесь:

```text
debezium/connectors/clickhouse-sink.json
```

Ключевая настройка:

```text
topic2TableMap
```

Она говорит:

```text
какой topic писать в какую ClickHouse table
```

Пример:

```text
pg.public.users=users_raw,pg.public.orders=orders_raw
```

Официальный ClickHouse Kafka Connect sink обычно предполагает, что target table в ClickHouse уже существует.

Поэтому есть два рабочих подхода:

1. **Manual schema mode** — DevOps заранее создает ClickHouse tables SQL-скриптами.
2. **Auto schema bootstrap mode** — отдельный bootstrap job сначала читает source schema и создает ClickHouse tables, а уже потом включается ClickHouse sink.

Второй вариант полезен, когда source-БД чужая и заранее неизвестно, какие таблицы и поля придется мигрировать.

Оба варианта реализованы в проекте.
___
### Grafana

**Grafana** — web UI для графиков.

Она подключается к ClickHouse и строит dashboards.

В проекте Grafana также нужна потому, что LLM-модель не всегда хорошо рисует картинки сама.

Лучше, чтобы модель возвращала ссылку на Grafana panel.
___
### MCP Server

**MCP** означает **Model Context Protocol**.

MCP server дает модели набор tools.

В этом проекте tools умеют:

- смотреть схему ClickHouse;
- находить все и непустые таблицы в базе `analytics`;
- профилировать любую актуальную таблицу из `analytics`;
- выбирать sample rows;
- считать уникальные значения и распределения по указанным колонкам;
- считать error rate;
- считать latency;
- возвращать ссылки на Grafana.

Сырой SQL-tool в LibreChat не публикуется. Модель должна понять вопрос пользователя, выбрать подходящий ClickHouse MCP tool, дождаться live-ответа ClickHouse и только после этого сформулировать ответ. Это важно, потому что таблицы и данные могут измениться после написания документации.
___
### LibreChat

**LibreChat** — web UI для общения с LLM-моделью.

Пользователь задает вопрос в LibreChat.

Модель через MCP tools обращается к ClickHouse и Grafana.
___
### Agent Proxy

**agent-proxy** — маленький OpenAI-compatible proxy.

OpenAI-compatible означает, что сервис выглядит для LibreChat как OpenAI API, даже если реальная модель запущена локально в Ollama или в другом облачном провайдере.

В этом проекте `agent-proxy` делает две вещи.

Во-первых, он пересылает запросы LibreChat в upstream model endpoint.

**Upstream** — это сервис выше по цепочке. Например, Ollama на Mac по адресу `http://host.docker.internal:11434/v1`.

Во-вторых, `agent-proxy` отправляет trace каждого LLM-запроса в Langfuse.

Это удобная точка интеграции, потому что почти все запросы LibreChat к модели проходят через этот proxy.
___
### Langfuse

**Langfuse** — observability-платформа для LLM-приложений.

**Observability** означает наблюдаемость.

В обычном приложении мы смотрим logs, metrics и traces.

В LLM-приложении этого мало: нужно видеть еще **prompt**, **input**, **output**, **model**, **latency**, **usage tokens**, ошибки и metadata.

**Trace** — запись одного пользовательского сценария или одного запроса.

Например, пользователь спросил в LibreChat:

```text
Какие endpoints самые проблемные по error rate?
```

В Langfuse такой запрос появится как trace.

Внутри trace будет generation.

**Generation** — конкретный вызов LLM-модели: модель, параметры, входные сообщения и ответ.

В этом проекте Langfuse работает так:

```text
LibreChat
  -> agent-proxy
  -> local/cloud LLM

agent-proxy
  -> Langfuse ingestion API
  -> Langfuse worker
  -> ClickHouse/Postgres/MinIO/Redis
```

Langfuse использует несколько внутренних хранилищ:

- **Postgres** хранит пользователей, organization, projects, API keys и настройки.


- **ClickHouse** хранит traces, observations и score-сущности, потому что это аналитические данные.


- **Redis** используется для очередей и cache.


- **MinIO** используется как S3-compatible object storage.


- **S3-compatible** означает, что сервис говорит тем же API, что Amazon S3. Для локального запуска удобно использовать MinIO.

В production Langfuse часто ставят отдельно от основного приложения, чтобы команда могла анализировать качество LLM, debug-ить плохие ответы, смотреть latency, считать стоимость и сравнивать разные модели.
___
### Airflow

**Airflow** — планировщик задач.

Он нужен, когда регистрацию или обновление Debezium connectors надо запускать в определенный день или время.

Например:

- каждый день в 02:00;
- каждый понедельник в 03:30;
- первого числа каждого месяца.

Airflow запускает DAG:

```text
scheduled_debezium_migration
```

**DAG** — это workflow в Airflow. Можно думать о нем как о сценарии из шагов.

В нашем DAG один основной шаг:

```text
apply_active_connectors
```

Он создает или обновляет active source connector и ClickHouse sink connector.

## 3. Рекомендуемая Схема Машин

В этом разделе есть две схемы.

Первая — минимальная, чтобы поднять проект на ноутбуке и спокойно проверить всю цепочку.

Вторая — рекомендованная production-like схема, когда компоненты раскладываются по разным машинам для отказоустойчивости.
___
### Минимальная Схема Для Ноутбука

Эта схема подходит для локального запуска на MacBook с Apple Silicon, например M4, 10 CPU cores и 24 GB RAM.

Это не HA.

**HA** означает **High Availability**, то есть отказоустойчивость. В минимальной схеме отказоустойчивости нет: если ноутбук или VM упали, вся система остановится.

- ### Вариант A: Все На Одной Машине

Самый простой вариант — создать локальный all-in-one `docker-compose.yml` по этому документу и запустить его прямо на ноутбуке.

| Машина | Компоненты | CPU | RAM | Disk | Комментарий |
|---|---|---:|---:|---:|---|
| `laptop` | все сервисы проекта, включая Langfuse | 8-10 cores | 20-22 GB доступно Docker | 120-180 GB free | удобно для demo и разработки |

Для Docker Desktop на macOS рекомендуется выделить:

```text
CPU: 8-10 cores
Memory: 20-22 GB
Swap: 2-4 GB
Disk image: 120 GB или больше
```


Команда запуска:

```bash
cd /opt/agentic-data-stack
cp .env.example .env
docker compose up -d --build
```

Для локальной проверки включить demo-режим:

```env
SOURCE_MODE=demo
ACTIVE_SOURCE_DB=postgres
COMPOSE_PROFILES=postgres-source
```

- ### Вариант B: Несколько Маленьких VM На Ноутбуке

Если нужно приблизиться к настоящей инфраструктуре, можно поднять несколько VM через UTM, Lima, Multipass или другой локальный virtualization tool.

На ноутбуке с 24 GB RAM не стоит делать много больших VM.

Лучше сделать 3-4 небольшие VM и не пытаться эмулировать полный production-кластер.

| VM | Компоненты | CPU | RAM | Disk | Комментарий |
|---|---|---:|---:|---:|---|
| `vm-data` | Redpanda, ClickHouse, demo PostgreSQL | 4 vCPU | 10-12 GB | 100 GB | data plane и ClickHouse для analytics/langfuse |
| `vm-orch` | Airflow DB, Airflow webserver, Airflow scheduler | 2 vCPU | 4 GB | 40 GB | расписание миграций |
| `vm-app` | Grafana, MCP, agent-proxy, LibreChat, MongoDB, Langfuse, Redis, MinIO | 4 vCPU | 10 GB | 80 GB | UI/API и LLM observability |

Если хочется еще проще, можно сделать 2 VM:

| VM | Компоненты | CPU | RAM | Disk |
|---|---|---:|---:|---:|
| `vm-data` | Redpanda, ClickHouse, Debezium, demo PostgreSQL | 5 vCPU | 12 GB | 100 GB |
| `vm-app` | Airflow, Grafana, MCP, agent-proxy, LibreChat, MongoDB, Langfuse, Redis, MinIO | 5 vCPU | 10 GB | 100 GB |

Главная цель такой минимальной схемы — проверить сетевое взаимодействие.

Например, чтобы `vm-app` ходила в ClickHouse на `vm-data`, а Airflow мог достучаться до Debezium REST API.

Важно: даже если сервисы разнесены по VM, это все еще не полноценная HA-схема.

Если одна VM упала, часть системы остановится.
___
### Рекомендованная HA-Схема

| Машина | Компоненты | CPU | RAM | Disk | Комментарий |
|---|---|---:|---:|---:|---|
| `rp-1` | Redpanda broker 1 | 4 vCPU | 16 GB | 200 GB NVMe | Kafka-compatible broker |
| `rp-2` | Redpanda broker 2 | 4 vCPU | 16 GB | 200 GB NVMe | второй broker |
| `rp-3` | Redpanda broker 3 | 4 vCPU | 16 GB | 200 GB NVMe | третий broker, quorum |
| `ch-1` | ClickHouse replica 1 | 8 vCPU | 32 GB | 1 TB NVMe | аналитика |
| `ch-2` | ClickHouse replica 2 | 8 vCPU | 32 GB | 1 TB NVMe | отказоустойчивость |
| `connect-1` | Debezium/Kafka Connect worker 1 | 4 vCPU | 8-16 GB | 100 GB SSD | CDC worker |
| `connect-2` | Debezium/Kafka Connect worker 2 | 4 vCPU | 8-16 GB | 100 GB SSD | второй worker |
| `airflow-1` | Airflow webserver/scheduler | 4 vCPU | 8 GB | 100 GB SSD | orchestration |
| `meta-1` | Airflow metadata PostgreSQL | 2-4 vCPU | 8 GB | 100 GB SSD | лучше managed DB |
| `app-1` | LibreChat, MCP, agent-proxy, Grafana | 4-8 vCPU | 16 GB | 100 GB SSD | UI/API |
| `mongo-1` | LibreChat MongoDB | 2-4 vCPU | 8 GB | 100 GB SSD | для production лучше replica set |
| `lf-web-1` | Langfuse web и worker | 4 vCPU | 8-16 GB | 100 GB SSD | LLM observability |
| `lf-meta-1` | Langfuse PostgreSQL, Redis, object storage gateway | 4 vCPU | 8-16 GB | 200 GB SSD | для production лучше managed services |

### Почему Так

Redpanda лучше запускать минимум в 3 узла, так кластер может пережить потерю одного broker.

ClickHouse лучше держать минимум в 2 реплики - так можно читать аналитику даже при проблеме на одной машине.

Debezium/Kafka Connect можно запускать в 2 worker. Если один worker упадет, второй сможет забрать задачи.

Airflow metadata DB лучше держать отдельно.
Airflow webserver и scheduler не должны хранить state только внутри контейнера.

LibreChat и Grafana можно сначала поставить на одну app-машину.
Если нагрузка вырастет, их можно разнести.

Langfuse можно сначала поставить рядом с app-компонентами.
Для production лучше выделить Langfuse отдельно, потому что traces могут быстро расти.

Langfuse ClickHouse database можно держать в том же ClickHouse cluster, но обязательно в отдельной database, например `langfuse`.

## 4. Сеть И DNS

Все машины должны видеть друг друга по стабильным именам.

Можно использовать внутренний DNS, например:

```text
rp-1.internal
rp-2.internal
rp-3.internal
ch-1.internal
ch-2.internal
connect-1.internal
connect-2.internal
airflow-1.internal
app-1.internal
```

Если DNS нет, можно временно использовать `/etc/hosts`.

Пример:

```bash
sudo nano /etc/hosts
```

Добавить:

```text
10.10.0.11 rp-1.internal
10.10.0.12 rp-2.internal
10.10.0.13 rp-3.internal
10.10.0.21 ch-1.internal
10.10.0.22 ch-2.internal
10.10.0.31 connect-1.internal
10.10.0.32 connect-2.internal
10.10.0.41 airflow-1.internal
10.10.0.51 app-1.internal
10.10.0.61 lf-web-1.internal
10.10.0.62 lf-meta-1.internal
```

## 5. Порты

Открывайте только нужные порты.

| Компонент | Порт | Откуда Доступ | Для Чего |
|---|---:|---|---|
| Redpanda Kafka API | `9092` | Debezium workers | чтение/запись topics |
| Redpanda Admin API | `9644` | ops/admin only | диагностика |
| Debezium Connect REST | `8083` | Airflow, ops/admin | регистрация connectors |
| ClickHouse HTTP | `8123` | Grafana, MCP, sink | SQL по HTTP |
| ClickHouse native | `9000` | ops/admin | native client |
| Grafana | `3001` или `3000` | users/admin | dashboards |
| Prometheus connector | `3355` | Prometheus, ops/admin | remote_write и backfill |
| Langfuse UI | `3002` или `3000` | users/admin | LLM traces и observability |
| Langfuse worker | `3030` | internal only | background jobs |
| Langfuse PostgreSQL | `5432` | Langfuse only | users/projects/settings |
| Langfuse Redis | `6379` | Langfuse only | queues/cache |
| Langfuse MinIO S3 API | `9090` или `9000` | Langfuse only | event/media object storage |
| Langfuse MinIO Console | `9091` или `9001` | ops/admin only | управление local object storage |
| LibreChat | `3080` | users | chat UI |
| MCP server | `3333` | LibreChat/app network | tools для модели |
| agent-proxy | `3344` | LibreChat/app network | OpenAI-compatible proxy |
| Airflow UI | `8081` | ops/admin | scheduler UI |
| Airflow metadata DB | `5432` | Airflow only | metadata |
| LibreChat MongoDB | `27017` | LibreChat only | user/chat metadata |

Внешнюю source-БД открывайте только для `connect-1` и `connect-2`.

Например, если source PostgreSQL находится у клиента, в allowlist должны попасть IP адреса Debezium workers.

Prometheus должен иметь доступ к `prometheus-connector:3355`, если используется `remote_write`.

Если Prometheus находится вне Docker network, откройте внешний порт `3355` только для IP адреса Prometheus.

## 6. Подготовка Ubuntu На Каждой Машине

Ниже команды для Ubuntu Server 22.04/24.04.

Выполнить на каждой машине.

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl git gnupg lsb-release ufw
```

Установить Docker repository:

```bash
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
```

Добавить Docker apt source:

```bash
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
```

Установить Docker:

```bash
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

Добавить пользователя в группу Docker:

```bash
sudo usermod -aG docker "$USER"
```

После этого нужно перелогиниться.

Проверить:

```bash
docker --version
docker compose version
```

## 7. Создание Проекта С Нуля

Создаем проект руками: директории, `.env`, `docker-compose.yml`, Debezium connector templates, ClickHouse schema, Airflow DAG и минимальные Node.js сервисы.

Сначала создаем базовую директорию.

Выполнить на машине, где будет собираться проект:

```bash
sudo mkdir -p /opt/agentic-data-stack
sudo chown "$USER":"$USER" /opt/agentic-data-stack
cd /opt/agentic-data-stack
```

Создать структуру каталогов:

```bash
mkdir -p \
  agent-proxy/src \
  airflow/dags \
  clickhouse/init \
  debezium/connectors \
  debezium/plugins \
  grafana/provisioning/datasources \
  grafana/provisioning/dashboards \
  grafana/dashboards \
  librechat \
  mcp-server/src \
  prometheus-connector/src \
  prometheus-connector/proto \
  postgres/init \
  docs
```

Что это значит:

- `agent-proxy` — маленький OpenAI-compatible proxy для LibreChat.
- `airflow/dags` — DAG-файлы Airflow.
- `clickhouse/init` — SQL, который создает базу и таблицы ClickHouse.
- `debezium/connectors` — JSON-шаблоны Debezium source/sink connectors.
- `debezium/plugins` — плагины Kafka Connect, включая ClickHouse sink connector.
- `grafana/provisioning` — автоматическое подключение datasource и dashboards.
- `librechat` — шаблон конфигурации LibreChat.
- `mcp-server` — tools для модели.
- `prometheus-connector` — прием `remote_write` и backfill из Prometheus HTTP API.
- `postgres/init` — demo PostgreSQL seed.

### 7.1 Создать `.gitignore`

Файл `.gitignore` нужен, чтобы случайно не закоммитить реальные пароли и локальные файлы.

```bash
cat > .gitignore <<'EOF'
.env
.DS_Store
.idea/
.vscode/
node_modules/
npm-debug.log*
.env.local
.env.*.local
__pycache__/
*.py[cod]
EOF
```

### 7.2 Создать `.env.example`

`.env.example` — это шаблон.

В нем не должно быть реальных production passwords.

```bash
cat > .env.example <<'EOF'
SOURCE_MODE=external
# SOURCE_MODE=demo

ACTIVE_SOURCE_DB=postgres
# ACTIVE_SOURCE_DB=mysql
# ACTIVE_SOURCE_DB=mongodb

# Demo PostgreSQL starts only when this profile is enabled.
# COMPOSE_PROFILES=postgres-source

POSTGRES_DB=app_logs
POSTGRES_USER=app
POSTGRES_PASSWORD=app_password

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

# MYSQL_SOURCE_HOST=customer-mysql.example.com
# MYSQL_SOURCE_PORT=3306
# MYSQL_SOURCE_USER=debezium_user
# MYSQL_SOURCE_PASSWORD=change-me-source-password
# MYSQL_SOURCE_DB=customer_app
# MYSQL_SOURCE_TOPIC_PREFIX=customer_mysql
# MYSQL_SOURCE_TABLE=app_events
# MYSQL_SOURCE_SERVER_ID=184054
# MYSQL_SOURCE_SSL_MODE=preferred
# MYSQL_SOURCE_TOPIC=customer_mysql.customer_app.app_events

# MONGODB_SOURCE_CONNECTION_STRING=mongodb://user:password@customer-mongo.example.com:27017/?replicaSet=rs0&authSource=admin
# MONGODB_SOURCE_DB=customer_app
# MONGODB_SOURCE_COLLECTION=app_events
# MONGODB_SOURCE_TOPIC_PREFIX=customer_mongo
# MONGODB_SOURCE_TOPIC=customer_mongo.customer_app.app_events

CLICKHOUSE_DB=analytics
CLICKHOUSE_USER=analytics
CLICKHOUSE_PASSWORD=analytics_password
CLICKHOUSE_SINK_TABLE=app_events_raw

PROMETHEUS_BASE_URL=http://prometheus:9090
PROMETHEUS_BEARER_TOKEN=
PROMETHEUS_BASIC_USER=
PROMETHEUS_BASIC_PASSWORD=
PROMETHEUS_BACKFILL_QUERY=up
PROMETHEUS_BACKFILL_STEP=60s
PROMETHEUS_SOURCE_NAME=prometheus
PROMETHEUS_DEBUG_JSON_ENABLED=true

GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin

LANGFUSE_PUBLIC_URL=http://localhost:3002
LANGFUSE_INTERNAL_URL=http://langfuse-web:3000
LANGFUSE_ENABLED=true
LANGFUSE_ENVIRONMENT=local
LANGFUSE_TELEMETRY_ENABLED=false
LANGFUSE_POSTGRES_DB=langfuse
LANGFUSE_POSTGRES_USER=langfuse
LANGFUSE_POSTGRES_PASSWORD=langfuse_password
LANGFUSE_REDIS_AUTH=langfuse_redis_password
LANGFUSE_MINIO_ROOT_USER=minio
LANGFUSE_MINIO_ROOT_PASSWORD=minio_password
LANGFUSE_S3_BUCKET=langfuse
LANGFUSE_CLICKHOUSE_DB=langfuse
LANGFUSE_NEXTAUTH_SECRET=change-me-generate-secure-langfuse-nextauth-secret
LANGFUSE_SALT=change-me-generate-secure-langfuse-salt
LANGFUSE_ENCRYPTION_KEY=0000000000000000000000000000000000000000000000000000000000000000
LANGFUSE_INIT_ORG_ID=agentic-data-stack-org
LANGFUSE_INIT_ORG_NAME=Agentic Data Stack
LANGFUSE_INIT_PROJECT_ID=agentic-data-stack-project
LANGFUSE_INIT_PROJECT_NAME=Agentic Data Stack LLM
LANGFUSE_INIT_USER_EMAIL=admin@example.com
LANGFUSE_INIT_USER_NAME=Admin
LANGFUSE_INIT_USER_PASSWORD=admin123456
LANGFUSE_PUBLIC_KEY=pk-lf-agentic-data-stack-local
LANGFUSE_SECRET_KEY=sk-lf-agentic-data-stack-local

AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com
AIRFLOW_WEBSERVER_SECRET_KEY=change-me-airflow-webserver-secret
AIRFLOW_MIGRATION_CRON=0 2 * * *
AIRFLOW_DAG_PAUSED=true

LIBRECHAT_JWT_SECRET=change-me-generate-secure-jwt-secret
LIBRECHAT_JWT_REFRESH_SECRET=change-me-generate-secure-jwt-refresh-secret
ALLOW_EMAIL_LOGIN=true
ALLOW_REGISTRATION=true
ALLOW_UNVERIFIED_EMAIL_LOGIN=true
ALLOW_PASSWORD_RESET=false

AGENT_PROXY_API_KEY=local-dev-key
AGENT_PROXY_BASE_URL=http://agent-proxy:3344/v1
UPSTREAM_OPENAI_API_KEY=local-dev-key
UPSTREAM_OPENAI_BASE_URL=http://host.docker.internal:11434/v1

OPENAI_MODEL=qwen2.5:7b
OPENAI_MODEL_FAST=qwen2.5:7b
OPENAI_MODEL_SMART=qwen3:14b
OPENAI_MODEL_VISION=llama3.2-vision:latest
OPENAI_EMBEDDING_MODEL=nomic-embed-text:latest
LIBRECHAT_MODELS=qwen2.5:7b,qwen2.5:14b,qwen3:14b,llama3.2-vision:latest
LIBRECHAT_TITLE_MODEL=qwen2.5:7b
LIBRECHAT_SUMMARY_MODEL=qwen2.5:7b
EOF
```

Создать рабочий `.env`:

```bash
cp .env.example .env
```

Для локального demo-режима отредактировать `.env` так:

```env
SOURCE_MODE=demo
ACTIVE_SOURCE_DB=postgres
COMPOSE_PROFILES=postgres-source
POSTGRES_SOURCE_HOST=postgres
POSTGRES_SOURCE_USER=app
POSTGRES_SOURCE_PASSWORD=app_password
POSTGRES_SOURCE_DB=app_logs
POSTGRES_SOURCE_TOPIC_PREFIX=pg_flat
POSTGRES_SOURCE_SLOT=app_events_slot
POSTGRES_SOURCE_PUBLICATION=app_events_publication
POSTGRES_SOURCE_SSL_MODE=disable
POSTGRES_SOURCE_TOPIC=pg_flat.public.app_events
```

Для production оставить `SOURCE_MODE=external` и заполнить реальные **host**, **port**, **user**, **password**.

### 7.3 Решить, Как Создавать ClickHouse Schema

Сначала важно договориться о принципе.

**Source-БД первична.**

ClickHouse schema должна быть следствием source schema.

Например, если внешняя PostgreSQL таблица называется `orders`, то ClickHouse table может называться `orders_raw`.

Если внешняя MySQL таблица называется `payments`, то ClickHouse table может называться `payments_raw`.

Не надо заставлять чужую БД подстраиваться под `app_events_raw`.

`app_events_raw` — это только demo-таблица для локального примера.

Есть два варианта работы.

- ### Вариант A: Manual Schema

DevOps заранее создает ClickHouse tables SQL-файлами.

Этот вариант проще, надежнее и лучше подходит для production, когда schema известна и согласована.

Плюсы:

- понятно, какие типы данных будут в ClickHouse;
- проще ревьюить изменения;
- меньше риска, что auto-generator создаст неудобные типы;
- проще оптимизировать `ORDER BY`, partitioning и indexes.

Минусы:

- нужно руками поддерживать ClickHouse schema при изменениях source-БД.

Не подходит для продакшн-среды

- ### Вариант B: Auto Schema Bootstrap

Мы можем вообще не создавать ClickHouse structure руками.

Вместо этого перед запуском ClickHouse sink запускается отдельный bootstrap job.

Этот job:

1. Подключается к source-БД.
2. Читает metadata: tables, columns, types.
3. Преобразует типы source-БД в типы ClickHouse.
4. Выполняет `CREATE DATABASE IF NOT EXISTS`.
5. Выполняет `CREATE TABLE IF NOT EXISTS`.
6. После этого запускается ClickHouse sink connector.

Важно: это не обязанность самого ClickHouse sink connector.

В нашей архитектуре автоматическое создание schema лучше делать отдельным шагом: `schema-bootstrap`.

Так проще контролировать ошибки, логику преобразования типов и безопасность.

Пример для PostgreSQL:

```text
PostgreSQL information_schema.columns
  -> schema-bootstrap
  -> CREATE TABLE analytics.orders_raw (...)
  -> Debezium source connector
  -> ClickHouse sink connector
```

#### Пример Auto Schema Bootstrap Для PostgreSQL

Ниже пример скрипта, который читает структуру одной PostgreSQL таблицы и создает похожую ClickHouse table.

Это базовый пример.

Для production его нужно расширить под arrays, json, numeric precision, dates, nullable columns, primary keys и naming rules.

Создать файл:

```bash
mkdir -p schema-bootstrap
cat > schema-bootstrap/postgres-to-clickhouse-schema.mjs <<'EOF'
import { Client as PgClient } from 'pg';
import { createClient as createClickHouseClient } from '@clickhouse/client';

const pg = new PgClient({
  host: process.env.POSTGRES_SOURCE_HOST,
  port: Number(process.env.POSTGRES_SOURCE_PORT || 5432),
  user: process.env.POSTGRES_SOURCE_USER,
  password: process.env.POSTGRES_SOURCE_PASSWORD,
  database: process.env.POSTGRES_SOURCE_DB,
  ssl: process.env.POSTGRES_SOURCE_SSL_MODE === 'disable' ? false : { rejectUnauthorized: false },
});

const clickhouse = createClickHouseClient({
  url: process.env.CLICKHOUSE_HOST || 'http://clickhouse:8123',
  username: process.env.CLICKHOUSE_USER,
  password: process.env.CLICKHOUSE_PASSWORD,
  database: process.env.CLICKHOUSE_DB,
});

function toClickHouseType(row) {
  const type = row.data_type;
  const nullable = row.is_nullable === 'YES';

  const map = {
    bigint: 'Int64',
    integer: 'Int32',
    smallint: 'Int16',
    boolean: 'Bool',
    real: 'Float32',
    'double precision': 'Float64',
    numeric: 'Decimal(18, 6)',
    text: 'String',
    'character varying': 'String',
    uuid: 'UUID',
    json: 'String',
    jsonb: 'String',
    timestamp: 'DateTime64(6)',
    'timestamp without time zone': 'DateTime64(6)',
    'timestamp with time zone': 'DateTime64(6)',
    date: 'Date',
  };

  const chType = map[type] || 'String';
  return nullable ? `Nullable(${chType})` : chType;
}

await pg.connect();

const sourceSchema = process.env.POSTGRES_SOURCE_SCHEMA || 'public';
const sourceTable = process.env.POSTGRES_SOURCE_TABLE;
const targetDb = process.env.CLICKHOUSE_DB;
const targetTable = process.env.CLICKHOUSE_SINK_TABLE || `${sourceTable}_raw`;

const result = await pg.query(
  `
  SELECT column_name, data_type, is_nullable
  FROM information_schema.columns
  WHERE table_schema = $1 AND table_name = $2
  ORDER BY ordinal_position
  `,
  [sourceSchema, sourceTable],
);

if (result.rows.length === 0) {
  throw new Error(`No columns found for ${sourceSchema}.${sourceTable}`);
}

const columns = result.rows
  .map((row) => `  \`${row.column_name}\` ${toClickHouseType(row)}`)
  .join(',\n');

const sql = `
CREATE DATABASE IF NOT EXISTS ${targetDb};

CREATE TABLE IF NOT EXISTS ${targetDb}.${targetTable}
(
${columns},
  \`ingest_time\` DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY tuple();
`;

console.log(sql);
await clickhouse.command({ query: sql });
await pg.end();
await clickhouse.close();
EOF
```

Создать `package.json`:

```bash
cat > schema-bootstrap/package.json <<'EOF'
{
  "name": "schema-bootstrap",
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "postgres": "node postgres-to-clickhouse-schema.mjs"
  },
  "dependencies": {
    "@clickhouse/client": "^1.12.1",
    "pg": "^8.13.1"
  }
}
EOF
```

Пример compose service:

```yaml
schema-bootstrap:
  image: node:22-alpine
  working_dir: /app
  env_file:
    - .env
  environment:
    CLICKHOUSE_HOST: http://clickhouse:8123
  volumes:
    - ./schema-bootstrap:/app:ro
  command: sh -c "npm install && npm run postgres"
```

Порядок запуска:

```bash
docker compose up schema-bootstrap
docker compose up connectors-init
```

Так ClickHouse table появится до старта sink connector.

### 7.4 Создать Demo ClickHouse Schema Вручную

Этот SQL создает demo-таблицу для событий.

Используйте этот вариант для локального demo.

Если включаете auto schema bootstrap, этот файл можно не создавать или оставить пустым.

```bash
cat > clickhouse/init/001_schema.sql <<'EOF'
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

CREATE TABLE IF NOT EXISTS analytics.prometheus_samples
(
  metric_name LowCardinality(String),
  labels_json String,
  fingerprint FixedString(64),
  sample_time DateTime64(3, 'UTC'),
  value Float64,
  source LowCardinality(String) DEFAULT 'prometheus',
  ingest_mode LowCardinality(String) DEFAULT 'remote_write',
  ingest_time DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(sample_time)
ORDER BY (metric_name, fingerprint, sample_time, source, ingest_mode);

CREATE VIEW IF NOT EXISTS analytics.v_prometheus_metric_summary AS
SELECT
  toStartOfMinute(sample_time) AS minute,
  metric_name,
  count() AS samples,
  min(value) AS min_value,
  max(value) AS max_value,
  avg(value) AS avg_value,
  quantile(0.95)(value) AS p95_value
FROM analytics.prometheus_samples
GROUP BY minute, metric_name
ORDER BY minute DESC, metric_name ASC;

CREATE VIEW IF NOT EXISTS analytics.v_prometheus_targets AS
SELECT
  JSONExtractString(labels_json, 'job') AS job,
  JSONExtractString(labels_json, 'instance') AS instance,
  max(sample_time) AS last_sample_time,
  argMax(value, sample_time) AS last_up,
  min(value) AS min_up,
  avg(value) AS avg_up
FROM analytics.prometheus_samples
WHERE metric_name = 'up'
GROUP BY job, instance
ORDER BY last_up ASC, job ASC, instance ASC;
EOF
```

### 7.5 Создать Demo PostgreSQL Seed

Этот файл нужен только для demo-режима.

Он создает таблицу `app_events` и наполняет ее тестовыми логами.

Сначала - тестовые логи.

Уже после того как соберете проект - подтянете нормальную БД.

```bash
cat > postgres/init/001_logs.sql <<'EOF'
CREATE TABLE IF NOT EXISTS app_events (
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

CREATE INDEX IF NOT EXISTS idx_app_events_event_time ON app_events(event_time);
CREATE INDEX IF NOT EXISTS idx_app_events_user_id ON app_events(user_id);
CREATE INDEX IF NOT EXISTS idx_app_events_event_type ON app_events(event_type);

ALTER TABLE app_events REPLICA IDENTITY FULL;

INSERT INTO app_events (
  event_time,
  user_id,
  session_id,
  event_type,
  route,
  status_code,
  latency_ms,
  model_name,
  prompt_tokens,
  completion_tokens,
  total_cost_usd,
  metadata
)
SELECT
  now() - (gs || ' minutes')::interval,
  'user_' || ((gs % 25) + 1),
  'session_' || ((gs % 80) + 1),
  (ARRAY['page_view', 'chat_message', 'tool_call', 'model_completion', 'error'])[1 + (gs % 5)],
  (ARRAY['/', '/chat', '/agents', '/settings', '/api/completions'])[1 + (gs % 5)],
  CASE WHEN gs % 17 = 0 THEN 500 WHEN gs % 11 = 0 THEN 429 ELSE 200 END,
  50 + (random() * 2500)::int,
  (ARRAY['gpt-4o-mini', 'claude-3-5-sonnet', 'local-llama', 'gemini-1.5-pro'])[1 + (gs % 4)],
  50 + (random() * 1500)::int,
  20 + (random() * 1200)::int,
  round((random() * 0.08)::numeric, 6),
  jsonb_build_object('source', 'seed', 'environment', 'local')
FROM generate_series(1, 1000) AS gs;
EOF
```

### 7.6 Создать Debezium Connector Templates

PostgreSQL source connector:

```bash
cat > debezium/connectors/postgres-source.json <<'EOF'
{
  "name": "postgres-app-events-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "${POSTGRES_SOURCE_HOST}",
    "database.port": "${POSTGRES_SOURCE_PORT}",
    "database.user": "${POSTGRES_SOURCE_USER}",
    "database.password": "${POSTGRES_SOURCE_PASSWORD}",
    "database.dbname": "${POSTGRES_SOURCE_DB}",
    "database.server.name": "${POSTGRES_SOURCE_DB}",
    "database.sslmode": "${POSTGRES_SOURCE_SSL_MODE}",
    "topic.prefix": "${POSTGRES_SOURCE_TOPIC_PREFIX}",
    "schema.include.list": "${POSTGRES_SOURCE_SCHEMA}",
    "table.include.list": "${POSTGRES_SOURCE_SCHEMA}.${POSTGRES_SOURCE_TABLE}",
    "slot.name": "${POSTGRES_SOURCE_SLOT}",
    "publication.name": "${POSTGRES_SOURCE_PUBLICATION}",
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

MySQL source connector:

```bash
cat > debezium/connectors/mysql-source.json <<'EOF'
{
  "name": "mysql-app-events-source",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "${MYSQL_SOURCE_HOST}",
    "database.port": "${MYSQL_SOURCE_PORT}",
    "database.user": "${MYSQL_SOURCE_USER}",
    "database.password": "${MYSQL_SOURCE_PASSWORD}",
    "database.server.id": "${MYSQL_SOURCE_SERVER_ID}",
    "database.ssl.mode": "${MYSQL_SOURCE_SSL_MODE}",
    "topic.prefix": "${MYSQL_SOURCE_TOPIC_PREFIX}",
    "database.include.list": "${MYSQL_SOURCE_DB}",
    "table.include.list": "${MYSQL_SOURCE_DB}.${MYSQL_SOURCE_TABLE}",
    "snapshot.mode": "initial",
    "include.schema.changes": "false",
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

MongoDB source connector:

```bash
cat > debezium/connectors/mongodb-source.json <<'EOF'
{
  "name": "mongodb-app-events-source",
  "config": {
    "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
    "mongodb.connection.string": "${MONGODB_SOURCE_CONNECTION_STRING}",
    "topic.prefix": "${MONGODB_SOURCE_TOPIC_PREFIX}",
    "database.include.list": "${MONGODB_SOURCE_DB}",
    "collection.include.list": "${MONGODB_SOURCE_DB}.${MONGODB_SOURCE_COLLECTION}",
    "snapshot.mode": "initial",
    "capture.mode": "change_streams_update_full",
    "tombstones.on.delete": "false",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}
EOF
```

ClickHouse sink connector:

```bash
cat > debezium/connectors/clickhouse-sink.json <<'EOF'
{
  "name": "clickhouse-app-events-sink",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "tasks.max": "1",
    "topics": "${ACTIVE_SOURCE_TOPIC}",
    "hostname": "clickhouse",
    "port": "8123",
    "database": "${CLICKHOUSE_DB}",
    "username": "${CLICKHOUSE_USER}",
    "password": "${CLICKHOUSE_PASSWORD}",
    "ssl": "false",
    "topic2TableMap": "${ACTIVE_SOURCE_TOPIC}=${CLICKHOUSE_SINK_TABLE}",
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

### 7.7 Скачать ClickHouse Sink Plugin

Debezium image не содержит ClickHouse sink plugin из коробки.

Без этого JAR-файла Kafka Connect не увидит class:

```text
com.clickhouse.kafka.connect.ClickHouseSinkConnector
```

Создайте папку:

```bash
mkdir -p debezium/plugins/clickhouse-kafka-connect
```

Скачайте plugin из официального релиза ClickHouse Kafka Connect или из Maven/Confluent artifact repository.

После скачивания JAR должен лежать внутри:

```text
debezium/plugins/clickhouse-kafka-connect/
```

Проверка после запуска Debezium:

```bash
curl http://localhost:8083/connector-plugins | grep ClickHouseSinkConnector
```

Если grep ничего не вернул, plugin не установлен или лежит не в той папке.

### 7.8 Создать Registrar Для Connectors

Этот скрипт подставляет `.env` в JSON templates и регистрирует connectors в Debezium REST API.

```bash
cat > debezium/register-connectors.mjs <<'EOF'
import fs from 'node:fs/promises';
import net from 'node:net';

const connectUrl = (process.env.CONNECT_URL || 'http://debezium:8083').replace(/\/$/, '');
const sourceMode = (process.env.SOURCE_MODE || 'external').trim().toLowerCase();
const activeSource = (process.env.ACTIVE_SOURCE_DB || 'postgres').trim().toLowerCase();
const connectorsDir = '/connectors';
const sourceTemplate = `${connectorsDir}/${activeSource}-source.json`;
const sinkTemplate = `${connectorsDir}/clickhouse-sink.json`;
const activePrefix = `${activeSource.toUpperCase()}_SOURCE`;
process.env.ACTIVE_SOURCE_TOPIC = process.env[`${activePrefix}_TOPIC`] || '';

function requiredEnv(name) {
  const value = process.env[name];
  if (value === undefined || value === '') {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function renderTemplate(text) {
  return text.replace(/\$\{([A-Z0-9_]+)\}/g, (_, name) => requiredEnv(name));
}

async function readConnector(path) {
  const rendered = renderTemplate(await fs.readFile(path, 'utf8'));
  return JSON.parse(rendered);
}

async function readConnectorName(path) {
  return JSON.parse(await fs.readFile(path, 'utf8')).name;
}

async function request(path, options = {}) {
  const response = await fetch(`${connectUrl}${path}`, {
    ...options,
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${options.method || 'GET'} ${path} failed with ${response.status}: ${text}`);
  }
  return text ? JSON.parse(text) : null;
}

async function waitForConnect() {
  for (let attempt = 1; attempt <= 60; attempt += 1) {
    try {
      await request('/connectors');
      return;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
  throw new Error(`Kafka Connect did not become ready at ${connectUrl}`);
}

function waitForPort(host, port) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host, port: Number(port) });
    socket.setTimeout(3000);
    socket.once('connect', () => {
      socket.end();
      resolve();
    });
    socket.once('timeout', () => {
      socket.destroy();
      reject(new Error('timeout'));
    });
    socket.once('error', reject);
  });
}

async function waitForSource() {
  const host = process.env[`${activePrefix}_HOST`];
  const port = process.env[`${activePrefix}_PORT`];
  if (!host || !port) return;

  for (let attempt = 1; attempt <= 60; attempt += 1) {
    try {
      await waitForPort(host, port);
      return;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }
  throw new Error(`Source ${activeSource} did not become reachable at ${host}:${port}`);
}

async function upsertConnector(connector) {
  try {
    await request(`/connectors/${connector.name}`);
    await request(`/connectors/${connector.name}/config`, {
      method: 'PUT',
      body: JSON.stringify(connector.config),
    });
  } catch (error) {
    if (!error.message.includes('404')) throw error;
    await request('/connectors', { method: 'POST', body: JSON.stringify(connector) });
  }
}

async function deleteConnector(name) {
  try {
    await request(`/connectors/${name}`, { method: 'DELETE' });
  } catch (error) {
    if (!error.message.includes('404')) throw error;
  }
}

async function main() {
  if (!['external', 'demo'].includes(sourceMode)) {
    throw new Error(`SOURCE_MODE must be "external" or "demo", got "${sourceMode}"`);
  }

  await fs.access(sourceTemplate);
  await waitForSource();
  await waitForConnect();

  const activeConnector = await readConnector(sourceTemplate);
  const sinkConnector = await readConnector(sinkTemplate);
  const files = await fs.readdir(connectorsDir);

  for (const file of files.filter((name) => name.endsWith('-source.json'))) {
    if (file === `${activeSource}-source.json`) continue;
    await deleteConnector(await readConnectorName(`${connectorsDir}/${file}`));
  }

  await upsertConnector(activeConnector);
  await upsertConnector(sinkConnector);
  console.log(`Debezium source is ${activeSource}; connectors are ready`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
EOF
```

### 7.9 Создать Airflow DAG

Airflow DAG запускает тот же процесс регистрации connectors, но по расписанию.

Создайте файл:

```bash
cat > airflow/dags/scheduled_debezium_migration.py <<'EOF'
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

CONNECT_URL = os.getenv("CONNECT_URL", "http://debezium:8083").rstrip("/")
CONNECTORS_DIR = Path(os.getenv("CONNECTORS_DIR", "/opt/airflow/connectors"))
ACTIVE_SOURCE = os.getenv("ACTIVE_SOURCE_DB", "postgres").strip().lower()
SOURCE_MODE = os.getenv("SOURCE_MODE", "external").strip().lower()
ACTIVE_PREFIX = f"{ACTIVE_SOURCE.upper()}_SOURCE"
CRON = os.getenv("AIRFLOW_MIGRATION_CRON", "0 2 * * *")
DAG_PAUSED = os.getenv("AIRFLOW_DAG_PAUSED", "true").strip().lower() == "true"

os.environ["ACTIVE_SOURCE_TOPIC"] = os.getenv(f"{ACTIVE_PREFIX}_TOPIC", "")

def required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def render_template(path: Path) -> dict:
    text = path.read_text()
    return json.loads(re.sub(r"\$\{([A-Z0-9_]+)\}", lambda m: required_env(m.group(1)), text))

def connector_name(path: Path) -> str:
    return json.loads(path.read_text())["name"]

def connect_request(path: str, method: str = "GET", payload: dict | None = None):
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{CONNECT_URL}{path}",
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            data = response.read().decode("utf-8")
            return json.loads(data) if data else None
    except urllib.error.HTTPError as error:
        data = error.read().decode("utf-8")
        raise RuntimeError(f"{method} {path} failed with {error.code}: {data}") from error

def wait_for_connect() -> None:
    for _ in range(60):
        try:
            connect_request("/connectors")
            return
        except Exception:
            time.sleep(5)
    raise RuntimeError(f"Kafka Connect did not become ready at {CONNECT_URL}")

def upsert_connector(connector: dict) -> None:
    name = connector["name"]
    try:
        connect_request(f"/connectors/{name}")
        connect_request(f"/connectors/{name}/config", method="PUT", payload=connector["config"])
        return
    except RuntimeError as error:
        if "404" not in str(error):
            raise
    connect_request("/connectors", method="POST", payload=connector)

def delete_connector(name: str) -> None:
    try:
        connect_request(f"/connectors/{name}", method="DELETE")
    except RuntimeError as error:
        if "404" not in str(error):
            raise

@dag(
    dag_id="scheduled_debezium_migration",
    description="Register or update active Debezium source and ClickHouse sink connectors on a schedule.",
    schedule=CRON,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=DAG_PAUSED,
    tags=["agentic-data-stack", "debezium", "clickhouse"],
)
def scheduled_debezium_migration():
    @task
    def apply_active_connectors() -> dict:
        if SOURCE_MODE not in {"external", "demo"}:
            raise RuntimeError(f'SOURCE_MODE must be "external" or "demo", got "{SOURCE_MODE}"')

        source_template = CONNECTORS_DIR / f"{ACTIVE_SOURCE}-source.json"
        sink_template = CONNECTORS_DIR / "clickhouse-sink.json"

        if not source_template.exists():
            raise RuntimeError(f"Unknown ACTIVE_SOURCE_DB={ACTIVE_SOURCE}; missing {source_template}")

        wait_for_connect()

        active_connector = render_template(source_template)
        sink_connector = render_template(sink_template)

        for file in CONNECTORS_DIR.glob("*-source.json"):
            if file.name == source_template.name:
                continue
            delete_connector(connector_name(file))

        upsert_connector(active_connector)
        upsert_connector(sink_connector)

        return {
            "source_mode": SOURCE_MODE,
            "active_source_db": ACTIVE_SOURCE,
            "source_connector": active_connector["name"],
            "sink_connector": sink_connector["name"],
            "schedule": CRON,
        }

    apply_active_connectors()

scheduled_debezium_migration()
EOF
```

Критично, чтобы DAG:

- читал `ACTIVE_SOURCE_DB`;
- читал `AIRFLOW_MIGRATION_CRON`;
- рендерил templates из `/opt/airflow/connectors`;
- вызывал Debezium REST API `CONNECT_URL`.

### 7.10 Создать Минимальный Agent Proxy

```bash
cat > agent-proxy/package.json <<'EOF'
{
  "name": "agent-proxy",
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "start": "node src/server.js"
  }
}
EOF

cat > agent-proxy/Dockerfile <<'EOF'
FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY src ./src
EXPOSE 3344
CMD ["npm", "start"]
EOF

cat > agent-proxy/src/server.js <<'EOF'
import http from 'node:http';

const port = Number(process.env.PORT || 3344);
const upstreamBaseUrl = (process.env.UPSTREAM_OPENAI_BASE_URL || 'http://host.docker.internal:11434/v1').replace(/\/$/, '');
const upstreamApiKey = process.env.UPSTREAM_OPENAI_API_KEY || 'local-dev-key';

function sendJson(res, status, data) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

async function readBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {};
}

async function proxy(req, res, path) {
  const body = req.method === 'POST' ? await readBody(req) : null;
  const upstream = await fetch(`${upstreamBaseUrl}${path}`, {
    method: req.method,
    headers: { Authorization: `Bearer ${upstreamApiKey}`, 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await upstream.json();
  sendJson(res, upstream.status, data);
}

http.createServer(async (req, res) => {
  try {
    if (req.url === '/health') return sendJson(res, 200, { ok: true, upstreamBaseUrl });
    if (req.url === '/v1/models') return proxy(req, res, '/models');
    if (req.url === '/v1/chat/completions') return proxy(req, res, '/chat/completions');
    sendJson(res, 404, { error: 'Not found' });
  } catch (error) {
    sendJson(res, 500, { error: { message: error.message } });
  }
}).listen(port, '0.0.0.0');
EOF
```

### 7.11 Создать MCP Server

В текущем проекте MCP server должен публиковать не сырой SQL endpoint, а набор безопасных ClickHouse tools.

Причина простая: LibreChat должен отвечать по актуальным данным без догадок. Модель интерпретирует вопрос пользователя, выбирает tool, tool валидирует таблицу и колонки через `system.tables`/`system.columns`, выполняет нужное чтение в ClickHouse и возвращает строки модели.

Минимальный набор tools для этого проекта:

- `describe_analytics_schema`;
- `list_analytics_tables`;
- `list_non_empty_analytics_tables`;
- `describe_analytics_table`;
- `sample_analytics_table`;
- `profile_analytics_table`;
- `distinct_analytics_values`;
- `count_analytics_by`.

Дополнительно оставлены purpose-built tools для частых сценариев: Prometheus metrics, application events, route performance, model usage, error trends и Grafana visualization links.

Важно: tool вида `run_readonly_query` не нужен LibreChat. Если дать модели raw SQL tool, она начинает показывать пользователю SQL/JSON и иногда отвечает из истории вместо live-данных. В этой архитектуре SQL остается внутри MCP server, а наружу публикуются понятные операции над таблицами и колонками.

Файлы MCP server в репозитории:

```bash
mcp-server/package.json
mcp-server/Dockerfile
mcp-server/src/server.js
```

Создать минимальную рабочую версию с generic ClickHouse tools:

```bash
cat > mcp-server/package.json <<'EOF'
{
  "name": "mcp-server",
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "start": "node src/server.js"
  },
  "dependencies": {
    "@clickhouse/client": "^1.12.1"
  }
}
EOF

cat > mcp-server/Dockerfile <<'EOF'
FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY src ./src
EXPOSE 3333
CMD ["npm", "start"]
EOF

mkdir -p mcp-server/src
cat > mcp-server/src/server.js <<'EOF'
import http from 'node:http';

import { createClient } from '@clickhouse/client';

const port = Number(process.env.PORT || 3333);
const database = process.env.CLICKHOUSE_DATABASE || 'analytics';

const clickhouse = createClient({
  url: process.env.CLICKHOUSE_HOST || 'http://clickhouse:8123',
  username: process.env.CLICKHOUSE_USER || 'analytics',
  password: process.env.CLICKHOUSE_PASSWORD || 'analytics_password',
  database,
});

const tools = [
  {
    name: 'describe_analytics_schema',
    description: 'Describe ClickHouse analytics tables and views available for analysis.',
    inputSchema: { type: 'object', properties: {} },
  },
  {
    name: 'list_analytics_tables',
    description: 'List real tables and views in the ClickHouse analytics database, including row and size estimates.',
    inputSchema: {
      type: 'object',
      properties: {
        include_empty: {
          type: 'boolean',
          description: 'Include empty analytics tables.',
          default: true,
        },
      },
    },
  },
  {
    name: 'list_non_empty_analytics_tables',
    description: 'Live authoritative list of real non-empty tables in the ClickHouse analytics database.',
    inputSchema: { type: 'object', properties: {} },
  },
  {
    name: 'describe_analytics_table',
    description: 'Describe one analytics table or view by name.',
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string', description: 'Table or view name without database prefix.' },
      },
      required: ['table'],
    },
  },
  {
    name: 'sample_analytics_table',
    description: 'Return live sample rows from any analytics table or view.',
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string', description: 'Table or view name without database prefix.' },
        limit: { type: 'number', description: 'Maximum number of rows to return.', default: 10 },
      },
      required: ['table'],
    },
  },
  {
    name: 'profile_analytics_table',
    description: 'Profile one analytics table: metadata, columns, row count, and sample rows.',
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string', description: 'Table or view name without database prefix.' },
        sample_limit: { type: 'number', description: 'Maximum number of sample rows.', default: 5 },
      },
      required: ['table'],
    },
  },
  {
    name: 'distinct_analytics_values',
    description: 'Return distinct values from one column in any analytics table or view.',
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string', description: 'Table or view name without database prefix.' },
        column: { type: 'string', description: 'Column name.' },
        limit: { type: 'number', description: 'Maximum number of values to return.', default: 100 },
      },
      required: ['table', 'column'],
    },
  },
  {
    name: 'count_analytics_by',
    description: 'Count rows in any analytics table grouped by one to three columns, with optional equality and comparison filters.',
    inputSchema: {
      type: 'object',
      properties: {
        table: { type: 'string', description: 'Table or view name without database prefix.' },
        dimensions: {
          type: 'array',
          items: { type: 'string' },
          description: 'One to three column names to group by.',
        },
        filters: {
          type: 'object',
          description: 'Optional equality filters by column name.',
          additionalProperties: { type: ['string', 'number', 'boolean'] },
        },
        filter_conditions: {
          type: 'array',
          description: 'Optional comparison filters. Operators: =, !=, >, >=, <, <=.',
          items: {
            type: 'object',
            properties: {
              column: { type: 'string', description: 'Column name.' },
              operator: {
                type: 'string',
                enum: ['=', '!=', '>', '>=', '<', '<=', 'eq', 'ne', 'gt', 'gte', 'lt', 'lte'],
              },
              value: { type: ['string', 'number', 'boolean'], description: 'Comparison value.' },
            },
            required: ['column', 'operator', 'value'],
          },
        },
        limit: { type: 'number', description: 'Maximum number of grouped rows.', default: 100 },
      },
      required: ['table', 'dimensions'],
    },
  },
];

function jsonRpc(id, result) {
  return JSON.stringify({ jsonrpc: '2.0', id, result });
}

function jsonRpcError(id, code, message) {
  return JSON.stringify({ jsonrpc: '2.0', id, error: { code, message } });
}

function quoteString(value) {
  return `'${String(value).replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`;
}

function sqlLiteral(value) {
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new Error('Only finite numeric filter values are allowed.');
    return String(value);
  }
  if (typeof value === 'boolean') return value ? '1' : '0';
  return quoteString(value);
}

function normalizeFilterOperator(operator) {
  const operators = {
    '=': '=',
    eq: '=',
    '!=': '!=',
    ne: '!=',
    '>': '>',
    gt: '>',
    '>=': '>=',
    gte: '>=',
    '<': '<',
    lt: '<',
    '<=': '<=',
    lte: '<=',
  };
  const sqlOperator = operators[String(operator || '').trim().toLowerCase()];
  if (!sqlOperator) throw new Error(`Unsupported filter operator: ${operator}`);
  return sqlOperator;
}

function quoteIdent(value) {
  const text = String(value || '');
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(text)) {
    throw new Error(`Unsafe SQL identifier: ${text}`);
  }
  return `\`${text}\``;
}

function boundedLimit(value, fallback, max) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(Math.floor(parsed), max);
}

async function runQuery(query) {
  const result = await clickhouse.query({ query, format: 'JSONEachRow' });
  return result.json();
}

async function analyticsTableExists(table) {
  const tableName = String(table || '').trim();
  quoteIdent(tableName);
  const rows = await runQuery(`
    SELECT name
    FROM system.tables
    WHERE database = ${quoteString(database)}
      AND name = ${quoteString(tableName)}
    LIMIT 1
  `);
  if (rows.length === 0) {
    throw new Error(`Unknown analytics table: ${tableName}`);
  }
  return tableName;
}

async function analyticsColumns(table) {
  const tableName = await analyticsTableExists(table);
  const columns = await runQuery(`
    SELECT
      name,
      type,
      default_kind,
      default_expression
    FROM system.columns
    WHERE database = ${quoteString(database)}
      AND table = ${quoteString(tableName)}
    ORDER BY position
  `);
  return { tableName, columns };
}

async function analyticsColumnExists(table, column) {
  const { tableName, columns } = await analyticsColumns(table);
  const columnName = String(column || '').trim();
  quoteIdent(columnName);
  if (!columns.some(item => item.name === columnName)) {
    throw new Error(`Unknown analytics column: ${tableName}.${columnName}`);
  }
  return { tableName, columnName };
}

async function handleRpc(payload) {
  const { id, method, params } = payload;

  if (method === 'initialize') {
    return jsonRpc(id, {
      protocolVersion: '2024-11-05',
      capabilities: { tools: {} },
      serverInfo: { name: 'clickhouse-analytics-mcp', version: '0.1.0' },
    });
  }

  if (method === 'tools/list') {
    return jsonRpc(id, { tools });
  }

  if (method !== 'tools/call') {
    return method === 'notifications/initialized' ? '' : jsonRpcError(id, -32601, `Unknown method: ${method}`);
  }

  const name = params?.name;
  const args = params?.arguments || {};

  if (name === 'describe_analytics_schema') {
    const rows = await runQuery(`
      SELECT
        table,
        name,
        type,
        default_kind,
        default_expression
      FROM system.columns
      WHERE database = ${quoteString(database)}
      ORDER BY table, position
    `);
    return jsonRpc(id, { content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }] });
  }

  if (name === 'list_analytics_tables') {
    const includeEmpty = args.include_empty !== false;
    const rows = await runQuery(`
      SELECT
        database,
        name AS table,
        engine,
        total_rows AS rows,
        formatReadableSize(total_bytes) AS bytes
      FROM system.tables
      WHERE database = ${quoteString(database)}
        ${includeEmpty ? '' : 'AND ifNull(total_rows, 0) > 0'}
      ORDER BY database, name
    `);
    return jsonRpc(id, { content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }] });
  }

  if (name === 'list_non_empty_analytics_tables') {
    const rows = await runQuery(`
      SELECT
        database,
        name AS table,
        engine,
        total_rows AS rows,
        formatReadableSize(total_bytes) AS bytes
      FROM system.tables
      WHERE database = ${quoteString(database)}
        AND engine NOT LIKE '%View'
        AND ifNull(total_rows, 0) > 0
      ORDER BY database, name
    `);
    return jsonRpc(id, { content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }] });
  }

  if (name === 'describe_analytics_table') {
    const { tableName, columns } = await analyticsColumns(args.table);
    const metadata = await runQuery(`
      SELECT
        database,
        name AS table,
        engine,
        total_rows AS rows,
        formatReadableSize(total_bytes) AS bytes
      FROM system.tables
      WHERE database = ${quoteString(database)}
        AND name = ${quoteString(tableName)}
      LIMIT 1
    `);
    return jsonRpc(id, { content: [{ type: 'text', text: JSON.stringify({ metadata: metadata[0] || null, columns }, null, 2) }] });
  }

  if (name === 'sample_analytics_table') {
    const tableName = await analyticsTableExists(args.table);
    const limit = boundedLimit(args.limit, 10, 100);
    const rows = await runQuery(`
      SELECT *
      FROM ${quoteIdent(database)}.${quoteIdent(tableName)}
      LIMIT ${limit}
    `);
    return jsonRpc(id, { content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }] });
  }

  if (name === 'profile_analytics_table') {
    const { tableName, columns } = await analyticsColumns(args.table);
    const sampleLimit = boundedLimit(args.sample_limit, 5, 50);
    const metadata = await runQuery(`
      SELECT
        database,
        name AS table,
        engine,
        total_rows AS rows,
        formatReadableSize(total_bytes) AS bytes
      FROM system.tables
      WHERE database = ${quoteString(database)}
        AND name = ${quoteString(tableName)}
      LIMIT 1
    `);
    const sampleRows = await runQuery(`
      SELECT *
      FROM ${quoteIdent(database)}.${quoteIdent(tableName)}
      LIMIT ${sampleLimit}
    `);
    return jsonRpc(id, { content: [{ type: 'text', text: JSON.stringify({ metadata: metadata[0] || null, columns, sampleRows }, null, 2) }] });
  }

  if (name === 'distinct_analytics_values') {
    const { tableName, columnName } = await analyticsColumnExists(args.table, args.column);
    const limit = boundedLimit(args.limit, 100, 500);
    const rows = await runQuery(`
      SELECT
        ${quoteIdent(columnName)} AS value,
        count() AS rows
      FROM ${quoteIdent(database)}.${quoteIdent(tableName)}
      GROUP BY value
      ORDER BY rows DESC, value ASC
      LIMIT ${limit}
    `);
    return jsonRpc(id, { content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }] });
  }

  if (name === 'count_analytics_by') {
    const tableName = await analyticsTableExists(args.table);
    const dimensions = Array.isArray(args.dimensions) ? args.dimensions.slice(0, 3) : [];
    if (dimensions.length === 0) {
      throw new Error('count_analytics_by requires at least one dimension.');
    }
    const validatedDimensions = [];
    for (const dimension of dimensions) {
      const { columnName } = await analyticsColumnExists(tableName, dimension);
      validatedDimensions.push(columnName);
    }
    const filters = args.filters && typeof args.filters === 'object' ? args.filters : {};
    const whereParts = [];
    for (const [column, value] of Object.entries(filters)) {
      const { columnName } = await analyticsColumnExists(tableName, column);
      whereParts.push(`${quoteIdent(columnName)} = ${sqlLiteral(value)}`);
    }
    const filterConditions = Array.isArray(args.filter_conditions) ? args.filter_conditions : [];
    for (const condition of filterConditions) {
      const { columnName } = await analyticsColumnExists(tableName, condition?.column);
      const operator = normalizeFilterOperator(condition?.operator);
      whereParts.push(`${quoteIdent(columnName)} ${operator} ${sqlLiteral(condition?.value)}`);
    }
    const limit = boundedLimit(args.limit, 100, 500);
    const groupBy = validatedDimensions.map(quoteIdent).join(', ');
    const whereClause = whereParts.length > 0 ? `WHERE ${whereParts.join(' AND ')}` : '';
    const rows = await runQuery(`
      SELECT
        ${groupBy},
        count() AS rows
      FROM ${quoteIdent(database)}.${quoteIdent(tableName)}
      ${whereClause}
      GROUP BY ${groupBy}
      ORDER BY rows DESC
      LIMIT ${limit}
    `);
    return jsonRpc(id, { content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }] });
  }

  return jsonRpcError(id, -32602, `Unknown tool: ${name}`);
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  if (req.method !== 'POST' || req.url !== '/mcp') {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not found' }));
    return;
  }

  let body = '';
  req.on('data', chunk => {
    body += chunk;
  });

  req.on('end', async () => {
    try {
      const payload = JSON.parse(body || '{}');
      const response = await handleRpc(payload);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(response);
    } catch (error) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(jsonRpcError(null, -32000, error.message));
    }
  });
});

server.listen(port, '0.0.0.0', () => {
  console.log(`ClickHouse MCP server listening on ${port}`);
});
EOF
```

Проверка синтаксиса:

```bash
node --check mcp-server/src/server.js
```

Пересборка:

```bash
docker compose up -d --build mcp-server
```

### 7.12 Создать LibreChat Template

```bash
cat > librechat/librechat.yaml.template <<'EOF'
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
${LIBRECHAT_MODEL_LIST_YAML}
        fetch: false
      titleConvo: true
      titleModel: "${LIBRECHAT_TITLE_MODEL}"
      summarize: false
      summaryModel: "${LIBRECHAT_SUMMARY_MODEL}"

mcpServers:
  clickhouse-analytics:
    type: streamable-http
    url: http://mcp-server:3333/mcp
    timeout: 60000
    serverInstructions: |
      You do have access to ClickHouse through the clickhouse-analytics MCP tools, not by a direct database socket.
      By default, "ClickHouse data", "project data", and "tables" mean the analytics database only.
      If the user asks for non-empty tables, always call list_non_empty_analytics_tables live in the current turn.
      If the user asks what tables or data are available, use list_analytics_tables or describe_analytics_schema before answering.
      If the user asks what data is inside a specific table, use profile_analytics_table or sample_analytics_table.
      If the user asks for unique values, use distinct_analytics_values.
      If the user asks for counts or distributions, use count_analytics_by. For conditions like greater than, less than, at least, more than, before, after, or over a threshold, use count_analytics_by.filter_conditions with operators >, >=, <, <=, !=, or =. Use filters only for equality.
      For any ClickHouse data question, the first step must be a real MCP tool call, not a written plan. Do not answer "I will call a tool", do not print JSON with a tool name, and do not ask the user to run a tool. If a tool is available, call it.
      Do not write SQL in the answer as if the user should run it. Do not show JSON tool-call payloads to the user.
      Never invent table names, columns, rows, JSON/tool results, or Prometheus metric tables.
EOF
```

Render script:

```bash
cat > librechat/render-config.sh <<'EOF'
#!/bin/sh
set -eu

models="${LIBRECHAT_MODELS:-${OPENAI_MODEL:-qwen2.5:7b}}"
title_model="${LIBRECHAT_TITLE_MODEL:-${OPENAI_MODEL:-$(printf '%s' "$models" | cut -d, -f1)}}"
summary_model="${LIBRECHAT_SUMMARY_MODEL:-$title_model}"
model_yaml=""

old_ifs="$IFS"
IFS=','
for model in $models; do
  trimmed="$(printf '%s' "$model" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  if [ -n "$trimmed" ]; then
    model_yaml="${model_yaml}          - \"${trimmed}\"
"
  fi
done
IFS="$old_ifs"

if [ -z "$model_yaml" ]; then
  model_yaml='          - "qwen2.5:7b"
'
fi

export AGENT_PROXY_API_KEY="${AGENT_PROXY_API_KEY:-local-dev-key}"
export AGENT_PROXY_BASE_URL="${AGENT_PROXY_BASE_URL:-http://agent-proxy:3344/v1}"
export LIBRECHAT_TITLE_MODEL="$title_model"
export LIBRECHAT_SUMMARY_MODEL="$summary_model"
export LIBRECHAT_MODEL_LIST_YAML="$model_yaml"

python3 - <<'PY'
from pathlib import Path
import os

template = Path('/app/librechat.yaml.template').read_text()
for key in [
    'AGENT_PROXY_API_KEY',
    'AGENT_PROXY_BASE_URL',
    'LIBRECHAT_TITLE_MODEL',
    'LIBRECHAT_SUMMARY_MODEL',
    'LIBRECHAT_MODEL_LIST_YAML',
]:
    template = template.replace('${' + key + '}', os.environ.get(key, ''))
Path('/app/librechat.yaml').write_text(template)
PY

exec npm run backend
EOF
```

### 7.13 Добавить Модель В LibreChat

LibreChat в этом проекте не ходит напрямую в Ollama или внешний LLM API.

Цепочка такая:

```text
LibreChat
  -> agent-proxy
  -> UPSTREAM_OPENAI_BASE_URL
```

`agent-proxy` нужен, чтобы все запросы к модели проходили через одну точку и попадали в Langfuse traces.

#### Вариант A: Добавить Локальную Ollama-Модель

Сначала скачайте модель в Ollama на host-машине:

```bash
ollama pull qwen3:14b
```

Проверьте, что Ollama отдает модель через OpenAI-compatible endpoint:

```bash
curl http://localhost:11434/v1/models
```

В `.env` укажите Ollama как upstream для `agent-proxy`:

```env
UPSTREAM_OPENAI_BASE_URL=http://host.docker.internal:11434/v1
UPSTREAM_OPENAI_API_KEY=local-dev-key
```

`host.docker.internal` нужен на macOS, потому что LibreChat и `agent-proxy` работают внутри Docker, а Ollama обычно запущена на host-машине.

Добавьте точный tag модели в список LibreChat:

```env
LIBRECHAT_MODELS=qwen2.5:7b,qwen2.5:14b,qwen3:14b,llama3.2-vision:latest
OPENAI_MODEL_SMART=qwen3:14b
```

`LIBRECHAT_MODELS` управляет тем, какие модели видны в dropdown LibreChat.

`OPENAI_MODEL` удобно держать быстрым дефолтом, например `qwen2.5:7b`.

`OPENAI_MODEL_SMART` можно указывать на более сильную модель, например `qwen3:14b`.

#### Вариант B: Добавить Внешнюю OpenAI-Compatible Модель

Если модель находится во внешнем API, укажите внешний base URL и API key:

```env
UPSTREAM_OPENAI_BASE_URL=https://api.openai.com/v1
UPSTREAM_OPENAI_API_KEY=sk-change-me
LIBRECHAT_MODELS=gpt-4.1-mini,gpt-4.1
OPENAI_MODEL=gpt-4.1-mini
OPENAI_MODEL_SMART=gpt-4.1
```

Для другого провайдера используйте его OpenAI-compatible endpoint:

```env
UPSTREAM_OPENAI_BASE_URL=https://provider.example.com/v1
UPSTREAM_OPENAI_API_KEY=provider-api-key
LIBRECHAT_MODELS=provider-fast-model,provider-smart-model
OPENAI_MODEL=provider-fast-model
OPENAI_MODEL_SMART=provider-smart-model
```

Важно: имена в `LIBRECHAT_MODELS` должны совпадать с model ids, которые реально поддерживает upstream API.

После любого изменения `.env` пересоздайте LibreChat и `agent-proxy`:

```bash
docker compose up -d --force-recreate agent-proxy librechat
```

`agent-proxy` перечитает `UPSTREAM_OPENAI_BASE_URL` и `UPSTREAM_OPENAI_API_KEY`.

`render-config.sh` внутри LibreChat перечитает `LIBRECHAT_MODELS` и соберет `/app/librechat.yaml` с новым списком моделей.

Проверить, что `agent-proxy` видит upstream models:

```bash
curl http://localhost:3344/v1/models
```

Проверить, что LibreChat получил новый список:

```bash
docker compose exec -T librechat sed -n '12,30p' /app/librechat.yaml
```

### 7.14 Создать `docker-compose.yml`

Для локального all-in-one запуска создайте `docker-compose.yml` самостоятельно.

Минимально он должен содержать:

- `postgres` demo profile;
- `redpanda`;
- `clickhouse`;
- `debezium`;
- `connectors-init`;
- `airflow-db`;
- `airflow-init`;
- `airflow-webserver`;
- `airflow-scheduler`;
- `grafana`;
- `langfuse-clickhouse-init`;
- `langfuse-db`;
- `langfuse-redis`;
- `langfuse-minio`;
- `langfuse-worker`;
- `langfuse-web`;
- `librechat-db`;
- `agent-proxy`;
- `mcp-server`;
- `librechat`.

Langfuse-сервисы можно взять из текущего `docker-compose.yml` проекта как reference implementation.

Если junior DevOps собирает проект полностью с нуля, важно не забыть, что Langfuse требует не только `langfuse-web`, но и worker, Postgres, Redis, S3-compatible storage и ClickHouse database.

В production, где компоненты идут на разные машины, используйте отдельные compose-файлы из следующих разделов.

Не используйте `git clone`.

В этом сценарии все файлы создаются руками командами из документа.

После создания all-in-one compose проверить:

```bash
docker compose --env-file .env config --quiet
```

Если команда ничего не вывела, compose синтаксически корректен.

## 8. Секреты

Нельзя оставлять production secrets как `admin/admin`.

Сгенерировать секреты:

```bash
openssl rand -hex 32
```

В `.env` заменить:

```env
GRAFANA_ADMIN_PASSWORD=change-me
AIRFLOW_ADMIN_PASSWORD=change-me
AIRFLOW_WEBSERVER_SECRET_KEY=change-me
LIBRECHAT_JWT_SECRET=change-me
LIBRECHAT_JWT_REFRESH_SECRET=change-me
CLICKHOUSE_PASSWORD=change-me
LANGFUSE_NEXTAUTH_SECRET=change-me
LANGFUSE_SALT=change-me
LANGFUSE_ENCRYPTION_KEY=change-me-64-hex
LANGFUSE_INIT_USER_PASSWORD=change-me-min-8-chars
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_SECRET_KEY=sk-lf-...
```

Для `LANGFUSE_ENCRYPTION_KEY` нужен 64-character hex string.

Пример:

```bash
openssl rand -hex 32
```

Реальные пароли source-БД не коммитить.

Файл `.env` уже должен быть в `.gitignore`.

## 9. Развертывание Redpanda

В production нужен кластер из 3 broker.

На `rp-1`, `rp-2`, `rp-3` создать `docker-compose.redpanda.yml`.

Пример для `rp-1`:

```yaml
services:
  redpanda:
    image: redpandadata/redpanda:v24.2.8
    command:
      - redpanda
      - start
      - --smp=2
      - --memory=12G
      - --reserve-memory=1G
      - --node-id=0
      - --check=false
      - --kafka-addr=PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr=PLAINTEXT://rp-1.internal:9092
      - --rpc-addr=0.0.0.0:33145
      - --advertise-rpc-addr=rp-1.internal:33145
      - --seeds=rp-1.internal:33145,rp-2.internal:33145,rp-3.internal:33145
    ports:
      - "9092:9092"
      - "9644:9644"
      - "33145:33145"
    volumes:
      - redpanda_data:/var/lib/redpanda/data
volumes:
  redpanda_data:
```

Для `rp-2` поменять:

```text
--node-id=1
--advertise-kafka-addr=PLAINTEXT://rp-2.internal:9092
--advertise-rpc-addr=rp-2.internal:33145
```

Для `rp-3` поменять:

```text
--node-id=2
--advertise-kafka-addr=PLAINTEXT://rp-3.internal:9092
--advertise-rpc-addr=rp-3.internal:33145
```

Запустить на каждом broker:

```bash
docker compose -f docker-compose.redpanda.yml up -d
```

Проверить:

```bash
docker exec -it $(docker ps -qf name=redpanda) rpk cluster health
```

Ожидаем:

```text
Healthy: true
```

## 10. Развертывание ClickHouse

Для первого production-like запуска можно поднять 2 ClickHouse узла.

Для строгой HA-схемы нужен ClickHouse Keeper или ZooKeeper для replicated tables.

Минимальная схема:

- `ch-1` — первая replica;
- `ch-2` — вторая replica;
- backup на объектное хранилище или отдельный storage.

На каждом ClickHouse узле создать `docker-compose.clickhouse.yml`:

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:24.8
    environment:
      CLICKHOUSE_DB: analytics
      CLICKHOUSE_USER: analytics
      CLICKHOUSE_PASSWORD: change-me
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - ./clickhouse/init:/docker-entrypoint-initdb.d:ro
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
volumes:
  clickhouse_data:
```

Запустить:

```bash
docker compose -f docker-compose.clickhouse.yml up -d
```

Проверить:

```bash
curl 'http://localhost:8123/?user=analytics&password=change-me' \
  --data-binary 'SELECT version()'
```

Важно: схема из этого документа содержит простую `MergeTree`-таблицу для demo.

Для настоящей HA-репликации ClickHouse нужно перевести таблицы на `ReplicatedMergeTree` и настроить Keeper.

Это отдельный production hardening шаг.

## 11. Развертывание Debezium Workers

Debezium workers должны видеть:

- внешнюю source-БД;
- Redpanda brokers;
- ClickHouse HTTP endpoint;
- connector templates, созданные в разделе 7.

На `connect-1` и `connect-2` создать `.env`.

Пример важных переменных:

```env
SOURCE_MODE=external
ACTIVE_SOURCE_DB=postgres

POSTGRES_SOURCE_HOST=customer-postgres.example.com
POSTGRES_SOURCE_PORT=5432
POSTGRES_SOURCE_USER=debezium_user
POSTGRES_SOURCE_PASSWORD=change-me
POSTGRES_SOURCE_DB=customer_app
POSTGRES_SOURCE_TOPIC_PREFIX=customer_pg
POSTGRES_SOURCE_SCHEMA=public
POSTGRES_SOURCE_TABLE=app_events
POSTGRES_SOURCE_SLOT=agentic_data_stack_slot
POSTGRES_SOURCE_PUBLICATION=agentic_data_stack_publication
POSTGRES_SOURCE_SSL_MODE=require
POSTGRES_SOURCE_TOPIC=customer_pg.public.app_events

CLICKHOUSE_DB=analytics
CLICKHOUSE_USER=analytics
CLICKHOUSE_PASSWORD=change-me
CLICKHOUSE_SINK_TABLE=app_events_raw
```

Создать `docker-compose.connect.yml`:

```yaml
services:
  debezium:
    image: debezium/connect:2.7.3.Final
    environment:
      BOOTSTRAP_SERVERS: rp-1.internal:9092,rp-2.internal:9092,rp-3.internal:9092
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

  connectors-init:
    image: node:22-alpine
    env_file:
      - .env
    volumes:
      - ./debezium/connectors:/connectors:ro
      - ./debezium/register-connectors.mjs:/register-connectors.mjs:ro
    environment:
      CONNECT_URL: http://debezium:8083
    command: node /register-connectors.mjs
```

Запустить Debezium:

```bash
docker compose -f docker-compose.connect.yml up -d debezium
```

Подождать 30-60 секунд.

Проверить:

```bash
curl http://localhost:8083/
curl http://localhost:8083/connector-plugins
```

Зарегистрировать connectors:

```bash
docker compose -f docker-compose.connect.yml up connectors-init
```

Проверить:

```bash
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/postgres-app-events-source/status
curl http://localhost:8083/connectors/clickhouse-app-events-sink/status
```

Ожидаем `RUNNING` у connector и task.

## 12. Развертывание Airflow

Airflow лучше ставить отдельно.

Он не должен жить внутри Debezium worker.

На `meta-1` поднять PostgreSQL metadata DB или использовать managed PostgreSQL.

На `airflow-1` создать `.env`:

```env
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=change-me
AIRFLOW_ADMIN_EMAIL=admin@example.com
AIRFLOW_WEBSERVER_SECRET_KEY=change-me
AIRFLOW_MIGRATION_CRON=0 2 * * *
AIRFLOW_DAG_PAUSED=true

SOURCE_MODE=external
ACTIVE_SOURCE_DB=postgres
POSTGRES_SOURCE_HOST=customer-postgres.example.com
POSTGRES_SOURCE_PORT=5432
POSTGRES_SOURCE_USER=debezium_user
POSTGRES_SOURCE_PASSWORD=change-me
POSTGRES_SOURCE_DB=customer_app
POSTGRES_SOURCE_TOPIC_PREFIX=customer_pg
POSTGRES_SOURCE_SCHEMA=public
POSTGRES_SOURCE_TABLE=app_events
POSTGRES_SOURCE_SLOT=agentic_data_stack_slot
POSTGRES_SOURCE_PUBLICATION=agentic_data_stack_publication
POSTGRES_SOURCE_SSL_MODE=require
POSTGRES_SOURCE_TOPIC=customer_pg.public.app_events

CLICKHOUSE_DB=analytics
CLICKHOUSE_USER=analytics
CLICKHOUSE_PASSWORD=change-me
CLICKHOUSE_SINK_TABLE=app_events_raw
```

Создать `docker-compose.airflow.yml`:

```yaml
services:
  airflow-webserver:
    image: apache/airflow:2.10.2
    env_file:
      - .env
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:change-me@meta-1.internal:5432/airflow
      AIRFLOW__WEBSERVER__SECRET_KEY: ${AIRFLOW_WEBSERVER_SECRET_KEY}
      CONNECT_URL: http://connect-1.internal:8083
    ports:
      - "8081:8080"
    volumes:
      - ./airflow/dags:/opt/airflow/dags:ro
      - ./debezium/connectors:/opt/airflow/connectors:ro
    command: webserver

  airflow-scheduler:
    image: apache/airflow:2.10.2
    env_file:
      - .env
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:change-me@meta-1.internal:5432/airflow
      AIRFLOW__WEBSERVER__SECRET_KEY: ${AIRFLOW_WEBSERVER_SECRET_KEY}
      CONNECT_URL: http://connect-1.internal:8083
    volumes:
      - ./airflow/dags:/opt/airflow/dags:ro
      - ./debezium/connectors:/opt/airflow/connectors:ro
    command: scheduler
```

Перед первым запуском выполнить миграцию Airflow DB и создать пользователя:

```bash
docker compose -f docker-compose.airflow.yml run --rm airflow-webserver airflow db migrate
docker compose -f docker-compose.airflow.yml run --rm airflow-webserver \
  airflow users create \
  --username "$AIRFLOW_ADMIN_USER" \
  --password "$AIRFLOW_ADMIN_PASSWORD" \
  --firstname Airflow \
  --lastname Admin \
  --role Admin \
  --email "$AIRFLOW_ADMIN_EMAIL"
```

Запустить:

```bash
docker compose -f docker-compose.airflow.yml up -d
```

Открыть:

```text
http://airflow-1.internal:8081
```

Найти DAG:

```text
scheduled_debezium_migration
```

Если `AIRFLOW_DAG_PAUSED=true`, DAG будет на паузе.

Это хорошо: внешняя миграция не стартует случайно.

Чтобы включить расписание, снять toggle pause в UI.

Чтобы запустить вручную, нажать Trigger DAG.

## 13. Как Выбрать Время И День Миграции

Расписание задается переменной:

```env
AIRFLOW_MIGRATION_CRON=0 2 * * *
```

Формат:

```text
минута час день_месяца месяц день_недели
```

Примеры:

```env
# каждый день в 02:00
AIRFLOW_MIGRATION_CRON=0 2 * * *

# каждый понедельник в 03:30
AIRFLOW_MIGRATION_CRON=30 3 * * 1

# первого числа каждого месяца в 01:00
AIRFLOW_MIGRATION_CRON=0 1 1 * *

# каждые 6 часов
AIRFLOW_MIGRATION_CRON=0 */6 * * *
```

После изменения cron перезапустить scheduler:

```bash
docker compose -f docker-compose.airflow.yml up -d airflow-scheduler
```

Важно: Debezium обычно работает непрерывно.

Airflow в этой системе управляет моментом регистрации или обновления connectors.

Если бизнес-требование звучит как “читать данные только с 02:00 до 03:00”, нужно добавить отдельные Airflow tasks для pause/resume/delete connector.

## 14. Развертывание App-Слоя

На `app-1` поднимаются:

- Grafana;
- MCP server;
- agent-proxy;
- LibreChat;
- MongoDB для LibreChat metadata.

Для production лучше вынести MongoDB в replica set.

Для первого запуска можно оставить на app-машине.

Основные переменные:

```env
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=change-me

CLICKHOUSE_DB=analytics
CLICKHOUSE_USER=analytics
CLICKHOUSE_PASSWORD=change-me

AGENT_PROXY_API_KEY=local-dev-key
AGENT_PROXY_BASE_URL=http://agent-proxy:3344/v1
UPSTREAM_OPENAI_BASE_URL=http://host.docker.internal:11434/v1
UPSTREAM_OPENAI_API_KEY=local-dev-key
```

Для app-машины нужно заменить внутренние адреса ClickHouse и Grafana в compose:

```env
CLICKHOUSE_HOST=http://ch-1.internal:8123
GRAFANA_API_URL=http://grafana:3000
GRAFANA_BASE_URL=https://grafana.example.com
```

Если Grafana стоит за reverse proxy, `GRAFANA_BASE_URL` должен быть публичным URL для браузера пользователя.

## 15. Развертывание Prometheus Connector

Prometheus connector нужен для метрик Prometheus.

Не используйте Debezium для Prometheus.

Debezium читает CDC-журналы транзакционных БД.

Prometheus отдает данные через свои API, но для локального проекта оставьте пользователю две понятные команды.

### 15.1 Потоковая Загрузка В ClickHouse

Из корня проекта:

```bash
sh tools/prometheus-stream-to-clickhouse.sh
```

Команда поднимает нужные сервисы и запускает synthetic lab Prometheus с уже подготовленным `remote_write`.

Connector пишет их в ClickHouse:

```text
analytics.prometheus_samples
```

### 15.2 Пакетная Загрузка В ClickHouse

Пакетная загрузка нужна, когда нужно забрать историю.

Например, последние сутки или последнюю неделю.

Из корня проекта:

```bash
sh tools/prometheus-batch-to-clickhouse.sh
```

По умолчанию команда забирает последние 72 часа synthetic metrics. Интервал можно переопределить переменными:

```bash
PROMETHEUS_BACKFILL_START=2026-05-11T00:00:00Z \
PROMETHEUS_BACKFILL_END=2026-05-11T01:00:00Z \
sh tools/prometheus-batch-to-clickhouse.sh
```

### 15.3 Переменные Prometheus Connector

В `.env`:

```env
PROMETHEUS_BASE_URL=http://prometheus:9090
PROMETHEUS_BEARER_TOKEN=
PROMETHEUS_BASIC_USER=
PROMETHEUS_BASIC_PASSWORD=
PROMETHEUS_BACKFILL_QUERY=up
PROMETHEUS_BACKFILL_STEP=60s
PROMETHEUS_SOURCE_NAME=prometheus
PROMETHEUS_DEBUG_JSON_ENABLED=false
```

`PROMETHEUS_BASE_URL` нужен только для backfill.

Для `remote_write` Prometheus сам приходит в connector, поэтому connector не обязан ходить в Prometheus API.

`PROMETHEUS_DEBUG_JSON_ENABLED` в production лучше выключить.

Текущая реализация принимает обычные Prometheus samples из remote write.

Native histograms, exemplars и metadata можно добавить позже отдельным расширением.

### 15.4 Проверка

Healthcheck:

```bash
curl http://app-1.internal:3355/health
```

Проверить строки:

```bash
curl 'http://ch-1.internal:8123/?user=analytics&password=change-me' \
  --data-binary 'SELECT count() FROM analytics.prometheus_samples'
```

Для локального проекта вывести все таблицы:

```bash
sh tools/clickhouse-tables.sh
```

Очистить локальную analytics database:

```bash
sh tools/clickhouse-clear.sh
```

### 15.5 Анализ Через LibreChat

После загрузки метрик LibreChat может использовать MCP tools:

- `prometheus_metric_summary`;
- `prometheus_targets`;
- `sample_prometheus_metrics`;
- `prometheus_label_values`;
- `create_prometheus_availability_dashboard`;
- `create_prometheus_metric_dashboard`.

Если пользователь просит создать dashboard в Grafana по Prometheus up/down, instance health, availability, incidents, service health, DB health или HTTP health, модель должна вызвать `create_prometheus_availability_dashboard`.

Важно: Prometheus metric `up` в этом проекте показывает, жив ли scrape target `synthetic-exporter`. Это не список всех сервисов и БД. Для operational dashboard tool использует `synthetic_service_up`, `synthetic_incident_active`, HTTP latency/traffic и DB disk/lag/query метрики.

Если пользователь просит dashboard по одной конкретной Prometheus-метрике, например `synthetic_log_events_total`, модель должна вызвать `create_prometheus_metric_dashboard`.

Tool создает dashboard через Grafana API и возвращает прямой `browserUrl` вида `http://localhost:3001/d/<uid>/<slug>`.

`/goto` short URL можно считать вторичной ссылкой, потому что он зависит от настроек Grafana `root_url`.


Примеры запросов:

```text
Проанализируй Prometheus targets: какие instance сейчас down?
```

```text
Покажи последние samples метрики up и объясни, какие targets проблемные.
```

```text
Какие значения label job есть у метрики http_requests_total?
```

```text
Создай в Grafana dashboard по метрике up с разбивкой по job за последние 24 часа и дай ссылку.
```

```text
Проанализируй prometheus_samples: какие instance и когда down, какие up? Сделай Grafana dashboard.
```

```text
Создай красивый operational dashboard по Prometheus: availability, incidents, HTTP latency, HTTP errors, DB disk usage и replication lag.
```

### 15.6 Grafana Dashboard По PostgreSQL Demo Inventory

PostgreSQL demo inventory попадает в ClickHouse table:

```text
analytics.car_inventory_raw
```

Для этой таблицы LibreChat может использовать MCP tool:

- `create_car_inventory_dashboard`.

Если пользователь просит создать dashboard по складам, автомобилям, городам, брендам, пробегу, ценам или статусам машин, модель должна вызвать `create_car_inventory_dashboard`.

Tool создает Grafana dashboard через API и возвращает прямой `browserUrl` вида `http://localhost:3001/d/<uid>/<slug>`.


Примеры запросов:

```text
Создай dashboard по складам автомобилей: количество машин по городам и брендам.
```

![img_1.png](img_1.png)

```text
Создай Grafana dashboard по inventory: сколько машин с пробегом больше 20000 в каждом городе.
```
![img_3.png](img_3.png)


## 16. Развертывание Langfuse

Langfuse можно поднять рядом с app-слоем или на отдельной машине.

Для локального Docker Compose в этом проекте используются:

- `langfuse-web`;
- `langfuse-worker`;
- `langfuse-db`;
- `langfuse-redis`;
- `langfuse-minio`;
- отдельная ClickHouse database `langfuse` внутри существующего ClickHouse server.

Почему не достаточно одного контейнера Langfuse?

Потому что Langfuse — не просто UI.

Он принимает traces, кладет события в queue, обрабатывает их worker-ом, сохраняет metadata в Postgres, аналитические события в ClickHouse и файлы в S3-compatible storage.

В локальном режиме S3-compatible storage — это MinIO.

### 16.1 Переменные Langfuse

В `.env` должны быть эти значения:

```env
LANGFUSE_PUBLIC_URL=http://localhost:3002
LANGFUSE_INTERNAL_URL=http://langfuse-web:3000
LANGFUSE_ENABLED=true
LANGFUSE_ENVIRONMENT=local
LANGFUSE_TELEMETRY_ENABLED=false

LANGFUSE_POSTGRES_DB=langfuse
LANGFUSE_POSTGRES_USER=langfuse
LANGFUSE_POSTGRES_PASSWORD=change-me

LANGFUSE_REDIS_AUTH=change-me
LANGFUSE_MINIO_ROOT_USER=minio
LANGFUSE_MINIO_ROOT_PASSWORD=change-me
LANGFUSE_S3_BUCKET=langfuse
LANGFUSE_CLICKHOUSE_DB=langfuse

LANGFUSE_NEXTAUTH_SECRET=change-me
LANGFUSE_SALT=change-me
LANGFUSE_ENCRYPTION_KEY=change-me-64-hex

LANGFUSE_INIT_ORG_NAME=Agentic Data Stack
LANGFUSE_INIT_PROJECT_NAME=Agentic Data Stack LLM
LANGFUSE_INIT_USER_EMAIL=admin@example.com
LANGFUSE_INIT_USER_PASSWORD=change-me-min-8-chars
LANGFUSE_PUBLIC_KEY=pk-lf-change-me
LANGFUSE_SECRET_KEY=sk-lf-change-me
```

`LANGFUSE_PUBLIC_URL` — адрес, который открывает человек в браузере.

`LANGFUSE_INTERNAL_URL` — адрес внутри Docker network, куда `agent-proxy` отправляет traces.

`LANGFUSE_PUBLIC_KEY` и `LANGFUSE_SECRET_KEY` — API keys проекта Langfuse.

Их использует `agent-proxy`, чтобы писать traces.

`LANGFUSE_INIT_USER_PASSWORD` должен быть не короче 8 символов.

Если пароль короче, Langfuse web может стартовать, но будет отдавать `Internal Server Error`, а в logs появится сообщение:

```text
Password needs to be at least 8 characters long.
```

### 16.2 Первый Запуск Langfuse

В локальном compose запуск общий:

```bash
docker compose up -d --build
```

Проверить Langfuse:

```bash
curl http://localhost:3002/api/public/health
docker compose ps langfuse-web langfuse-worker langfuse-db langfuse-redis langfuse-minio
```

Открыть UI:

```text
http://localhost:3002
```

Войти пользователем:

```env
LANGFUSE_INIT_USER_EMAIL=admin@example.com
LANGFUSE_INIT_USER_PASSWORD=admin123456
```

В production пароль должен быть другим.

### 16.3 Регистрация И Авторизация Пользователя

В локальном режиме пользователь создается автоматически через `LANGFUSE_INIT_USER_EMAIL` и `LANGFUSE_INIT_USER_PASSWORD`.

Это удобнее, чем вручную проходить sign up после каждого пересоздания volume.

Порядок:

1. Открыть `http://localhost:3002`.
2. Ввести email из `LANGFUSE_INIT_USER_EMAIL`.
3. Ввести password из `LANGFUSE_INIT_USER_PASSWORD`.
4. Нажать Sign in.
5. Открыть organization `Agentic Data Stack`.
6. Открыть project `Agentic Data Stack LLM`.

Если автоматический пользователь не появился, проверить logs:

```bash
docker compose logs langfuse-web
docker compose logs langfuse-worker
```

Проверить Prometheus connector:

```bash
curl http://app-1.internal:3355/health
```

Проверить Prometheus samples в ClickHouse:

```bash
curl 'http://ch-1.internal:8123/?user=analytics&password=change-me' \
  --data-binary 'SELECT count() FROM analytics.prometheus_samples'
```

### 16.4 Создание Project И API Keys

Для локального запуска project создается автоматически.

Project name:

```text
Agentic Data Stack LLM
```

API keys тоже задаются через `.env`:

```env
LANGFUSE_PUBLIC_KEY=pk-lf-agentic-data-stack-local
LANGFUSE_SECRET_KEY=sk-lf-agentic-data-stack-local
```

Если нужно создать project руками:

1. Войти в Langfuse.
2. Открыть organization.
3. Нажать New Project.
4. Указать project name.
5. Открыть Project Settings.
6. Создать API keys.
7. Перенести public key и secret key в `.env`.
8. Перезапустить `agent-proxy`.

Команда перезапуска:

```bash
docker compose up -d --build agent-proxy
```

### 16.5 Как Trace Попадает В Langfuse

В этой системе трассировка сделана на уровне `agent-proxy`.

Поток такой:

```text
LibreChat
  -> agent-proxy /v1/chat/completions
  -> upstream LLM
  -> response to LibreChat

agent-proxy
  -> Langfuse /api/public/ingestion
```

Если `LANGFUSE_ENABLED=true`, `agent-proxy` отправляет trace после каждого non-streaming и streaming chat completion.

Если Langfuse временно недоступен, `agent-proxy` не ломает ответ пользователю.

Он пишет warning в logs и продолжает отдавать ответ LibreChat.

### 16.6 Ограничения Langfuse В Этом Проекте

Langfuse видит те LLM-запросы, которые проходят через `agent-proxy`.

Если какой-то сервис ходит в модель напрямую, минуя `agent-proxy`, Langfuse этот вызов не увидит.

В текущей реализации `agent-proxy` пишет в Langfuse:

- model name;
- input messages;
- output text;
- latency по start/end time;
- token usage, если upstream вернул `usage`;
- metadata с endpoint и environment.

Для streaming responses некоторые OpenAI-compatible providers не возвращают usage tokens в конце stream.

В таком случае trace будет без token usage.

Это нормально для локальных Ollama-compatible сценариев.

### 16.7 Безопасность Langfuse

Langfuse хранит prompts и outputs.

Если пользователь отправил в LibreChat персональные данные, коммерческую тайну или токены, они могут попасть в Langfuse trace.

Перед production нужно решить:

- какие поля маскировать;
- кто имеет доступ к Langfuse UI;
- сколько хранить traces;
- можно ли хранить prompts полностью;
- нужен ли VPN, reverse proxy или SSO.

Для production не оставляйте Langfuse открытым в интернет без авторизации и TLS.

## 17. Регистрация В LibreChat

Открыть:

```text
http://app-1.internal:3080/register
```

Создать первого пользователя.

Первый пользователь становится администратором LibreChat.

После регистрации:

```text
http://app-1.internal:3080/login
```

В LibreChat выбрать endpoint:

```text
Local OpenAI-compatible
```

Включить MCP tools:

```text
clickhouse-analytics
```

## 18. Проверки После Развертывания

Проверить Redpanda:

```bash
rpk cluster health
```

Проверить Debezium:

```bash
curl http://connect-1.internal:8083/connectors
```

Проверить статус source connector:

```bash
curl http://connect-1.internal:8083/connectors/postgres-app-events-source/status
```

Проверить статус ClickHouse sink:

```bash
curl http://connect-1.internal:8083/connectors/clickhouse-app-events-sink/status
```

Проверить ClickHouse:

```bash
curl 'http://ch-1.internal:8123/?user=analytics&password=change-me' \
  --data-binary 'SELECT count() FROM analytics.app_events_raw'
```

Проверить Airflow DAG:

```bash
docker compose -f docker-compose.airflow.yml exec airflow-scheduler \
  airflow dags list
```

Тестовый запуск DAG:

```bash
docker compose -f docker-compose.airflow.yml exec airflow-scheduler \
  airflow dags test scheduled_debezium_migration 2026-01-01
```

Проверить MCP:

```bash
curl http://app-1.internal:3333/health
```

Проверить agent-proxy:

```bash
curl http://app-1.internal:3344/health
```

Проверить Langfuse:

```bash
curl http://localhost:3002/api/public/health
docker compose logs agent-proxy
docker compose logs langfuse-web
docker compose logs langfuse-worker
```

## 19. Backup И Restore

Нужно делать backup:

- ClickHouse data;
- Prometheus samples в ClickHouse;
- Airflow metadata DB;
- LibreChat MongoDB;
- Langfuse Postgres;
- Langfuse ClickHouse database;
- Langfuse MinIO bucket;
- `.env` secrets;
- Grafana dashboards;
- connector configs.

ClickHouse backup зависит от production-схемы.

Минимально:

```bash
clickhouse-client --query "BACKUP DATABASE analytics TO Disk('backups', 'analytics.zip')"
```

Для серьезной эксплуатации лучше использовать object storage: S3-compatible хранилище, MinIO, AWS S3, Yandex Object Storage.

Airflow metadata DB:

```bash
pg_dump -h meta-1.internal -U airflow airflow > airflow_metadata.sql
```

MongoDB:

```bash
mongodump --uri "mongodb://mongo-1.internal:27017/LibreChat" --out ./mongo-backup
```

Langfuse Postgres:

```bash
pg_dump "postgresql://langfuse:password@lf-meta-1.internal:5432/langfuse" > langfuse-postgres.sql
```

Langfuse ClickHouse database:

```bash
clickhouse-client --host ch-1.internal --query "BACKUP DATABASE langfuse TO Disk('backups', 'langfuse.zip')"
```

Langfuse MinIO:

```bash
mc mirror minio/langfuse ./langfuse-minio-backup
```

## 20. Monitoring

Минимально мониторить:

- Redpanda health;
- consumer lag ClickHouse sink connector;
- Debezium connector status;
- ClickHouse disk usage;
- ClickHouse query latency;
- Airflow DAG failures;
- Grafana availability;
- LibreChat availability;
- Langfuse web/worker availability;
- Langfuse ingestion errors;
- Langfuse trace volume;
- Prometheus connector `/health`;
- количество Prometheus samples;
- свежесть Prometheus samples;
- MCP `/health`;
- agent-proxy `/health`.

Если connector status не `RUNNING`, сразу смотреть:

```bash
curl http://connect-1.internal:8083/connectors/<connector-name>/status
```

Потом логи:

```bash
docker logs <debezium-container>
```

## 21. Типовые Проблемы

### Debezium Не Может Подключиться К Source-БД

Проверить **host** и **port**:

```bash
nc -vz customer-postgres.example.com 5432
```

Проверить **user** и **password** обычным клиентом БД.

Проверить firewall, VPN, allowlist IP.

### PostgreSQL Connector Падает Из-За WAL

Проверить:

```sql
SHOW wal_level;
```

Должно быть:

```text
logical
```

### MySQL Connector Не Читает Изменения

Проверить:

```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';
```

Нужно:

```text
log_bin=ON
binlog_format=ROW
binlog_row_image=FULL
```

### ClickHouse Sink Пишет Не В Ту Таблицу

Проверить:

```text
CLICKHOUSE_SINK_TABLE
ACTIVE_SOURCE_TOPIC
topic2TableMap
```

Файл:

```text
debezium/connectors/clickhouse-sink.json
```

### Airflow DAG Не Запускается По Расписанию

Проверить, что DAG не на паузе.

Проверить `AIRFLOW_MIGRATION_CRON`.

Проверить scheduler:

```bash
docker compose -f docker-compose.airflow.yml logs airflow-scheduler
```

### Grafana Ссылка Открывается На Неправильном Порту

Проверить:

```env
GRAFANA_BASE_URL
```

Для браузера пользователя должен быть внешний URL.

Например:

```env
GRAFANA_BASE_URL=https://grafana.example.com
```

### Langfuse Открывается, Но Traces Не Появляются

Проверить, что включена отправка traces:

```env
LANGFUSE_ENABLED=true
LANGFUSE_INTERNAL_URL=http://langfuse-web:3000
```

Проверить logs:

```bash
docker compose logs agent-proxy
docker compose logs langfuse-web
docker compose logs langfuse-worker
```

Проверить, что в LibreChat выбран endpoint, который идет через `agent-proxy`.

Если LibreChat ходит напрямую в Ollama или другой provider, Langfuse не увидит запрос.

### Langfuse Не Стартует Из-За ClickHouse

Проверить, что ClickHouse жив:

```bash
curl 'http://localhost:8123/?user=analytics&password=analytics_password' \
  --data-binary 'SELECT 1'
```

Проверить, что database `langfuse` создана:

```bash
curl 'http://localhost:8123/?user=analytics&password=analytics_password' \
  --data-binary 'SHOW DATABASES'
```

В этом проекте database создает сервис `langfuse-clickhouse-init`.

Если volume ClickHouse старый, это нормально: init-service выполняет `CREATE DATABASE IF NOT EXISTS` при каждом запуске.

### Prometheus Connector Не Получает Samples

Проверить health:

```bash
curl http://localhost:3355/health
```

Перезапустить потоковую загрузку одной командой:

```bash
sh tools/prometheus-stream-to-clickhouse.sh
```

Если Prometheus находится на другой машине, проверьте firewall и allowlist IP до `prometheus-connector:3355`.

### Prometheus Batch Load Не Работает

Проверить `PROMETHEUS_BASE_URL`:

```env
PROMETHEUS_BASE_URL=http://prometheus:9090
```

Из контейнера connector должен открываться Prometheus HTTP API:

```bash
docker compose exec prometheus-connector wget -qO- http://prometheus:9090/api/v1/status/runtimeinfo
```

Если Prometheus защищен basic auth или bearer token, заполнить:

```env
PROMETHEUS_BEARER_TOKEN=
PROMETHEUS_BASIC_USER=
PROMETHEUS_BASIC_PASSWORD=
```

Повторить пакетную загрузку одной командой:

```bash
sh tools/prometheus-batch-to-clickhouse.sh
```

## 22. Порядок Первого Production Запуска

1. Подготовить все машины.
2. Настроить DNS или `/etc/hosts`.
3. Открыть только нужные firewall ports.
4. Установить Docker на все машины.
5. Поднять Redpanda cluster.
6. Поднять ClickHouse.
7. Проверить, что Debezium workers видят external source-БД.
8. Поднять Debezium workers.
9. Зарегистрировать connectors через `connectors-init` или Airflow DAG.
10. Проверить connector statuses.
11. Проверить строки в ClickHouse.
12. Поднять Airflow.
13. Настроить `AIRFLOW_MIGRATION_CRON`.
14. Поднять Grafana и проверить dashboard.
15. Поднять Prometheus connector.
16. Выполнить `sh tools/prometheus-stream-to-clickhouse.sh` или `sh tools/prometheus-batch-to-clickhouse.sh`.
17. Проверить строки в `analytics.prometheus_samples`.
18. Поднять MCP server и проверить `/health`.
19. Поднять LibreChat и зарегистрировать первого пользователя.
20. Включить MCP tools в LibreChat.
21. Поднять Langfuse.
22. Войти в Langfuse, проверить organization, project и API keys.
23. Задать вопрос в LibreChat и проверить trace в Langfuse.
24. Задать тестовый вопрос модели.
25. Настроить backups.
26. Настроить monitoring и alerts.

## 23. Что Обязательно Передать Следующей Смене

- Список машин и их роли.
- Все внутренние DNS names и IP.
- Какие ports открыты и почему.
- Где лежат `.env` файлы.
- Где хранятся secrets.
- Какой `ACTIVE_SOURCE_DB` сейчас включен.
- Какой `AIRFLOW_MIGRATION_CRON` сейчас активен.
- Как проверить Debezium connector status.
- Как проверить ClickHouse row count.
- Какая команда используется для потоковой загрузки Prometheus в ClickHouse.
- Какая команда используется для пакетной загрузки Prometheus в ClickHouse.
- Где смотреть Grafana dashboards.
- Как зайти в Airflow.
- Как зайти в LibreChat.
- Как зайти в Langfuse.
- Как найти traces LLM-запросов.
- Где backup и как восстановиться.
