# Agentic Data Stack

Это локальный стек для аналитики данных, которые приходят из внешней БД через **Debezium** или из **Prometheus** через отдельный Prometheus connector и попадают в **ClickHouse**.

Идея простая: подключаемся к чужой source-БД, забираем изменения, складываем их в аналитическое хранилище, смотрим графики в **Grafana**, задаем вопросы данным через **LibreChat** + **MCP** и отслеживаем работу LLM через **Langfuse**.

Локальный PostgreSQL в проекте оставлен только как demo-пример. В реальной работе чаще используется внешняя БД: чужой **host**, внешний **IP**, отдельный **user**, отдельный **password**, свои правила firewall/VPN/TLS.

Для развертывания системы с нуля на нескольких машинах используйте подробный документ:

```text
docs/JUNIOR_DEVOPS_DEPLOYMENT_GUIDE.md
```

![Agentic Data Stack data flow](docs/images/guide_architecture_flow.png)

Для отдельной Prometheus demo-БД с синтетическими метриками используйте:

```text
prometheus-synthetic-lab/README.md
```

Она имитирует мониторинг 1 PostgreSQL, 2 MySQL, 2 MongoDB и 5 сервисов, включая нормальную работу и аварии.

Для отдельной Elasticsearch demo-БД с синтетическими логами используйте:

```text
elasticsearch-synthetic-lab/README.md
```

Она генерирует HTTP/application logs в индексы `nginx-logs-*`, после чего их можно перенести в ClickHouse через `elasticsearch-connector`.

## Что Делают Сервисы

| Сервис | Что Делает | Когда Смотреть |
|---|---|---|
| Debezium | читает изменения из source-БД и пишет события в Kafka | когда не появляются новые строки в ClickHouse |
| Prometheus connector | принимает `remote_write` и выполняет batch backfill в ClickHouse | когда нужны metrics history и operational dashboards |
| ClickHouse | хранит analytics tables, Prometheus samples и Langfuse events | когда нужны быстрые агрегаты, таблицы и Grafana |
| MCP server | публикует безопасные tools для LibreChat | когда модель должна отвечать по live-данным без догадок |
| Grafana | строит dashboards по ClickHouse | когда нужен человекочитаемый график или таблица |
| Langfuse | показывает traces, latency, tokens и ошибки LLM | когда нужно понять поведение модели и стоимость запросов |

**Airflow** — планировщик.

Он нужен, когда миграцию надо запускать не сразу, а в определенное **время**, **день недели** или по регулярному расписанию. В этом проекте Airflow запускает DAG `scheduled_debezium_migration`, который регистрирует или обновляет Debezium connectors.

В других проектах Airflow чаще всего используют для ETL/ELT-процессов: загрузить данные, преобразовать, проверить качество, запустить отчет, отправить уведомление.
___
**Debezium** — CDC-инструмент.

CDC означает Change Data Capture. Это способ читать изменения из БД: новые строки, обновления и удаления. Debezium читает журнал изменений source-БД и отправляет события дальше.

В других проектах Debezium часто используют для репликации данных, аудита, realtime-аналитики и синхронизации микросервисов.
___
**Prometheus connector** — сервис для переноса метрик Prometheus в ClickHouse.

Для Prometheus Debezium не подходит: Prometheus не является транзакционной БД с WAL/binlog/change stream для CDC.

В этом проекте Prometheus переносится в ClickHouse двумя командами:

- `sh tools/prometheus-stream-to-clickhouse.sh` — включает потоковую загрузку через `remote_write`;
- `sh tools/prometheus-batch-to-clickhouse.sh` — выполняет пакетную загрузку истории через Prometheus HTTP API `query_range`.

В других проектах такой connector используют, когда Prometheus хорош для scraping и alerting, а ClickHouse нужен для долгого хранения, дешевой аналитики и запросов через LLM.
___
**Kafka** — Kafka-compatible брокер сообщений.

Здесь он работает как транспорт между Debezium и ClickHouse sink connector. Debezium пишет изменения в **topic**, а ClickHouse sink читает этот **topic**.

В других проектах Kafka или Kafka обычно используют как надежную “шину событий” между сервисами.
___
**ClickHouse** — аналитическая БД.

Она хранит данные в формате, удобном для быстрых агрегатов: count, group by, latency, error rate, временные ряды.

В других проектах ClickHouse часто используют для логов, продуктовой аналитики, метрик, observability и дешевых быстрых отчетов по большим объемам данных.
___
**Grafana** — интерфейс для графиков.

Она читает данные из ClickHouse и показывает dashboards. В этом проекте MCP tools возвращают ссылки на Grafana, чтобы модель не пыталась рисовать SVG-картинки внутри LibreChat.

В других проектах Grafana обычно используют для мониторинга, алертов, метрик и операционных dashboards.
___
**LibreChat** — web UI для общения с моделью.

В этом проекте LibreChat подключен к локальной или облачной OpenAI-compatible модели через `llm-gateway`. Также LibreChat видит MCP tools и может просить их анализировать ClickHouse.

В других проектах LibreChat часто используют как единый чат-интерфейс к нескольким LLM providers.
___
**Langfuse** — observability-платформа для LLM.

**Observability** означает наблюдаемость: мы видим не только итоговый ответ модели, но и trace запроса, latency, model name, input, output, usage tokens и ошибки.

В этом проекте Langfuse получает traces от `llm-gateway`. LibreChat отправляет запрос в `llm-gateway`, `llm-gateway` вызывает локальную или облачную модель и параллельно отправляет trace в Langfuse.

В других проектах Langfuse часто используют для debugging LLM-приложений, оценки качества ответов, анализа стоимости, prompt management и поиска “почему модель ответила именно так”.
___
**MCP server** — мост между моделью и инструментами.

MCP означает Model Context Protocol. Это способ дать модели безопасные tools: посмотреть актуальную схему ClickHouse, найти непустые таблицы, показать профиль таблицы, выбрать примеры строк, посчитать уникальные значения и распределения, получить ссылку на Grafana panel.

Сырой SQL-инструмент в LibreChat намеренно не публикуется. Модель должна интерпретировать вопрос пользователя, вызвать подходящий ClickHouse MCP tool и уже по результату tool сформулировать ответ без догадок.

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

**topic** — поток сообщений в Apache Kafka, куда Debezium пишет изменения.

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

## Prometheus В ClickHouse

![Monitoring signal map](docs/images/guide_monitoring_map.png)

Prometheus подключается не через Debezium.

Если нужен готовый Prometheus с реалистичными synthetic metrics, запустите отдельное приложение:

```bash
cd prometheus-synthetic-lab
docker compose up -d --build
```

Prometheus будет доступен по адресу:

```text
http://localhost:9095
```

![img_15.png](docs/images/img_15.png)

Для подключения из Agentic-Data-Stack:

```env
PROMETHEUS_BASE_URL=http://host.docker.internal:9095
```

Для него используется сервис:

```text
prometheus-connector
```

Потоковая загрузка в ClickHouse одной командой:

```bash
sh tools/prometheus-stream-to-clickhouse.sh
```

Команда поднимает `clickhouse`, `prometheus-connector`, `mcp-server` и запускает synthetic lab Prometheus с уже подготовленным `remote_write`.

Схема потока:

```text
Prometheus remote_write
  -> prometheus-connector /api/v1/write
  -> ClickHouse analytics.prometheus_samples
  -> MCP
  -> LibreChat
```

Пакетная загрузка истории в ClickHouse одной командой:

```bash
sh tools/prometheus-batch-to-clickhouse.sh
```

По умолчанию команда забирает последние 72 часа synthetic Prometheus metrics. Интервал можно переопределить без изменения кода:

```bash
PROMETHEUS_BACKFILL_START=2026-05-11T00:00:00Z \
PROMETHEUS_BACKFILL_END=2026-05-11T01:00:00Z \
sh tools/prometheus-batch-to-clickhouse.sh
```

Схема пакетной загрузки:

```text
prometheus-connector /backfill
  -> Prometheus /api/v1/query_range
  -> ClickHouse analytics.prometheus_samples
```

Метрики пишутся в таблицу:

```text
analytics.prometheus_samples
```

Посмотреть все таблицы ClickHouse одной командой:

```bash
sh tools/clickhouse-tables.sh
```

Очистить данные в базе `analytics`, не удаляя схему, одной командой:

```bash
sh tools/clickhouse-clear.sh
```

Если нужно очистить другую database, укажите ее явно:

```bash
CLICKHOUSE_CLEAR_DATABASE=langfuse sh tools/clickhouse-clear.sh
```

Для LibreChat опубликованы только lifecycle MCP tools для generated Python-коннекторов:

- `list_generated_connectors`;
- `describe_generated_connector`;
- `create_generated_connector`;
- `update_generated_connector`;
- `run_generated_connector`.

Модель не должна отвечать на пользовательский вопрос через заранее подготовленный аналитический tool. Правильная схема такая: модель создает schema discovery connector, выполняет его, затем создает data/dashboard connector по реальной схеме ClickHouse, выполняет его и только потом формулирует короткий человеческий ответ.

Generated-коннекторы хранятся вне репозитория:

```text
/Users/subbotaevgenij/mcp-connectors/<connector_name>/connector.py
```

Если пользователь явно назвал существующий generated-коннектор, модель может переиспользовать или изменить именно его. В остальных случаях коннекторы создаются на ходу под текущий вопрос.

Важно: Prometheus metric `up` в этом проекте показывает, жив ли scrape target `synthetic-exporter`. Это не список всех сервисов и БД. Для реального operational dashboard tool использует `synthetic_service_up`, `synthetic_incident_active`, HTTP latency/traffic и DB disk/lag/query метрики. В availability-панелях служебный `synthetic-exporter:9201` скрывается, чтобы dashboard показывал реальные application/database targets.

Если пользователь просит dashboard по Prometheus или PostgreSQL demo inventory, модель также создает generated Python-коннектор. Dashboard connector после чтения ClickHouse создает Grafana dashboard и возвращает прямую ссылку вида `http://localhost:3001/d/<uid>/<slug>`.


Пример запроса в LibreChat:



```text
Создай красивый operational dashboard по Prometheus: availability, incidents, HTTP latency, HTTP errors, DB disk usage и replication lag.
```

![img_17.png](docs/images/img_17.png)


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

Он запускает локальный PostgreSQL и может наполнить его осмысленным складским набором `public.car_inventory`: машины разных марок на складах в Токио, Москве и Минске.

Наполнить ClickHouse demo-данными из PostgreSQL одной командой:

```bash
sh tools/postgres-demo-to-clickhouse.sh
```

Команда поднимает demo PostgreSQL, Kafka, Debezium и ClickHouse, регистрирует connectors, вставляет свежую пачку строк в `public.car_inventory` и ждёт, пока они появятся в `analytics.car_inventory_raw`.

По умолчанию вставляется 3000 строк. Количество можно изменить:

```bash
POSTGRES_DEMO_ROWS=5000 sh tools/postgres-demo-to-clickhouse.sh
```

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
POSTGRES_SOURCE_TABLE=car_inventory
POSTGRES_SOURCE_SLOT=car_inventory_slot
POSTGRES_SOURCE_PUBLICATION=car_inventory_publication
POSTGRES_SOURCE_SSL_MODE=disable
POSTGRES_SOURCE_TOPIC=pg_flat.public.car_inventory
```

## Как Работает Миграция

`connectors-init` запускается один раз при старте compose.

Он смотрит на `ACTIVE_SOURCE_DB`, берет нужный шаблон из `debezium/connectors/<db>-source.json`, подставляет значения из `.env` и регистрирует Debezium connector.

ClickHouse sink connector тоже создается автоматически.

Он читает **topic** активной source-БД и пишет строки в таблицу ClickHouse:

```env
CLICKHOUSE_SINK_TABLE=car_inventory_raw
```

Складской demo-набор пишет в таблицу `analytics.car_inventory_raw`.

Старые demo-логи приложения по-прежнему могут жить в `analytics.app_events_raw`, но новая PostgreSQL demo-команда использует автомобильный складской домен.

Если внешняя БД имеет другую структуру, нужно адаптировать ClickHouse schema и `CLICKHOUSE_SINK_TABLE`. LibreChat не должен хардкодить demo-набор: schema discovery generated connector каждый раз читает актуальные таблицы и колонки из ClickHouse.

### Ремарка Про ClickHouse Sink

**ClickHouse sink** в этом проекте — это отдельный Kafka Connect connector.

Он не читает внешнюю БД сам.

Он читает сообщения из **Apache Kafka topic** и записывает их в **ClickHouse**.

Вся цепочка выглядит так:

```text
External DB / demo PostgreSQL
  -> Debezium source connector
  -> Kafka topic
  -> ClickHouse sink connector
  -> ClickHouse table analytics.car_inventory_raw
```

То есть **Debezium source connector** отвечает за чтение source-БД.

**Kafka** хранит поток изменений в topic.

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
pg_flat.public.car_inventory -> analytics.car_inventory_raw
```

Поэтому фраза “текущий ClickHouse sink настроен на одну таблицу” верна, но конкретная таблица задается через `CLICKHOUSE_SINK_TABLE`.

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


## Запуск

```bash
cd Agentic-Data-Stack
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



## Airflow: Запуск Миграции По Расписанию

Airflow доступен здесь:

```text
http://localhost:8081
```

![img_13.png](docs/images/img_13.png)


Логин и пароль задаются в `.env`:

```env
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com
```

При первом запуске `airflow-init` создает локального пользователя Airflow.

![img_12.png](docs/images/img_12.png)

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
4. Нажмите переключатель, чтобы снять DAG с паузы.


Чтобы запустить миграцию вручную, нажмите кнопку Trigger DAG в Airflow UI.

![img_14.png](docs/images/img_14.png)

Если меняете `AIRFLOW_MIGRATION_CRON`, перезапустите Airflow scheduler:

```bash
docker compose up -d airflow-scheduler airflow-webserver
```

Важно: Debezium обычно работает как непрерывный CDC-процесс.

Airflow в этом проекте отвечает за момент регистрации или обновления connectors. Если нужен строгий “миграционный интервал”, например запускать в 02:00 и останавливать в 03:00, нужно добавить отдельный DAG для pause/resume или delete connectors.


## LibreChat

LibreChat доступен здесь:

```text
http://localhost:3080
```

![img.png](docs/images/img.png)

Сначала нужно зарегистрироваться:

```text
http://localhost:3080/register
```
![img_1.png](docs/images/img_1.png)

Первый зарегистрированный пользователь становится администратором LibreChat.

После регистрации откройте:

```text
http://localhost:3080/login
```

Введите email и пароль, затем нажмите "Продолжить".

Откроется LibreChat.

![img_2.png](docs/images/img_2.png)

В LibreChat в левом верхнем углу выберите `My Agents` -> `LLM Gateway`, затем выберите модель.


![img_3.png](docs/images/img_3.png)

Затем в окне чата включите `MCP Сервисы` -> `clickhouse-analytics`.


![img_4.png](docs/images/img_4.png)

Примеры запросов:

```text
Какие есть непустые таблицы в ClickHouse?
```

```text
Что содержится в таблице car_inventory_raw?
```

```text
Сходи в ClickHouse и найди все уникальные марки машин.
```

```text
Посчитай количество машин по brand и city.
```

```text
Проанализируй данные, мигрированные в ClickHouse через Debezium: какие routes самые проблемные по error rate и latency?
```

```text
Построй график количества логов по времени с разбивкой по event_type.
```

```text
Визуализируй error rate по routes и дай ссылку на Grafana.
```

![LibreChat response example](docs/images/img_9.png)

### LibreChat RAG Bulk

Для пакетной загрузки документов в RAG не нужно загружать файлы по одному через чат. Положите документы в директорию:

```text
librechat/rag_bulk/incoming
```

Airflow DAG `scheduled_librechat_rag_bulk_ingest` регулярно сканирует эту директорию и выполняет две операции:

1. вызывает штатный LibreChat RAG API (`/local/embed`), который читает файлы из общего volume, режет их на chunks, строит embeddings и сохраняет их в PostgreSQL/pgvector;
2. синхронизирует записи в MongoDB коллекцию `LibreChat.files`, чтобы проиндексированные bulk-файлы отображались в боковой панели файлов LibreChat и их можно было прикрепить к чату из UI.

Расписание задается переменной:

```env
AIRFLOW_LIBRECHAT_RAG_BULK_CRON=*/10 * * * *
LIBRECHAT_MONGO_URI=mongodb://librechat-db:27017/LibreChat
# Пустое значение означает: показывать bulk RAG файлы всем пользователям LibreChat.
LIBRECHAT_RAG_BULK_USER_EMAIL=
```

Текущая директория является источником правды: если поддерживаемый файл удалить из `incoming`, следующий запуск DAG удалит его из RAG API и из панели файлов LibreChat. Неподдерживаемые расширения не индексируются; они попадут в `unsupported` в результате Airflow task.

Поддерживаемые форматы: `csv`, `doc`, `docx`, `html`, `json`, `md`, `pdf`, `ppt`, `pptx`, `rst`, `text`, `txt`, `xls`, `xlsx`, `xml`.

После запуска DAG откройте LibreChat, нажмите иконку файлов в левой панели и убедитесь, что документ виден в таблице. Затем выберите модель `kimi-k2.6`, кликните документ в таблице файлов, чтобы прикрепить его к сообщению, и задайте вопрос по содержимому:

```text
Какая директория используется для пакетной загрузки документов в RAG?
```

```text
Какую роль выполняет LLM Gateway?
```

```text
Что делает DAG scheduled_librechat_rag_bulk_ingest?
```

## Langfuse

Langfuse доступен здесь:

```text
http://localhost:3002
```
![img_5.png](docs/images/img_5.png)

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

При первом запуске проект автоматически создает локального пользователя, organization, project и API keys.

![Langfuse organizations](docs/images/img_6.png)

![img_7.png](docs/images/img_7.png)

Чтобы увидеть tracing:

1. Откройте `http://localhost:3002`.
2. Войдите под пользователем из `LANGFUSE_INIT_USER_EMAIL`.
3. Откройте project `Agentic Data Stack LLM`.
4. В LibreChat задайте любой вопрос модели.

![img_10.png](docs/images/img_10.png)

5. Вернитесь в Langfuse и откройте раздел `Tracing`.

![img_8.png](docs/images/img_8.png)

![img_11.png](docs/images/img_11.png)
Если traces не появляются, проверьте:

```bash
curl http://localhost:3002/api/public/health
docker compose logs llm-gateway
docker compose logs langfuse-web
docker compose logs langfuse-worker
```

## Локальная Или Облачная Модель

LibreChat ходит в модель через `llm-gateway`.

`llm-gateway` также отправляет traces в Langfuse, если включено:

```env
LANGFUSE_ENABLED=true
LANGFUSE_INTERNAL_URL=http://langfuse-web:3000
```

Текущая целевая облачная модель - Kimi K2.6 через OpenAI-compatible Moonshot API:

```env
UPSTREAM_MODEL_BASE_URL=https://api.moonshot.ai/v1
UPSTREAM_MODEL_API_KEY=replace-with-kimi-api-key
MODEL=kimi-k2.6
LIBRECHAT_MODELS=kimi-k2.6
```

RAG API использует отдельную embedding-настройку. Chat-модель остается одной (`kimi-k2.6`), а embeddings строятся через локальную Ollama-модель:

```env
RAG_API_URL=http://rag_api:8000
EMBEDDINGS_PROVIDER=ollama
EMBEDDINGS_MODEL=nomic-embed-text
OLLAMA_BASE_URL=http://host.docker.internal:11434
```

После изменения `.env` пересоздайте сервисы:

```bash
docker compose up -d --force-recreate llm-gateway rag_api librechat airflow-scheduler airflow-webserver
```

Менять Docker image для этого не нужно: `librechat/render-config.sh` на старте собирает `/app/librechat.yaml` из `.env` и `librechat/librechat.yaml.template`.

В текущей конфигурации UI должен показывать одну модель: `kimi-k2.6`.

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
- `http://localhost:3355/health` — healthcheck `prometheus-connector`.
- `http://localhost:3355/api/v1/write` — Prometheus remote_write receiver.
- `http://localhost:3355/backfill` — historical backfill из Prometheus HTTP API.
- `http://localhost:3333/health` — healthcheck MCP server.
- `http://localhost:3333/mcp` — MCP endpoint. Внутри Docker LibreChat использует `http://mcp-server:3333/mcp`.
- `http://localhost:3344/health` — healthcheck `llm-gateway`.
- `http://localhost:3344/v1/models` — debug endpoint списка моделей через `llm-gateway`.
- `http://llm-gateway:3344/v1` — внутренний Docker endpoint для LibreChat.
- `http://localhost:8000/health` — healthcheck LibreChat RAG API.
- `http://localhost:8083` — Debezium Kafka Connect REST API.
- `http://localhost:8083/connectors` — список зарегистрированных Debezium connectors.
- `http://localhost:8123/play` — ClickHouse Web UI.
- `http://localhost:8123` — ClickHouse HTTP API.
- `localhost:9000` — ClickHouse native TCP port.
- `localhost:9092` — Kafka Kafka API.
- `localhost:5432` — demo PostgreSQL, только при `COMPOSE_PROFILES=postgres-source`.

Grafana внутри Docker работает на `grafana:3000`.

Пользовательские ссылки должны использовать внешний адрес `http://localhost:3001`.

## Проверка

```bash
docker compose ps
curl http://localhost:3333/health
curl http://localhost:3344/health
curl http://localhost:3002/api/public/health
curl http://localhost:3355/health
curl http://localhost:8083/connectors
```

Проверить строки в ClickHouse:

```bash
curl 'http://localhost:8123/?user=analytics&password=analytics_password' \
  --data-binary 'SELECT count() FROM analytics.car_inventory_raw'
```

Вывести все таблицы ClickHouse:

```bash
sh tools/clickhouse-tables.sh
```

После `sh tools/postgres-demo-to-clickhouse.sh` ожидается:

```text
3000 или больше
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

Текущий ClickHouse sink настроен на одну таблицу, заданную через `CLICKHOUSE_SINK_TABLE`.

Для нескольких таблиц или другой схемы данных нужно добавить новые ClickHouse tables и расширить `topic2TableMap`.

Prometheus connector хранит labels в `labels_json`.

Это гибко, потому что разные метрики имеют разные labels: `job`, `instance`, `pod`, `namespace`, `route`, `service` и так далее.

Текущая реализация принимает обычные Prometheus samples из remote write.

Native histograms, exemplars и metadata можно добавить отдельным расширением, если они понадобятся.

Для production отключите debug endpoint:

```env
PROMETHEUS_DEBUG_JSON_ENABLED=false
```
