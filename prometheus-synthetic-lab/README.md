# Prometheus Synthetic Lab

Это отдельное demo-приложение с Prometheus TSDB и synthetic exporter.

Если говорить по-человечески, это маленький “учебный продакшен”.

Он притворяется инфраструктурой компании: есть базы данных, backend-сервисы, нормальные периоды работы и несколько неприятных аварий.

Prometheus собирает с него метрики так, как собирал бы с настоящих сервисов. Потом эти данные можно перенести в ClickHouse и попросить LibreChat с моделью объяснить, что ломалось, когда ломалось и на что смотреть в первую очередь.

Оно имитирует мониторинг:

- 1 PostgreSQL;
- 2 MySQL;
- 2 MongoDB;
- 5 application services.

В данных есть нормальная работа и аварийные сценарии: падение MySQL billing, деградация payment service, рост latency, рост error rate, проблемы с MongoDB disk pressure, PostgreSQL checkpoint saturation и backlog notification service.

Важно: Prometheus хранит не “логи” в классическом смысле, а time-series metrics.

Поэтому синтетические “логи” представлены как метрики вида:

```text
synthetic_log_events_total{level="error",event="request_failed",service="payment-service"}
```

Так их удобно переносить в ClickHouse и анализировать через LibreChat.

## Как Этим Пользоваться

Самый простой сценарий такой.

Сначала вы запускаете lab:

```bash
cd prometheus-synthetic-lab
docker compose up -d --build
```

После этого открываете Prometheus:

```text
http://localhost:9095
```

![Prometheus targets](../docs/images/img_15.png)

В UI Prometheus можно сразу попробовать запрос:

```promql
synthetic_service_up
```

Он покажет, какие targets живы.

Потом попробуйте:

```promql
synthetic_incident_active
```

Эта метрика показывает, какие аварийные сценарии сейчас активны.

Если хотите посмотреть “логи” ошибок как метрики:

```promql
rate(synthetic_log_events_total{level="error"}[5m])
```

Если хотите увидеть latency:

```promql
synthetic_http_request_duration_seconds_p95
```

Если хотите увидеть проблемы БД:

```promql
synthetic_db_replication_lag_seconds
```

и:

```promql
synthetic_db_disk_usage_ratio
```

Дальше есть два пути.

Первый путь — просто дать Prometheus поработать 10-20 минут. Exporter будет отдавать живые метрики, Prometheus будет их scrape-ить, и в TSDB появятся свежие данные.

Второй путь — сразу насыпать историю за несколько дней. Это удобнее для аналитики, потому что модели будет что сравнивать: нормальные часы, деградации, падения, восстановление.

Для этого выполните:

```bash
docker compose down
sh scripts/import-history.sh
docker compose up -d --build
```

После этого Prometheus уже будет содержать исторические данные.

Теперь можно переносить их в ClickHouse через `prometheus-connector` в Agentic-Data-Stack.

Обычно для первого теста достаточно забрать такие метрики:

```text
synthetic_service_up
synthetic_incident_active
synthetic_log_events_total
synthetic_http_requests_total
synthetic_http_request_duration_seconds_p95
synthetic_db_connections
synthetic_db_query_duration_seconds_p95
synthetic_db_replication_lag_seconds
synthetic_db_disk_usage_ratio
synthetic_process_restarts_total
```

После переноса в ClickHouse открывайте LibreChat и задавайте вопросы уже обычным языком.

Например:

```text
Какие сервисы падали за последние 72 часа и по каким метрикам это видно?
```

Или:

```text
Сравни normal behavior и incident behavior для payment-service.
```

Так вы проверите всю цепочку: Prometheus -> ClickHouse -> MCP -> LibreChat -> модель.

## Состав

- `prometheus` — Prometheus TSDB на порту `9095`.
- `synthetic-exporter` — приложение, которое отдает realistic metrics на `/metrics`.
- `scripts/generate_openmetrics.py` — генератор исторических OpenMetrics samples.
- `scripts/import-history.sh` — импортирует историю в Prometheus TSDB через `promtool`.

## Быстрый Запуск

```bash
cd prometheus-synthetic-lab
docker compose up -d --build
```

Открыть Prometheus:

```text
http://localhost:9095
```

Проверить exporter:

```bash
curl http://localhost:9201/health
curl http://localhost:9201/metrics | head
```

Проверить Prometheus API:

```bash
curl 'http://localhost:9095/api/v1/query?query=up'
curl 'http://localhost:9095/api/v1/query?query=synthetic_incident_active'
```

## Наполнить Историей

Для создания исторических блоков остановите Prometheus:

```bash
docker compose down
```

Сгенерируйте 72 часа истории с шагом 60 секунд:

```bash
sh scripts/import-history.sh
```

Запустите Prometheus снова:

```bash
docker compose up -d --build
```

Проверить историю:

```bash
curl 'http://localhost:9095/api/v1/query_range?query=synthetic_service_up&start=2026-05-08T00:00:00Z&end=2026-05-11T00:00:00Z&step=1h'
```

Можно изменить объем истории:

```bash
HISTORY_HOURS=168 HISTORY_STEP_SECONDS=60 sh scripts/import-history.sh
```

`HISTORY_HOURS=168` означает 7 дней.

`HISTORY_STEP_SECONDS=60` означает 1 sample в минуту.

## Подключение К Этой Prometheus БД

Основной адрес Prometheus для браузера и host-machine:

```text
http://localhost:9095
```

Prometheus HTTP API:

```text
http://localhost:9095/api/v1
```

Query endpoint:

```text
http://localhost:9095/api/v1/query
```

Query range endpoint:

```text
http://localhost:9095/api/v1/query_range
```

Адрес из другого Docker Compose в той же Docker-сети может отличаться.

Если переносите данные из Agentic-Data-Stack, проще использовать host address:

```env
PROMETHEUS_BASE_URL=http://host.docker.internal:9095
```

Если запускаете Agentic-Data-Stack на Linux, вместо `host.docker.internal` используйте IP машины, где работает Prometheus Synthetic Lab.

## Перенос В ClickHouse Agentic-Data-Stack

В `.env` проекта Agentic-Data-Stack укажите:

```env
PROMETHEUS_BASE_URL=http://host.docker.internal:9095
PROMETHEUS_BACKFILL_STEP=60s
PROMETHEUS_SOURCE_NAME=synthetic-prometheus-lab
```

Запустите потоковую загрузку в ClickHouse из корня Agentic-Data-Stack:

```bash
sh tools/prometheus-stream-to-clickhouse.sh
```

Или выполните пакетную загрузку истории в ClickHouse:

```bash
sh tools/prometheus-batch-to-clickhouse.sh
```

Проверить в ClickHouse Agentic-Data-Stack:

```bash
sh tools/clickhouse-tables.sh
```

Для быстрой очистки ClickHouse analytics database:

```bash
sh tools/clickhouse-clear.sh
```

## Что Спросить В LibreChat

После переноса в ClickHouse можно спрашивать:

```text
Проанализируй Prometheus targets: какие instance сейчас down?
```

![LibreChat Prometheus answer](../docs/images/img_16.png)

```text
Найди интервалы, где payment-service деградировал: сравни latency, 5xx и synthetic_log_events_total.
```

```text
Какие БД выглядят проблемными по replication lag, disk usage и p95 query latency?
```

```text
Покажи, какие сервисы работали нормально, а какие падали или деградировали.
```

## Основные Метрики

- `synthetic_service_up` — аналог `up`, показывает доступность target.
- `synthetic_incident_active` — активный incident по сервису.
- `synthetic_log_events_total` — synthetic log events по `level` и `event`.
- `synthetic_http_requests_total` — HTTP requests по service/route/status_class.
- `synthetic_http_request_duration_seconds_p95` — p95 latency сервиса.
- `synthetic_db_connections` — активные DB connections.
- `synthetic_db_query_duration_seconds_p95` — p95 query latency.
- `synthetic_db_replication_lag_seconds` — replication lag.
- `synthetic_db_disk_usage_ratio` — disk usage ratio.
- `synthetic_process_restarts_total` — process/container restarts.

## Сценарии Аварий

- `payment_gateway_degradation` — payment-service: высокий error rate и latency.
- `mysql_billing_crash_loop` — MySQL billing: target становится down, растут errors/restarts.
- `mongodb_events_disk_pressure` — MongoDB events: disk usage и query latency растут.
- `postgres_checkpoint_saturation` — PostgreSQL: query latency и replication lag растут.
- `notification_queue_backlog` — notification-service: latency, warnings и retries растут.
