# Elasticsearch Synthetic Lab

Это демонстрационный набор журналов Elasticsearch для проверки цепочки:

```text
Elasticsearch -> elasticsearch-connector -> ClickHouse -> MCP -> LibreChat -> Grafana
```

В репозитории уже лежит небольшой статический образец:

```text
elasticsearch-synthetic-lab/fixtures/synthetic-logs.bulk.ndjson
elasticsearch-synthetic-lab/fixtures/synthetic-logs.meta.json
```

Он не генерируется заново при каждом запуске. Это намеренно: быстрые тесты должны быть воспроизводимыми.

Для полного аналитического прогона генератор создаёт настраиваемый набор. Текущие рекомендуемые параметры дают 1 ГиБ исходных журналов пяти служб за период с 2021 по 2026 год. В набор входят рост нагрузки, суточная и сезонная динамика, фоновые предупреждения, причинно связанные инциденты, восстановление после сбоев, пропуски доставки, неполные поля и редкие дубли.

Службы, маршруты, инциденты, разрывы данных и коэффициенты генерации находятся в `config/log_scenarios.json`. Другой профиль можно передать без изменения Python-кода:

```bash
ELASTICSEARCH_DEMO_SCENARIO_FILE=/path/to/scenario.json \
  sh tools/elasticsearch-demo-to-clickhouse.sh
```

Fixture содержит 720 документов за фиксированный интервал:

```text
2026-05-21T12:00:00Z .. 2026-05-24T12:00:00Z
```

Индексы:

```text
nginx-logs-2026.05.21
nginx-logs-2026.05.22
nginx-logs-2026.05.23
nginx-logs-2026.05.24
```

Документы похожи на HTTP/application logs для нескольких сервисов:

- `api-gateway`;
- `checkout-service`;
- `inventory-service`;
- `payment-service`;
- `notification-service`.

Поля документа:

- `@timestamp` — время события;
- `service`, `host`, `environment` — источник события;
- `level`, `message`, `error_code` — уровень и смысл события;
- `http.method`, `http.path`, `http.status_code`, `http.latency_ms`, `http.user_agent` — HTTP-контекст;
- `geo.city` — город пользователя;
- `trace_id`, `user_id` — поля для трассировки;
- `incident` — название synthetic incident;
- `labels.team`, `labels.source` — дополнительные labels.

В данных есть нормальный трафик и осмысленные synthetic incidents:

- рост latency и 503 у `payment-service`;
- lock contention у `inventory-service`;
- queue backlog у `notification-service`;
- редкие случайные 500 errors.

Эти сценарии нужны, чтобы модель могла отвечать не только на “сколько документов”, но и на вопросы вроде:

```text
Какие сервисы чаще всего ошибались?
```

```text
Когда был всплеск latency у payment-service?
```

```text
Построй график ошибок по сервисам.
```

## Быстрый Запуск

Из корня проекта:

```bash
sh tools/elasticsearch-demo-to-clickhouse.sh
```

Команда:

1. Поднимает локальный Elasticsearch из `docker-compose.yml` profile `elasticsearch`.
2. Создаёт набор по JSON-профилю или берёт указанный готовый файл.
3. Загружает документы в индексы `nginx-logs-*`.
4. Запускает `elasticsearch-connector` batch migration.
5. Печатает сводку из `analytics.elasticsearch_events_raw`.

## Настройки

```bash
ELASTICSEARCH_DEMO_CLEAR=false sh tools/elasticsearch-demo-to-clickhouse.sh
ELASTICSEARCH_DEMO_BULK_FILE=/path/to/custom.bulk.ndjson sh tools/elasticsearch-demo-to-clickhouse.sh
ELASTICSEARCH_DEMO_META_FILE=/path/to/custom.meta.json sh tools/elasticsearch-demo-to-clickhouse.sh
```

По умолчанию старые demo-документы с тем же `source_name` и `index_prefix` удаляются из ClickHouse перед новой загрузкой, чтобы результат команды был предсказуемым.

## Ручное Пересоздание Fixture

Обычно это не нужно.

Если fixture нужно явно пересоздать:

```bash
ELASTICSEARCH_DEMO_REGENERATE=true sh tools/elasticsearch-demo-to-clickhouse.sh
```

Полный рекомендуемый прогон использует значения из `.env.example`: 1 ГиБ, период 2021–2026, месячные индексы и профиль `config/log_scenarios.json`. После загрузки существующий `elasticsearch-connector` переносит тот же диапазон в ClickHouse.

Либо вручную:

```bash
OUTPUT=elasticsearch-synthetic-lab/fixtures/synthetic-logs.bulk.ndjson \
  ELASTICSEARCH_DEMO_DOCS=720 \
  ELASTICSEARCH_DEMO_HOURS=72 \
  ELASTICSEARCH_DEMO_INDEX_PREFIX=nginx-logs \
  python3 elasticsearch-synthetic-lab/scripts/generate_bulk.py \
  > elasticsearch-synthetic-lab/fixtures/synthetic-logs.meta.json
```

После пересоздания fixture лучше отдельно проверить diff, потому что это меняет тестовые данные.
