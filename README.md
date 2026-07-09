# ADS-2 Elasticsearch Demo

Эта ветка - облегченная demo-версия ADS-2 архитектуры для анализа логов.

Здесь больше нет Kafka, Debezium, PostgreSQL как анализируемого источника, Prometheus, Airflow и adcore. Elasticsearch остается внешней базой с логами. Внутри compose поднимается только то, что нужно, чтобы показать идею: LibreChat, ClickHouse, MCP-сервисы, Grafana, Langfuse и служебные базы LibreChat/RAG/Langfuse.

## Что Это За Архитектура

Пользователь пишет вопрос в LibreChat. Kimi получает вопрос и, если нужно анализировать большой период логов, вызывает `ads-log-workflow` MCP.

`ads-log-workflow` не читает Elasticsearch напрямую. Сначала логи из внешнего Elasticsearch переносятся в ClickHouse. Затем ClickHouse сжимает логи в компактные пачки и хранит их как рабочий материал для анализа.

После этого ClickHouse сам запускает LLM-анализ пачек через `aiGenerate`: сначала map-анализ по отдельным пачкам, потом reduce-сборка общего вывода. Kimi читает результаты из ClickHouse через ClickHouse MCP и формирует человеческий ответ, SQL или Grafana-график.

Коротко:

```text
External Elasticsearch
  -> elasticsearch-connector
  -> ClickHouse raw logs
  -> compressed batches
  -> ads-log-workflow MCP
  -> ClickHouse aiGenerate map
  -> map results
  -> ClickHouse aiGenerate reduce
  -> reduce results / refined SQL
  -> Kimi reads via ClickHouse MCP
  -> answer / SQL / Grafana
```

## Что Внутри Compose

Основные сервисы:

- `librechat` - чат, куда пользователь пишет вопрос.
- `clickhouse` - аналитическая рабочая база ADS-2.
- `elasticsearch-connector` - разово переносит данные из внешнего Elasticsearch в ClickHouse.
- `mcp-log-workflow` - управляет расследованием: создает investigation, ставит пачки в очередь, запускает map/reduce.
- `mcp-clickhouse` - читает схемы, результаты и выполняет проверочные SELECT.
- `grafana` и `mcp-grafana` - строят дашборды при необходимости.
- `langfuse-*` - наблюдаемость LLM-вызовов.
- `librechat-db`, `vectordb`, `rag_api` - служебная инфраструктура LibreChat.

Служебный PostgreSQL все еще есть внутри Langfuse и pgvector/RAG. Это не анализируемая внешняя БД, а внутреннее хранилище сервисов.

## Быстрый Старт

1. Подготовьте `.env`:

```bash
cp .env.example .env
```

Минимально укажите:

```bash
KIMI_API_KEY=...
ELASTICSEARCH_PUBLIC_URL=http://localhost:9200
ELASTICSEARCH_BASE_URL=http://host.docker.internal:9200
```

`ELASTICSEARCH_PUBLIC_URL` используется shell-скриптами на вашей машине.
`ELASTICSEARCH_BASE_URL` используется контейнерами внутри Docker.
`ELASTICSEARCH_INDEX_PATTERN` использует маску Elasticsearch, например `nginx-logs-*`.
`ADS_LLM_LOG_INDEX_LIKE` использует ClickHouse `LIKE`, например `nginx-logs-%`.

2. Поднимите demo-стек:

```bash
docker compose up -d
```

3. Загрузите логи из внешнего Elasticsearch в ClickHouse:

```bash
sh tools/elasticsearch-batch-to-clickhouse.sh
```

Если в Elasticsearch еще нет тестовых логов, можно сгенерировать synthetic fixture и залить его во внешний Elasticsearch:

```bash
sh tools/elasticsearch-demo-to-clickhouse.sh
```

4. Сожмите импортированные логи:

```bash
sh tools/compress_raw_logs.sh
```

5. Откройте LibreChat:

```text
http://localhost:3080
```

Спросите, например:

```text
Проанализируй ошибки в nginx-logs за весь доступный период и найди вероятную причину деградации.
```

## Основные Таблицы ClickHouse

- Сырые импортированные логи (`analytics.es_raw_logs`).
- Сжатые пачки логов (`analytics.es_log_compressed_batches`).
- Расследования пользователя (`analytics.llm_investigations`).
- Очередь map-задач (`analytics.llm_map_queue`).
- Результаты анализа отдельных пачек (`analytics.llm_map_results`).
- Итоговые reduce-результаты и возможный refined SQL (`analytics.llm_reduce_results`).

## Важная Идея

Kimi не должен тащить в себя миллионы строк логов. Большой объем остается в ClickHouse. Модель получает только сжатые пачки, промежуточные выводы и агрегаты. Поэтому demo показывает не просто чат с SQL, а управляемый цикл:

```text
загрузить -> сжать -> проанализировать пачками -> собрать вывод -> проверить SQL/график
```

## Что Убрано В Этой Ветке

- Kafka.
- Debezium.
- PostgreSQL как внешний анализируемый источник.
- Prometheus connector и Prometheus-поток.
- Airflow.
- adcore MCP.
- Старый Airflow-путь анализа логов.

Цель ветки - не промышленная доставка данных, а понятная демонстрация ADS-2 log MapReduce поверх внешнего Elasticsearch.
