# Debezium/Kafka Connect connector templates

Эта директория содержит JSON-шаблоны Kafka Connect connectors.

Обычные комментарии в `.json` вставлять нельзя: Kafka Connect читает эти файлы как строгий JSON. Поэтому пояснения лежат здесь, рядом с шаблонами.

## Общий поток данных

1. Source connector читает изменения из исходной БД.
2. Debezium/Kafka Connect публикует изменения в Kafka topic.
3. Sink connector читает Kafka topic.
4. ClickHouse sink connector пишет строки в таблицу ClickHouse.

Схема:

```text
PostgreSQL/MySQL/MongoDB -> source connector -> Kafka -> clickhouse-sink.json -> ClickHouse
```

## `postgres-source.json`

Source connector для PostgreSQL.

Важные поля:

- `connector.class`: использует стандартный Debezium PostgreSQL connector.
- `plugin.name=pgoutput`: PostgreSQL logical replication plugin.
- `database.*`: host, port, user, password и имя БД источника.
- `topic.prefix`: префикс Kafka topic'ов, куда Debezium пишет изменения.
- `schema.include.list` и `table.include.list`: ограничивают чтение нужными таблицами. Для одной таблицы можно оставить старую пару `POSTGRES_SOURCE_SCHEMA` + `POSTGRES_SOURCE_TABLE`; для группы таблиц задайте `POSTGRES_SOURCE_TABLE_INCLUDE_LIST=public.orders,public.payments,public.customers`.
- `slot.name`: replication slot PostgreSQL, через который Debezium читает WAL.
- `publication.name`: publication PostgreSQL для logical replication.
- `publication.autocreate.mode=filtered`: Debezium сам создает publication только для выбранных таблиц.
- `snapshot.mode=initial`: при первом запуске connector сначала снимает текущий снимок таблицы, потом читает изменения.
- `transforms.unwrap.*`: убирает Debezium envelope и оставляет плоскую строку, удобную для ClickHouse sink.

## `mysql-source.json`

Source connector для MySQL.

Для одной таблицы можно оставить старую переменную `MYSQL_SOURCE_TABLE`.
Для группы таблиц задайте явный список:

```env
MYSQL_SOURCE_TABLE_INCLUDE_LIST=customer_app.orders,customer_app.payments,customer_app.customers
CLICKHOUSE_SINK_TOPICS=customer_mysql.customer_app.orders,customer_mysql.customer_app.payments,customer_mysql.customer_app.customers
CLICKHOUSE_TOPIC_TABLE_MAP=customer_mysql.customer_app.orders=orders_raw,customer_mysql.customer_app.payments=payments_raw,customer_mysql.customer_app.customers=customers_raw
```

Важные поля:

- `connector.class`: стандартный Debezium MySQL connector.
- `database.server.id`: уникальный id клиента репликации MySQL.
- `database.include.list` и `table.include.list`: выбирают БД и таблицу.
- `snapshot.mode=initial`: сначала загружает текущие строки, потом читает binlog.
- `transforms.unwrap.*`: превращает Debezium event в плоскую строку.

## `mongodb-source.json`

Source connector для MongoDB.

Важные поля:

- `connector.class`: стандартный Debezium MongoDB connector.
- `mongodb.connection.string`: строка подключения к MongoDB.
- `topic.prefix`: префикс Kafka topic'ов.
- `database.include.list` и `collection.include.list`: выбирают БД и коллекцию.
- `capture.mode=change_streams_update_full`: читает change streams и старается получать полный документ при update.

## `clickhouse-sink.json`

Sink connector для ClickHouse.

Именно для него подключен plugin в `debezium/plugins`: в образе `debezium/connect` есть Debezium source connectors, но нет официального ClickHouse sink connector.

Важные поля:

- `connector.class`: официальный ClickHouse Kafka Connect Sink connector.
- `topics`: Kafka topics, которые нужно читать. Значение подставляется через `${CLICKHOUSE_SINK_TOPICS}`.
- `topic2TableMap`: соответствие Kafka topic -> ClickHouse table. Значение подставляется через `${CLICKHOUSE_TOPIC_TABLE_MAP}`.

Пример для группы таблиц:

```env
POSTGRES_SOURCE_TABLE_INCLUDE_LIST=public.orders,public.payments,public.customers
CLICKHOUSE_SINK_TOPICS=customer_pg.public.orders,customer_pg.public.payments,customer_pg.public.customers
CLICKHOUSE_TOPIC_TABLE_MAP=customer_pg.public.orders=orders_raw,customer_pg.public.payments=payments_raw,customer_pg.public.customers=customers_raw
```
- `hostname`, `port`, `database`, `username`, `password`: подключение к ClickHouse.
- `topic2TableMap`: соответствие Kafka topic -> ClickHouse table.
- `schemas.enable=false`: сообщения идут как JSON без Kafka schema envelope.
- `exactlyOnce=false`: для локального demo упрощаем delivery semantics.
- `value.converter` и `key.converter`: JSON converter без schemas.

## Почему шаблоны, а не готовые JSON

Файлы содержат placeholders `${...}`. Их подставляет `debezium/register_connectors.py` или Airflow DAG перед отправкой в Kafka Connect REST API.

Так один и тот же код может работать с demo PostgreSQL, внешним PostgreSQL, MySQL или MongoDB: меняются `.env` и активный source template, а схема регистрации остается одна.
