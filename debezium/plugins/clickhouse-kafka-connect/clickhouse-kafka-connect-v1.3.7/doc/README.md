# ClickHouse Kafka Connect Sink

## Что это

`clickhouse-kafka-connect` - официальный Kafka Connect sink connector для [ClickHouse](https://clickhouse.com/).

Он читает сообщения из Kafka topic и записывает их в таблицу ClickHouse.

В нашем проекте он лежит в `debezium/plugins`, потому что готовый образ `debezium/connect` умеет запускать Kafka Connect runtime и Debezium source connectors, но ClickHouse sink connector в него не встроен.

## Документация

Полная документация находится на сайте ClickHouse:

https://clickhouse.com/docs/en/integrations/kafka/clickhouse-kafka-connect-sink

## Дизайн

Подробное описание внутреннего дизайна и exactly-once delivery semantics есть в upstream design document:

`./docs/DESIGN.md`

Если этого файла нет в локальной копии plugin bundle, смотри upstream-репозиторий ClickHouse Kafka Connect.

## Помощь

Вопросы и баги по самому connector можно отправлять в upstream:

https://github.com/ClickHouse/clickhouse-kafka-connect/issues

## KeyToValue Transformation

Connector содержит transformation, которая умеет переносить Kafka message key в value.

Это полезно, если нужно сохранить key в отдельную колонку ClickHouse. По умолчанию колонка называется `_key` и имеет тип `String`.

```sql
CREATE TABLE your_table_name
(
    `your_column_name` String,
    ...
    ...
    ...
    `_key` String
) ENGINE = MergeTree()
```

Чтобы включить transformation, добавь ее в config connector:
    
```properties
transforms=keyToValue
transforms.keyToValue.type=com.clickhouse.kafka.connect.transforms.KeyToValue
transforms.keyToValue.field=_key
```

## Performance testing

В upstream-репозитории есть отдельный Gradle-проект `benchmark` для performance testing.

В этом проекте benchmark не используется: нам нужен только готовый sink connector jar из `lib/`.
