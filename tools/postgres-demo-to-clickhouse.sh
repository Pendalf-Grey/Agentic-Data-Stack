#!/bin/sh
set -eu

# Скрипт поднимает demo PostgreSQL -> Debezium/Kafka -> ClickHouse pipeline
# и загружает осмысленные demo-данные по складам автомобилей.

# Корень репозитория нужен, чтобы docker compose и относительные пути работали стабильно.
ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

# Количество строк автомобилей, которое будет вставлено в PostgreSQL и затем доедет до ClickHouse.
POSTGRES_DEMO_ROWS=${POSTGRES_DEMO_ROWS:-3000}

# Сколько секунд ждать, пока Debezium и ClickHouse sink перенесут строки.
WAIT_SECONDS=${POSTGRES_DEMO_WAIT_SECONDS:-180}

# Проверяем, что POSTGRES_DEMO_ROWS - число, чтобы не подставить мусор в SQL.
case "$POSTGRES_DEMO_ROWS" in
  ''|*[!0-9]*)
    echo "POSTGRES_DEMO_ROWS must be a positive integer, got: $POSTGRES_DEMO_ROWS" >&2
    exit 1
    ;;
esac

# Проверяем timeout ожидания загрузки.
case "$WAIT_SECONDS" in
  ''|*[!0-9]*)
    echo "POSTGRES_DEMO_WAIT_SECONDS must be a positive integer, got: $WAIT_SECONDS" >&2
    exit 1
    ;;
esac

# Для этого сценария принудительно включаем demo-режим и локальный PostgreSQL profile.
export SOURCE_MODE=demo
export ACTIVE_SOURCE_DB=postgres
export COMPOSE_PROFILES=postgres-source

export POSTGRES_DB=${POSTGRES_DB:-app_logs}
export POSTGRES_USER=${POSTGRES_USER:-app}
export POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-app_password}

# Эти переменные использует postgres-source.json при регистрации Debezium source connector.
export POSTGRES_SOURCE_HOST=postgres
export POSTGRES_SOURCE_PORT=5432
export POSTGRES_SOURCE_USER=${POSTGRES_SOURCE_USER:-$POSTGRES_USER}
export POSTGRES_SOURCE_PASSWORD=${POSTGRES_SOURCE_PASSWORD:-$POSTGRES_PASSWORD}
export POSTGRES_SOURCE_DB=${POSTGRES_SOURCE_DB:-$POSTGRES_DB}
export POSTGRES_SOURCE_TOPIC_PREFIX=${POSTGRES_SOURCE_TOPIC_PREFIX:-pg_flat}
export POSTGRES_SOURCE_SCHEMA=${POSTGRES_SOURCE_SCHEMA:-public}
export POSTGRES_SOURCE_TABLE=car_inventory
export POSTGRES_SOURCE_SLOT=${POSTGRES_SOURCE_SLOT:-car_inventory_slot}
export POSTGRES_SOURCE_PUBLICATION=${POSTGRES_SOURCE_PUBLICATION:-car_inventory_publication}
export POSTGRES_SOURCE_SSL_MODE=disable
export POSTGRES_SOURCE_TOPIC=${POSTGRES_SOURCE_TOPIC:-pg_flat.public.car_inventory}

# Эти переменные использует clickhouse-sink.json.
export CLICKHOUSE_DB=${CLICKHOUSE_DB:-analytics}
export CLICKHOUSE_USER=${CLICKHOUSE_USER:-analytics}
export CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-analytics_password}
export CLICKHOUSE_SINK_TABLE=car_inventory_raw

cd "$ROOT_DIR"

# Поднимаем минимальный стек для CDC: source DB, Kafka, ClickHouse и Kafka Connect runtime.
docker compose up -d --build postgres kafka clickhouse debezium

# Одноразовый контейнер регистрирует source/sink connectors в Kafka Connect REST API.
docker compose run --rm --build \
  -e SOURCE_MODE \
  -e ACTIVE_SOURCE_DB \
  -e POSTGRES_SOURCE_HOST \
  -e POSTGRES_SOURCE_PORT \
  -e POSTGRES_SOURCE_USER \
  -e POSTGRES_SOURCE_PASSWORD \
  -e POSTGRES_SOURCE_DB \
  -e POSTGRES_SOURCE_TOPIC_PREFIX \
  -e POSTGRES_SOURCE_SCHEMA \
  -e POSTGRES_SOURCE_TABLE \
  -e POSTGRES_SOURCE_SLOT \
  -e POSTGRES_SOURCE_PUBLICATION \
  -e POSTGRES_SOURCE_SSL_MODE \
  -e POSTGRES_SOURCE_TOPIC \
  -e CLICKHOUSE_DB \
  -e CLICKHOUSE_USER \
  -e CLICKHOUSE_PASSWORD \
  -e CLICKHOUSE_SINK_TABLE \
  connectors-init

# На всякий случай создаем целевую таблицу в ClickHouse.
# Это делает команду самодостаточной даже после очистки/пересоздания volume.
docker compose exec -T clickhouse clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "
    CREATE TABLE IF NOT EXISTS \`$CLICKHOUSE_DB\`.\`$CLICKHOUSE_SINK_TABLE\`
    (
      id UInt64,
      batch_id String,
      inventory_time String,
      city LowCardinality(String),
      warehouse_name LowCardinality(String),
      brand LowCardinality(String),
      model String,
      model_year UInt16,
      body_type LowCardinality(String),
      color LowCardinality(String),
      vin String,
      stock_status LowCardinality(String),
      price_usd Decimal(12, 2),
      mileage_km UInt32,
      arrived_at String,
      metadata String,
      ingest_time DateTime DEFAULT now()
    )
    ENGINE = MergeTree
    ORDER BY (city, brand, model, id)
  "

# View для быстрых агрегатов по складам автомобилей.
docker compose exec -T clickhouse clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "
    CREATE VIEW IF NOT EXISTS \`$CLICKHOUSE_DB\`.v_car_inventory_summary AS
    SELECT
      city,
      warehouse_name,
      brand,
      count() AS cars,
      countIf(stock_status = 'available') AS available_cars,
      countIf(stock_status = 'reserved') AS reserved_cars,
      countIf(stock_status = 'maintenance') AS maintenance_cars,
      round(avg(price_usd), 2) AS avg_price_usd,
      min(model_year) AS oldest_model_year,
      max(model_year) AS newest_model_year
    FROM \`$CLICKHOUSE_DB\`.\`$CLICKHOUSE_SINK_TABLE\`
    GROUP BY city, warehouse_name, brand
    ORDER BY city ASC, cars DESC, brand ASC
  "

# Очищаем только текущую целевую таблицу, чтобы результат команды был предсказуемым.
docker compose exec -T clickhouse clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "TRUNCATE TABLE \`$CLICKHOUSE_DB\`.\`$CLICKHOUSE_SINK_TABLE\`"

# batch_id нужен, чтобы отличать текущую загрузку от старых строк, если они где-то остались.
batch_id="car-demo-$(date -u '+%Y%m%d%H%M%S')"

# Создаем/обновляем demo-таблицу в PostgreSQL и вставляем складские данные.
# Дальше Debezium сам увидит INSERT'ы и отправит изменения через Kafka в ClickHouse.
docker compose exec -T postgres psql \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -v ON_ERROR_STOP=1 \
  -c "
    CREATE EXTENSION IF NOT EXISTS pgcrypto;

    CREATE TABLE IF NOT EXISTS car_inventory (
      id BIGSERIAL PRIMARY KEY,
      batch_id TEXT NOT NULL,
      inventory_time TIMESTAMPTZ NOT NULL DEFAULT now(),
      city TEXT NOT NULL,
      warehouse_name TEXT NOT NULL,
      brand TEXT NOT NULL,
      model TEXT NOT NULL,
      model_year INTEGER NOT NULL,
      body_type TEXT NOT NULL,
      color TEXT NOT NULL,
      vin TEXT NOT NULL UNIQUE,
      stock_status TEXT NOT NULL,
      price_usd NUMERIC(12, 2) NOT NULL,
      mileage_km INTEGER NOT NULL,
      arrived_at TIMESTAMPTZ NOT NULL,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb
    );

    CREATE INDEX IF NOT EXISTS idx_car_inventory_city ON car_inventory(city);
    CREATE INDEX IF NOT EXISTS idx_car_inventory_brand ON car_inventory(brand);
    CREATE INDEX IF NOT EXISTS idx_car_inventory_status ON car_inventory(stock_status);
    CREATE INDEX IF NOT EXISTS idx_car_inventory_batch_id ON car_inventory(batch_id);
    ALTER TABLE car_inventory REPLICA IDENTITY FULL;

    WITH city_data AS (
      -- Три города и коэффициенты спроса, чтобы цены отличались по рынкам.
      SELECT *
      FROM (VALUES
        ('Tokyo', 'Tokyo Bay Auto Hub', 'JPY', 1.12),
        ('Moscow', 'Moscow North Vehicle Depot', 'RUB', 0.91),
        ('Minsk', 'Minsk Central Car Storage', 'BYN', 0.78)
      ) AS t(city, warehouse_name, local_currency, demand_multiplier)
    ),
    model_data AS (
      -- Каталог брендов/моделей. Это делает demo-данные похожими на реальную инвентаризацию.
      SELECT *
      FROM (VALUES
        ('Toyota', 'Corolla', 'sedan', 22100),
        ('Toyota', 'RAV4', 'suv', 31900),
        ('Toyota', 'Camry', 'sedan', 28700),
        ('Nissan', 'Qashqai', 'suv', 27600),
        ('Nissan', 'Leaf', 'hatchback', 29900),
        ('Honda', 'Civic', 'sedan', 25400),
        ('Honda', 'CR-V', 'suv', 33600),
        ('Mazda', 'CX-5', 'suv', 30400),
        ('Mazda', 'Mazda3', 'hatchback', 24600),
        ('BMW', 'X5', 'suv', 66900),
        ('BMW', '320i', 'sedan', 45900),
        ('Mercedes-Benz', 'C-Class', 'sedan', 48700),
        ('Mercedes-Benz', 'GLC', 'suv', 57900),
        ('Audi', 'A4', 'sedan', 43800),
        ('Audi', 'Q5', 'suv', 53200),
        ('Volkswagen', 'Tiguan', 'suv', 34900),
        ('Volkswagen', 'Golf', 'hatchback', 29200),
        ('Kia', 'Sportage', 'suv', 30900),
        ('Hyundai', 'Tucson', 'suv', 31700),
        ('Geely', 'Monjaro', 'suv', 36200)
      ) AS t(brand, model, body_type, base_price_usd)
    ),
    colors AS (
      -- Набор цветов равномерно распределяется по generated rows.
      SELECT *
      FROM (VALUES
        ('White'), ('Black'), ('Silver'), ('Blue'), ('Red'), ('Graphite')
      ) AS t(color)
    ),
    generated AS (
      -- generated связывает города, модели и цвета с generate_series.
      SELECT
        gs,
        c.city,
        c.warehouse_name,
        c.local_currency,
        c.demand_multiplier,
        m.brand,
        m.model,
        m.body_type,
        m.base_price_usd,
        color,
        2020 + (gs % 6) AS model_year,
        CASE
          WHEN gs % 19 = 0 THEN 'maintenance'
          WHEN gs % 7 = 0 THEN 'reserved'
          ELSE 'available'
        END AS stock_status
      FROM generate_series(1, $POSTGRES_DEMO_ROWS::int) AS gs
      JOIN city_data c ON ((gs - 1) % 3) + 1 = CASE c.city WHEN 'Tokyo' THEN 1 WHEN 'Moscow' THEN 2 ELSE 3 END
      JOIN model_data m ON ((gs - 1) % 20) + 1 = (
        SELECT row_number
        FROM (
          SELECT brand, model, row_number() OVER () AS row_number
          FROM model_data
        ) numbered
        WHERE numbered.brand = m.brand AND numbered.model = m.model
      )
      JOIN colors cl ON ((gs - 1) % 6) + 1 = (
        SELECT row_number
        FROM (
          SELECT color, row_number() OVER () AS row_number
          FROM colors
        ) numbered
        WHERE numbered.color = cl.color
      )
    )
    INSERT INTO car_inventory (
      batch_id,
      inventory_time,
      city,
      warehouse_name,
      brand,
      model,
      model_year,
      body_type,
      color,
      vin,
      stock_status,
      price_usd,
      mileage_km,
      arrived_at,
      metadata
    )
    SELECT
      '$batch_id',
      now() - (gs || ' minutes')::interval,
      city,
      warehouse_name,
      brand,
      model,
      model_year,
      body_type,
      color,
      upper(substr(md5('$batch_id' || '-' || gs), 1, 17)) AS vin,
      stock_status,
      round((base_price_usd * demand_multiplier * (1 + ((model_year - 2020) * 0.035)))::numeric, 2) AS price_usd,
      CASE WHEN model_year >= 2024 THEN (gs % 9000) ELSE 12000 + (gs * 37 % 95000) END AS mileage_km,
      now() - ((gs % 120) || ' days')::interval,
      jsonb_build_object(
        'source', 'postgres-demo-to-clickhouse',
        'batch_id', '$batch_id',
        'local_currency', local_currency,
        'inspection_passed', gs % 13 != 0,
        'storage_zone', chr(65 + (gs % 5)),
        'city', city
      )
    FROM generated;
  "

# Ждем, пока ClickHouse sink connector перенесет все строки текущего batch_id.
for attempt in $(seq 1 "$WAIT_SECONDS"); do
  count=$(docker compose exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --query "SELECT count() FROM \`$CLICKHOUSE_DB\`.\`$CLICKHOUSE_SINK_TABLE\` WHERE batch_id = '$batch_id'")
  if [ "$count" -ge "$POSTGRES_DEMO_ROWS" ]; then
    echo "Loaded $count car inventory rows into $CLICKHOUSE_DB.$CLICKHOUSE_SINK_TABLE"
    # Печатаем короткую сводку, чтобы пользователь сразу видел, что данные осмысленные.
    docker compose exec -T clickhouse clickhouse-client \
      --user "$CLICKHOUSE_USER" \
      --password "$CLICKHOUSE_PASSWORD" \
      --query "
        SELECT city, brand, count() AS cars, countIf(stock_status = 'available') AS available
        FROM \`$CLICKHOUSE_DB\`.\`$CLICKHOUSE_SINK_TABLE\`
        WHERE batch_id = '$batch_id'
        GROUP BY city, brand
        ORDER BY city, cars DESC, brand
        LIMIT 20
        FORMAT PrettyCompact
      "
    exit 0
  fi

  sleep 1
done

echo "Timed out waiting for car inventory rows in ClickHouse. Last count: $count / $POSTGRES_DEMO_ROWS" >&2
echo "Check connector status with: curl http://localhost:8083/connectors/clickhouse-app-events-sink/status" >&2
exit 1
