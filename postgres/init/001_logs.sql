-- Этот файл выполняется официальным Docker entrypoint контейнера postgres
-- при первом создании volume postgres_data.
-- Его задача: подготовить demo/source PostgreSQL, из которого Debezium будет читать изменения.

-- app_events - пример таблицы с событиями приложения.
-- Она нужна как простой источник CDC: PostgreSQL -> Debezium -> Kafka -> ClickHouse.
CREATE TABLE IF NOT EXISTS app_events (
  -- BIGSERIAL генерирует числовой id на стороне PostgreSQL.
  id BIGSERIAL PRIMARY KEY,
  -- Время события. Если вставляющий клиент не передал время, PostgreSQL ставит now().
  event_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Технические и пользовательские поля, по которым удобно строить аналитику.
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
  -- metadata хранит произвольные дополнительные свойства события в JSONB.
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Индексы ускоряют локальные запросы в PostgreSQL.
-- Для Debezium они не обязательны, но помогают, если смотреть source DB напрямую.
CREATE INDEX IF NOT EXISTS idx_app_events_event_time ON app_events(event_time);
CREATE INDEX IF NOT EXISTS idx_app_events_user_id ON app_events(user_id);
CREATE INDEX IF NOT EXISTS idx_app_events_event_type ON app_events(event_type);

-- REPLICA IDENTITY FULL заставляет PostgreSQL отдавать Debezium полную строку при UPDATE/DELETE.
-- Это проще для demo и для sink-коннектора, потому что downstream получает все поля записи.
ALTER TABLE app_events REPLICA IDENTITY FULL;

-- car_inventory - осмысленная demo-таблица со складами автомобилей в разных городах.
-- Именно ее удобно использовать для вопросов модели: бренды, города, пробег, цены, статусы.
CREATE TABLE IF NOT EXISTS car_inventory (
  id BIGSERIAL PRIMARY KEY,
  -- batch_id позволяет отличить одну тестовую загрузку данных от другой.
  batch_id TEXT NOT NULL,
  inventory_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- География и склад, где машина физически находится.
  city TEXT NOT NULL,
  warehouse_name TEXT NOT NULL,
  -- Описание автомобиля.
  brand TEXT NOT NULL,
  model TEXT NOT NULL,
  model_year INTEGER NOT NULL,
  body_type TEXT NOT NULL,
  color TEXT NOT NULL,
  -- VIN уникален, чтобы demo-данные выглядели как реальные складские записи.
  vin TEXT NOT NULL UNIQUE,
  -- Статус нужен для аналитики: доступна, зарезервирована, на обслуживании.
  stock_status TEXT NOT NULL,
  price_usd NUMERIC(12, 2) NOT NULL,
  mileage_km INTEGER NOT NULL,
  arrived_at TIMESTAMPTZ NOT NULL,
  -- Дополнительные свойства, которые не хочется фиксировать отдельными колонками.
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Индексы под типовые вопросы к складской таблице.
CREATE INDEX IF NOT EXISTS idx_car_inventory_city ON car_inventory(city);
CREATE INDEX IF NOT EXISTS idx_car_inventory_brand ON car_inventory(brand);
CREATE INDEX IF NOT EXISTS idx_car_inventory_status ON car_inventory(stock_status);
CREATE INDEX IF NOT EXISTS idx_car_inventory_batch_id ON car_inventory(batch_id);

-- Полная репликация строки для Debezium, как и у app_events.
ALTER TABLE car_inventory REPLICA IDENTITY FULL;

-- Начальная синтетическая загрузка app_events.
-- Эти 1000 строк нужны только для demo-сценариев и проверки CDC-пайплайна.
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
  -- Набор типов событий имитирует обычный web/LLM-продукт.
  (ARRAY['page_view', 'chat_message', 'tool_call', 'model_completion', 'error'])[1 + (gs % 5)],
  (ARRAY['/', '/chat', '/agents', '/settings', '/api/completions'])[1 + (gs % 5)],
  -- Иногда добавляем 500/429, чтобы в Grafana были ошибки и rate-limit события.
  CASE WHEN gs % 17 = 0 THEN 500 WHEN gs % 11 = 0 THEN 429 ELSE 200 END,
  50 + (random() * 2500)::int,
  (ARRAY['qwen2.5:7b', 'qwen2.5:14b', 'qwen3:14b', 'local-vision-model'])[1 + (gs % 4)],
  50 + (random() * 1500)::int,
  20 + (random() * 1200)::int,
  round((random() * 0.08)::numeric, 6),
  jsonb_build_object(
    'source', 'seed',
    'environment', 'local',
    'request_id', gen_random_uuid()::text
  )
FROM generate_series(1, 1000) AS gs;
