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

CREATE TABLE IF NOT EXISTS analytics.car_inventory_raw
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
ORDER BY (city, brand, model, id);

CREATE VIEW IF NOT EXISTS analytics.v_car_inventory_summary AS
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
FROM analytics.car_inventory_raw
GROUP BY city, warehouse_name, brand
ORDER BY city ASC, cars DESC, brand ASC;

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
