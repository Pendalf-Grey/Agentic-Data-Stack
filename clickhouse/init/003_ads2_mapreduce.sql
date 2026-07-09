CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.es_raw_logs
(
  source_name LowCardinality(String),
  index_name String,
  document_id String,
  event_time DateTime64(3, 'UTC'),
  ingest_time DateTime64(3, 'UTC') DEFAULT now64(3),
  document_json String,
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_time)
ORDER BY (source_name, index_name, event_time, document_id);

CREATE TABLE IF NOT EXISTS analytics.es_log_compressed_batches
(
  batch_id String,
  source_name LowCardinality(String),
  index_name String,
  batch_no UInt64,
  event_time_from DateTime64(3, 'UTC'),
  event_time_to DateTime64(3, 'UTC'),
  rows_read UInt64,
  raw_chars UInt64,
  compressed_chars UInt64,
  compressed_json String,
  created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(event_time_from)
ORDER BY (source_name, index_name, event_time_from, batch_no);

CREATE TABLE IF NOT EXISTS analytics.llm_investigations
(
  investigation_id String,
  user_question String,
  time_from DateTime64(3, 'UTC'),
  time_to DateTime64(3, 'UTC'),
  source_name LowCardinality(String),
  index_like String,
  status LowCardinality(String),
  created_at DateTime64(3, 'UTC') DEFAULT now64(3),
  updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY investigation_id;

CREATE TABLE IF NOT EXISTS analytics.llm_prompts
(
  prompt_name String,
  prompt String,
  updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY prompt_name;

CREATE TABLE IF NOT EXISTS analytics.llm_map_results
(
  investigation_id String,
  batch_id String,
  batch_no UInt64,
  event_time_from DateTime64(3, 'UTC'),
  event_time_to DateTime64(3, 'UTC'),
  rows_read UInt64,
  map_summary_json String,
  created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (investigation_id, batch_no, batch_id);

CREATE TABLE IF NOT EXISTS analytics.llm_map_queue
(
  investigation_id String,
  batch_id String,
  batch_no UInt64,
  event_time_from DateTime64(3, 'UTC'),
  event_time_to DateTime64(3, 'UTC'),
  rows_read UInt64,
  status LowCardinality(String),
  locked_by String,
  locked_until DateTime64(3, 'UTC'),
  attempt_count UInt32,
  last_error String,
  version UInt64,
  created_at DateTime64(3, 'UTC') DEFAULT now64(3),
  updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_time_from)
ORDER BY (investigation_id, batch_no, batch_id);

CREATE TABLE IF NOT EXISTS analytics.llm_reduce_results
(
  investigation_id String,
  reduce_level UInt8,
  reduce_group UInt64,
  summary_json String,
  refined_sql String DEFAULT '',
  created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (investigation_id, reduce_level, reduce_group);

CREATE VIEW IF NOT EXISTS analytics.v_llm_map_results_preview AS
SELECT
  investigation_id,
  batch_no,
  batch_id,
  event_time_from,
  event_time_to,
  rows_read,
  left(map_summary_json, 2000) AS map_summary_preview,
  created_at
FROM analytics.llm_map_results;

CREATE VIEW IF NOT EXISTS analytics.v_llm_map_queue_status AS
SELECT
  investigation_id,
  status,
  count() AS batches,
  sum(rows_read) AS rows_read,
  min(batch_no) AS first_batch_no,
  max(batch_no) AS last_batch_no,
  min(event_time_from) AS event_time_from,
  max(event_time_to) AS event_time_to
FROM analytics.llm_map_queue FINAL
GROUP BY investigation_id, status;
