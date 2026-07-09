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
