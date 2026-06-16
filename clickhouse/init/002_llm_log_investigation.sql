-- Tables used by the LLM-guided SQL refinement flow.
-- The flow reads raw Elasticsearch documents from ClickHouse in bounded chunks,
-- asks Kimi to summarize each chunk, then stores chunk reports and final SQL here.

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.llm_log_investigations
(
  investigation_id String,
  user_question String,
  source_table String,
  time_from DateTime64(3, 'UTC'),
  time_to DateTime64(3, 'UTC'),
  status LowCardinality(String),
  refined_sql String DEFAULT '',
  final_report String DEFAULT '',
  error String DEFAULT '',
  created_at DateTime64(3, 'UTC') DEFAULT now64(3),
  updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY investigation_id;
CREATE TABLE IF NOT EXISTS analytics.llm_log_chunk_reports
(
  investigation_id String,
  chunk_id UInt32,
  chunk_from DateTime64(3, 'UTC'),
  chunk_to DateTime64(3, 'UTC'),
  rows_read UInt64,
  chars_read UInt64,
  kimi_summary_json String,
  candidate_filters_json String DEFAULT '',
  evidence_json String DEFAULT '',
  error String DEFAULT '',
  created_at DateTime64(3, 'UTC') DEFAULT now64(3),
  updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (investigation_id, chunk_id);

CREATE TABLE IF NOT EXISTS analytics.llm_log_refined_sql
(
  investigation_id String,
  refined_sql String,
  rationale String,
  confidence Float32 DEFAULT 0,
  validation_result String DEFAULT '',
  created_at DateTime64(3, 'UTC') DEFAULT now64(3),
  updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY investigation_id;
