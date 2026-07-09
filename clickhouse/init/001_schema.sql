-- Minimal demo schema for the Elasticsearch-only ADS-2 branch.
-- ClickHouse is the analytical workspace. Elasticsearch stays outside the
-- compose stack and is imported into ClickHouse by elasticsearch-connector.

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.ingestion_offsets
(
  source_name LowCardinality(String),
  checkpoint_name String,
  last_event_time DateTime64(3, 'UTC'),
  last_document_id String,
  updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (source_name, checkpoint_name);
