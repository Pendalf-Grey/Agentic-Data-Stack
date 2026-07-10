ALTER TABLE analytics.es_log_compressed_batches
  ADD COLUMN IF NOT EXISTS document_id_to String DEFAULT '' AFTER event_time_to;
