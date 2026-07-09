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
