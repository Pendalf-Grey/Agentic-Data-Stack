SELECT
  investigation_id,
  status,
  batches,
  rows_read,
  first_batch_no,
  last_batch_no,
  event_time_from,
  event_time_to
FROM analytics.v_llm_map_queue_status
ORDER BY investigation_id, status;
