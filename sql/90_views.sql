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
