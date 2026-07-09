INSERT INTO analytics.llm_map_queue
(
  investigation_id,
  batch_id,
  batch_no,
  event_time_from,
  event_time_to,
  rows_read,
  status,
  locked_by,
  locked_until,
  attempt_count,
  last_error,
  version,
  created_at,
  updated_at
)
SELECT
  i.investigation_id,
  b.batch_id,
  b.batch_no,
  b.event_time_from,
  b.event_time_to,
  b.rows_read,
  'pending' AS status,
  '' AS locked_by,
  toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC') AS locked_until,
  0 AS attempt_count,
  '' AS last_error,
  toUInt64(toUnixTimestamp64Milli(now64(3))) * 1000000 + b.batch_no AS version,
  now64(3) AS created_at,
  now64(3) AS updated_at
FROM analytics.llm_investigations AS i FINAL
INNER JOIN analytics.es_log_compressed_batches AS b
  ON b.source_name = i.source_name
 AND b.index_name LIKE i.index_like
 AND b.event_time_to >= i.time_from
 AND b.event_time_from < i.time_to
WHERE i.investigation_id = {investigation_id:String}
  AND (i.investigation_id, b.batch_id) NOT IN
  (
    SELECT investigation_id, batch_id
    FROM analytics.llm_map_queue FINAL
    WHERE investigation_id = {investigation_id:String}
  )
  AND (i.investigation_id, b.batch_id) NOT IN
  (
    SELECT investigation_id, batch_id
    FROM analytics.llm_map_results FINAL
    WHERE investigation_id = {investigation_id:String}
  )
ORDER BY b.batch_no;
