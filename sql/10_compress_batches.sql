INSERT INTO analytics.es_log_compressed_batches
SELECT
  batch_id,
  source_name,
  index_name,
  batch_no,
  event_time_from,
  event_time_to,
  rows_read,
  raw_chars,
  length(compressed_json) AS compressed_chars,
  compressed_json,
  created_at
FROM
(
  SELECT
    concat(source_name, ':', index_name, ':', toString(batch_no)) AS batch_id,
    source_name,
    index_name,
    batch_no,
    min(event_time) AS event_time_from,
    max(event_time) AS event_time_to,
    count() AS rows_read,
    sum(length(document_json)) AS raw_chars,
    log_compress_json(arrayStringConcat(groupArray(document_json), '\n')) AS compressed_json,
    now64(3) AS created_at
  FROM
  (
    WITH 500 AS batch_rows
    SELECT
      *,
      intDiv(row_number() OVER (ORDER BY event_time, document_id) - 1, batch_rows) AS batch_no
    FROM analytics.es_raw_logs
    WHERE source_name = 'elasticsearch-synthetic'
      AND index_name LIKE 'synthetic-logs%'
  )
  GROUP BY source_name, index_name, batch_no
)
ORDER BY batch_no
SETTINGS allow_experimental_analyzer = 1;
