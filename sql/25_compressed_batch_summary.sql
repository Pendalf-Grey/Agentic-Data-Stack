SELECT
  compressed_batches,
  source_records,
  total_raw_chars AS approximated_raw_chars,
  formatReadableSize(total_raw_chars) AS approximated_raw_size,
  total_compressed_chars AS compressed_chars,
  formatReadableSize(total_compressed_chars) AS compressed_size,
  first_batch_no,
  last_batch_no,
  event_time_from,
  event_time_to,
  source_names,
  index_names
FROM
(
  SELECT
    count() AS compressed_batches,
    sum(rows_read) AS source_records,
    sum(raw_chars) AS total_raw_chars,
    sum(compressed_chars) AS total_compressed_chars,
    min(batch_no) AS first_batch_no,
    max(batch_no) AS last_batch_no,
    min(event_time_from) AS event_time_from,
    max(event_time_to) AS event_time_to,
    groupUniqArray(source_name) AS source_names,
    groupUniqArray(index_name) AS index_names
  FROM analytics.es_log_compressed_batches
);
