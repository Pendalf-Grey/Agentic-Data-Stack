SELECT
  sum(template_count) AS template_count_sum,
  sum(rows_read) AS rows_read_sum
FROM
(
  SELECT
    batch_id,
    any(rows_read) AS rows_read,
    sum(JSONExtractUInt(template, 'count')) AS template_count
  FROM analytics.es_log_compressed_batches
  ARRAY JOIN JSONExtractArrayRaw(compressed_json, 'templates') AS template
  GROUP BY batch_id
);
