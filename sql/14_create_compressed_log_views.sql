CREATE VIEW IF NOT EXISTS analytics.v_es_log_compressed_templates AS
SELECT
  batch_id,
  source_name,
  index_name,
  batch_no,
  event_time_from,
  event_time_to,
  rows_read,
  JSONExtractString(template, 'id') AS template_id,
  JSONExtractString(template, 'template') AS template_text,
  JSONExtractUInt(template, 'count') AS template_count,
  parseDateTime64BestEffortOrNull(JSONExtractString(template, 'first_seen'), 3, 'UTC') AS first_seen,
  parseDateTime64BestEffortOrNull(JSONExtractString(template, 'last_seen'), 3, 'UTC') AS last_seen,
  JSONExtractString(JSONExtractArrayRaw(template, 'levels', 'top')[1], 'value') AS top_level,
  JSONExtractUInt(JSONExtractArrayRaw(template, 'levels', 'top')[1], 'count') AS top_level_count,
  JSONExtractString(JSONExtractArrayRaw(template, 'services', 'top')[1], 'value') AS top_service,
  JSONExtractUInt(JSONExtractArrayRaw(template, 'services', 'top')[1], 'count') AS top_service_count,
  JSONExtractString(JSONExtractArrayRaw(template, 'hosts', 'top')[1], 'value') AS top_host,
  JSONExtractUInt(JSONExtractArrayRaw(template, 'hosts', 'top')[1], 'count') AS top_host_count
FROM analytics.es_log_compressed_batches
ARRAY JOIN JSONExtractArrayRaw(compressed_json, 'templates') AS template;

CREATE VIEW IF NOT EXISTS analytics.v_es_log_compressed_rle_runs AS
SELECT
  batch_id,
  source_name,
  index_name,
  batch_no,
  event_time_from,
  event_time_to,
  JSONExtractString(run, 'template_id') AS template_id,
  JSONExtractUInt(run, 'count') AS run_count,
  parseDateTime64BestEffortOrNull(JSONExtractString(run, 'start_time'), 3, 'UTC') AS start_time,
  parseDateTime64BestEffortOrNull(JSONExtractString(run, 'end_time'), 3, 'UTC') AS end_time,
  JSONExtractString(run, 'service') AS service,
  JSONExtractString(run, 'host') AS host,
  JSONExtractString(run, 'level') AS level,
  JSONExtractUInt(run, 'first_record_index') AS first_record_index,
  JSONExtractUInt(run, 'last_record_index') AS last_record_index
FROM analytics.es_log_compressed_batches
ARRAY JOIN JSONExtractArrayRaw(compressed_json, 'rle_runs') AS run;
