CREATE OR REPLACE VIEW analytics.v_es_log_map_batch_inputs AS
WITH
  JSONExtractArrayRaw(compressed_json, 'templates') AS templates_raw,
  JSONExtractArrayRaw(compressed_json, 'rle_runs') AS runs_raw,
  arrayMap(t -> JSONExtractString(t, 'id'), templates_raw) AS template_ids,
  arrayMap(t -> JSONExtractString(t, 'template'), templates_raw) AS template_texts,
  arrayFilter(
    run ->
      JSONExtractString(run, 'level') IN ('WARN', 'ERROR', 'FATAL')
      OR positionCaseInsensitive(arrayElement(template_texts, indexOf(template_ids, JSONExtractString(run, 'template_id'))), 'timeout') > 0
      OR positionCaseInsensitive(arrayElement(template_texts, indexOf(template_ids, JSONExtractString(run, 'template_id'))), 'degraded') > 0
      OR positionCaseInsensitive(arrayElement(template_texts, indexOf(template_ids, JSONExtractString(run, 'template_id'))), 'failed') > 0
      OR positionCaseInsensitive(arrayElement(template_texts, indexOf(template_ids, JSONExtractString(run, 'template_id'))), 'exhausted') > 0
      OR positionCaseInsensitive(arrayElement(template_texts, indexOf(template_ids, JSONExtractString(run, 'template_id'))), 'backlog') > 0
      OR positionCaseInsensitive(arrayElement(template_texts, indexOf(template_ids, JSONExtractString(run, 'template_id'))), 'lock') > 0
      OR positionCaseInsensitive(arrayElement(template_texts, indexOf(template_ids, JSONExtractString(run, 'template_id'))), 'ssl') > 0
      OR positionCaseInsensitive(arrayElement(template_texts, indexOf(template_ids, JSONExtractString(run, 'template_id'))), 'tls') > 0,
    runs_raw
  ) AS signal_runs_raw,
  if(length(signal_runs_raw) > 0, arraySlice(signal_runs_raw, 1, 80), arraySlice(runs_raw, 1, 80)) AS selected_runs_raw,
  toJSONString(
    CAST(
      (
        batch_id,
        source_name,
        index_name,
        batch_no,
        toString(event_time_from),
        toString(event_time_to),
        rows_read,
        raw_chars,
        compressed_chars,
        arrayMap(
          t ->
            CAST(
              (
                JSONExtractString(t, 'id'),
                JSONExtractString(t, 'template'),
                JSONExtractUInt(t, 'count'),
                JSONExtractString(t, 'first_seen'),
                JSONExtractString(t, 'last_seen'),
                JSONExtractString(JSONExtractArrayRaw(t, 'levels', 'top')[1], 'value'),
                JSONExtractString(JSONExtractArrayRaw(t, 'services', 'top')[1], 'value'),
                JSONExtractString(JSONExtractArrayRaw(t, 'hosts', 'top')[1], 'value')
              ),
              'Tuple(template_id String, template_text String, event_count UInt64, first_seen String, last_seen String, top_level String, top_service String, top_host String)'
            ),
          templates_raw
        ),
        arrayMap(
          run ->
            CAST(
              (
                JSONExtractString(run, 'template_id'),
                arrayElement(template_texts, indexOf(template_ids, JSONExtractString(run, 'template_id'))),
                JSONExtractUInt(run, 'count'),
                JSONExtractString(run, 'start_time'),
                JSONExtractString(run, 'end_time'),
                JSONExtractString(run, 'service'),
                JSONExtractString(run, 'host'),
                JSONExtractString(run, 'level')
              ),
              'Tuple(template_id String, template_text String, run_count UInt64, start_time String, end_time String, service String, host String, level String)'
            ),
          selected_runs_raw
        )
      ),
      'Tuple(batch_id String, source_name String, index_name String, batch_no UInt64, event_time_from String, event_time_to String, rows_read UInt64, raw_chars UInt64, compressed_chars UInt64, important_templates Array(Tuple(template_id String, template_text String, event_count UInt64, first_seen String, last_seen String, top_level String, top_service String, top_host String)), important_runs Array(Tuple(template_id String, template_text String, run_count UInt64, start_time String, end_time String, service String, host String, level String)))'
    )
  ) AS map_input_json
SELECT
  batch_id,
  source_name,
  index_name,
  batch_no,
  event_time_from,
  event_time_to,
  rows_read,
  length(map_input_json) AS map_input_chars,
  map_input_json
FROM analytics.es_log_compressed_batches;
