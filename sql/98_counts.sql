SELECT 'raw_logs' AS table_name, count() AS rows
FROM analytics.es_raw_logs
UNION ALL
SELECT 'compressed_batches' AS table_name, count() AS rows
FROM analytics.es_log_compressed_batches
UNION ALL
SELECT 'llm_map_results' AS table_name, count() AS rows
FROM analytics.llm_map_results
UNION ALL
SELECT 'llm_map_queue' AS table_name, count() AS rows
FROM analytics.llm_map_queue FINAL;
