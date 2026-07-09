INSERT INTO analytics.llm_reduce_results
SELECT
  investigation_id,
  reduce_level,
  reduce_group,
  summary_json,
  JSONExtractString(summary_json, 'refined_sql') AS refined_sql,
  created_at
FROM
(
SELECT
  i.investigation_id AS investigation_id,
  2 AS reduce_level,
  0 AS reduce_group,
  aiGenerate(
    concat(
      'User question: ', i.user_question, '\n',
      'Reduce summaries:',
      '\n',
      arrayStringConcat(groupArray(r.summary_json), '\n')
    ),
    'You are the final Reduce LLM for ClickHouse log analysis. Return valid JSON only with keys final_answer, refined_sql, grafana_hint. refined_sql must be a SELECT only against analytics.es_raw_logs with time, source, and index filters. Do not invent data.',
    0.1
  ) AS summary_json,
  now64(3) AS created_at
FROM analytics.llm_investigations AS i FINAL
INNER JOIN analytics.llm_reduce_results AS r FINAL
  ON r.investigation_id = i.investigation_id
WHERE i.investigation_id = {investigation_id:String}
  AND r.reduce_level = 1
GROUP BY i.investigation_id, i.user_question
)
SETTINGS
  allow_experimental_ai_functions = 1,
  ai_function_credentials = 'llm_reduce',
  ai_function_max_api_calls_per_query = 1,
  ai_function_max_input_tokens_per_query = 1000000,
  ai_function_max_output_tokens_per_query = 20000,
  ai_function_request_timeout_sec = 240,
  ai_function_max_retries = 2;
