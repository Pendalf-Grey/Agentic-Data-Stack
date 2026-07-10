import os
import socket
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import clickhouse_connect
from fastmcp import FastMCP
from fastmcp.tools import Tool


EPOCH = "toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC')"
DEFAULT_SOURCE_NAME = os.getenv("ADS_LLM_LOG_SOURCE_NAME", os.getenv("LOGS_SOURCE_NAME", "elasticsearch-demo"))
DEFAULT_INDEX_LIKE = os.getenv("ADS_LLM_LOG_INDEX_LIKE", os.getenv("LOGS_INDEX_LIKE", "nginx-logs-%"))

mcp = FastMCP(name="ads-log-mapreduce")


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "analytics"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "analytics_password"),
        database=os.getenv("CLICKHOUSE_DATABASE", "analytics"),
        secure=os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true",
        connect_timeout=int(os.getenv("CLICKHOUSE_CONNECT_TIMEOUT", "30")),
        send_receive_timeout=int(os.getenv("CLICKHOUSE_SEND_RECEIVE_TIMEOUT", "900")),
    )


def command(sql: str) -> Any:
    return ch_client().command(sql)


def rows(sql: str) -> List[Dict[str, Any]]:
    result = ch_client().query(sql)
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def scalar(sql: str) -> Any:
    result = ch_client().query(sql)
    return result.result_rows[0][0] if result.result_rows else None


def version_expr(offset: str = "batch_no") -> str:
    return f"toUInt64(toUnixTimestamp64Milli(now64(3))) * 1000000 + toUInt64({offset})"


def read_file(path_value: str) -> str:
    return Path(path_value).read_text(encoding="utf-8").strip()


def ensure_schema() -> None:
    command(
        """
CREATE TABLE IF NOT EXISTS analytics.llm_prompts
(
  prompt_name String,
  prompt String,
  updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY prompt_name
"""
    )
    command(
        """
CREATE TABLE IF NOT EXISTS analytics.llm_map_queue
(
  investigation_id String,
  batch_id String,
  batch_no UInt64,
  event_time_from DateTime64(3, 'UTC'),
  event_time_to DateTime64(3, 'UTC'),
  rows_read UInt64,
  status LowCardinality(String),
  locked_by String,
  locked_until DateTime64(3, 'UTC'),
  attempt_count UInt32,
  last_error String,
  version UInt64,
  created_at DateTime64(3, 'UTC') DEFAULT now64(3),
  updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_time_from)
ORDER BY (investigation_id, batch_no, batch_id)
"""
    )
    command(
        """
CREATE VIEW IF NOT EXISTS analytics.v_llm_map_queue_status AS
SELECT
  investigation_id,
  status,
  count() AS batches,
  sum(rows_read) AS rows_read,
  min(batch_no) AS first_batch_no,
  max(batch_no) AS last_batch_no,
  min(event_time_from) AS event_time_from,
  max(event_time_to) AS event_time_to
FROM analytics.llm_map_queue FINAL
GROUP BY investigation_id, status
"""
    )
    view_file = os.getenv("MAP_INPUT_VIEW_SQL", "/workspace/sql/17_create_map_batch_inputs.sql")
    if Path(view_file).exists():
        command(read_file(view_file))


def sync_map_prompt() -> Dict[str, str]:
    prompt_file = os.getenv("MAP_PROMPT_FILE", "/workspace/prompts/map_compressed_logs.en.txt")
    prompt_name = os.getenv("MAP_PROMPT_NAME", "map_compressed_logs_en")
    prompt = read_file(prompt_file)
    command(
        f"""
INSERT INTO analytics.llm_prompts (prompt_name, prompt, updated_at)
VALUES ({sql_string(prompt_name)}, {sql_string(prompt)}, now64(3))
"""
    )
    return {"prompt_name": prompt_name, "prompt_file": prompt_file}


def queue_status(investigation_id: str) -> List[Dict[str, Any]]:
    return rows(
        f"""
SELECT
  status,
  count() AS batches,
  sum(rows_read) AS rows_read,
  min(batch_no) AS first_batch_no,
  max(batch_no) AS last_batch_no
FROM analytics.llm_map_queue FINAL
WHERE investigation_id = {sql_string(investigation_id)}
GROUP BY status
ORDER BY status
"""
    )


def create_log_investigation(
    user_question: str,
    time_from: Optional[str] = None,
    time_to: Optional[str] = None,
    source_name: str = DEFAULT_SOURCE_NAME,
    index_like: str = DEFAULT_INDEX_LIKE,
    investigation_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Create an ADS log investigation for the ClickHouse MapReduce pipeline."""
    ensure_schema()
    investigation_id = investigation_id or "log-investigation-" + uuid.uuid4().hex[:12]
    bounds = rows(
        f"""
SELECT
  toString(min(event_time_from)) AS min_time,
  toString(max(event_time_to)) AS max_time,
  count() AS batches,
  sum(rows_read) AS rows_read
FROM analytics.es_log_compressed_batches
WHERE source_name = {sql_string(source_name)}
  AND index_name LIKE {sql_string(index_like)}
"""
    )[0]
    resolved_time_from = time_from or bounds["min_time"]
    resolved_time_to = time_to or bounds["max_time"]
    command(
        f"""
INSERT INTO analytics.llm_investigations
(
  investigation_id,
  user_question,
  time_from,
  time_to,
  source_name,
  index_like,
  status
)
VALUES
(
  {sql_string(investigation_id)},
  {sql_string(user_question)},
  toDateTime64({sql_string(resolved_time_from)}, 3, 'UTC'),
  toDateTime64({sql_string(resolved_time_to)}, 3, 'UTC'),
  {sql_string(source_name)},
  {sql_string(index_like)},
  'running'
)
"""
    )
    return {
        "investigation_id": investigation_id,
        "time_from": resolved_time_from,
        "time_to": resolved_time_to,
        "available_batches": bounds["batches"],
        "available_rows_read": bounds["rows_read"],
    }


def enqueue_log_map_batches(investigation_id: str) -> Dict[str, Any]:
    """Create pending queue rows for all matching compressed log batches."""
    ensure_schema()
    sync_map_prompt()
    command(
        f"""
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
  'pending',
  '',
  {EPOCH},
  0,
  '',
  {version_expr("b.batch_no")},
  now64(3),
  now64(3)
FROM analytics.llm_investigations AS i FINAL
INNER JOIN analytics.es_log_compressed_batches AS b
  ON b.source_name = i.source_name
 AND b.index_name LIKE i.index_like
 AND b.event_time_to >= i.time_from
 AND b.event_time_from < i.time_to
WHERE i.investigation_id = {sql_string(investigation_id)}
  AND (i.investigation_id, b.batch_id) NOT IN
  (
    SELECT investigation_id, batch_id
    FROM analytics.llm_map_queue FINAL
    WHERE investigation_id = {sql_string(investigation_id)}
  )
  AND (i.investigation_id, b.batch_id) NOT IN
  (
    SELECT investigation_id, batch_id
    FROM analytics.llm_map_results FINAL
    WHERE investigation_id = {sql_string(investigation_id)}
  )
ORDER BY b.batch_no
"""
    )
    return {"investigation_id": investigation_id, "queue": queue_status(investigation_id)}


def run_log_map_step(
    investigation_id: str,
    claim_size: int = 1,
    lease_seconds: int = 900,
    max_attempts: int = 3,
    request_timeout_sec: int = 180,
    ai_retries: int = 1,
    max_input_tokens: int = 1000000,
    max_output_tokens: int = 100000,
    worker_index: int = 0,
    worker_count: int = 1,
) -> Dict[str, Any]:
    """Run one bounded Map-LLM step through ClickHouse aiGenerate."""
    ensure_schema()
    sync_map_prompt()
    claim_size = max(1, min(int(claim_size), 10))
    lease_id = f"{socket.gethostname()}-{uuid.uuid4().hex[:12]}"
    command(
        f"""
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
  investigation_id,
  batch_id,
  batch_no,
  event_time_from,
  event_time_to,
  rows_read,
  'in_progress',
  {sql_string(lease_id)},
  now64(3) + toIntervalSecond({lease_seconds}),
  attempt_count + 1,
  last_error,
  {version_expr("batch_no")},
  created_at,
  now64(3)
FROM
(
  SELECT *
  FROM analytics.llm_map_queue FINAL
  WHERE investigation_id = {sql_string(investigation_id)}
    AND attempt_count < {max_attempts}
    AND modulo(batch_no, toUInt64({worker_count})) = toUInt64({worker_index})
    AND (status IN ('pending', 'failed') OR (status = 'in_progress' AND locked_until < now64(3)))
    AND (investigation_id, batch_id) NOT IN
    (
      SELECT investigation_id, batch_id
      FROM analytics.llm_map_results FINAL
      WHERE investigation_id = {sql_string(investigation_id)}
    )
  ORDER BY batch_no
  LIMIT {claim_size}
)
"""
    )
    claimed = int(
        scalar(
            f"""
SELECT count()
FROM analytics.llm_map_queue FINAL
WHERE investigation_id = {sql_string(investigation_id)}
  AND status = 'in_progress'
  AND locked_by = {sql_string(lease_id)}
"""
        )
        or 0
    )
    if claimed == 0:
        return {"investigation_id": investigation_id, "claimed": 0, "queue": queue_status(investigation_id)}

    prompt = read_file(os.getenv("MAP_PROMPT_FILE", "/workspace/prompts/map_compressed_logs.en.txt"))
    try:
        command(
            f"""
INSERT INTO analytics.llm_map_results
SELECT
  q.investigation_id,
  b.batch_id,
  b.batch_no,
  b.event_time_from,
  b.event_time_to,
  b.rows_read,
  aiGenerate(
    concat(
      'Investigation context:',
      '\\nuser_question=', i.user_question,
      '\\ninvestigation_time_from=', toString(i.time_from),
      '\\ninvestigation_time_to=', toString(i.time_to),
      '\\nbatch_time_from=', toString(b.event_time_from),
      '\\nbatch_time_to=', toString(b.event_time_to),
      '\\nmap_input_json=', m.map_input_json
    ),
    {sql_string(prompt)},
    0.1
  ) AS map_summary_json,
  now64(3) AS created_at
FROM analytics.llm_map_queue AS q FINAL
INNER JOIN analytics.es_log_compressed_batches AS b
  ON b.batch_id = q.batch_id
INNER JOIN analytics.v_es_log_map_batch_inputs AS m
  ON m.batch_id = b.batch_id
INNER JOIN analytics.llm_investigations AS i FINAL
  ON i.investigation_id = q.investigation_id
WHERE q.investigation_id = {sql_string(investigation_id)}
  AND q.status = 'in_progress'
  AND q.locked_by = {sql_string(lease_id)}
  AND q.locked_until >= now64(3)
ORDER BY q.batch_no
SETTINGS
  allow_experimental_ai_functions = 1,
  ai_function_credentials = 'llm_map',
  ai_function_max_api_calls_per_query = {claimed},
  ai_function_max_input_tokens_per_query = {max_input_tokens},
  ai_function_max_output_tokens_per_query = {max_output_tokens},
  ai_function_request_timeout_sec = {request_timeout_sec},
  ai_function_max_retries = {ai_retries}
"""
        )
    except Exception as exc:
        command(
            f"""
INSERT INTO analytics.llm_map_queue
SELECT
  investigation_id,
  batch_id,
  batch_no,
  event_time_from,
  event_time_to,
  rows_read,
  'failed',
  locked_by,
  locked_until,
  attempt_count,
  {sql_string(str(exc)[:1000])},
  {version_expr("batch_no")},
  created_at,
  now64(3)
FROM analytics.llm_map_queue FINAL
WHERE investigation_id = {sql_string(investigation_id)}
  AND status = 'in_progress'
  AND locked_by = {sql_string(lease_id)}
"""
        )
        return {"investigation_id": investigation_id, "claimed": claimed, "status": "failed", "error": str(exc)[:1000], "queue": queue_status(investigation_id)}

    command(
        f"""
INSERT INTO analytics.llm_map_queue
SELECT
  q.investigation_id,
  q.batch_id,
  q.batch_no,
  q.event_time_from,
  q.event_time_to,
  q.rows_read,
  'done',
  q.locked_by,
  q.locked_until,
  q.attempt_count,
  q.last_error,
  {version_expr("q.batch_no")},
  q.created_at,
  now64(3)
FROM analytics.llm_map_queue AS q FINAL
INNER JOIN analytics.llm_map_results AS r FINAL
  ON r.investigation_id = q.investigation_id
 AND r.batch_id = q.batch_id
WHERE q.investigation_id = {sql_string(investigation_id)}
  AND q.status = 'in_progress'
  AND q.locked_by = {sql_string(lease_id)}
"""
    )
    return {"investigation_id": investigation_id, "claimed": claimed, "status": "done", "queue": queue_status(investigation_id)}


def run_log_reduce(investigation_id: str, group_size: int = 50, request_timeout_sec: int = 240) -> Dict[str, Any]:
    """Run reduce over stored map results and store reduce summaries."""
    group_size = max(1, min(int(group_size), 200))
    command(
        f"""
INSERT INTO analytics.llm_reduce_results
SELECT
  investigation_id,
  1 AS reduce_level,
  reduce_group,
  aiGenerate(
    concat('Map summaries:', '\\n', arrayStringConcat(groupArray(map_summary_json), '\\n')),
    'You are a level-1 Reduce LLM for SRE log analysis. Compress Map-LLM results into valid JSON only. Keep only root causes, affected services, time windows, ClickHouse filters, evidence, missing data, and confidence. Do not invent data.',
    1.0
  ) AS summary_json,
  '' AS refined_sql,
  now64(3) AS created_at
FROM
(
  SELECT
    *,
    intDiv(row_number() OVER (ORDER BY batch_no) - 1, {group_size}) AS reduce_group
  FROM analytics.llm_map_results FINAL
  WHERE investigation_id = {sql_string(investigation_id)}
)
GROUP BY investigation_id, reduce_group
SETTINGS
  allow_experimental_ai_functions = 1,
  ai_function_credentials = 'llm_reduce',
  ai_function_max_api_calls_per_query = 16,
  ai_function_max_input_tokens_per_query = 1000000,
  ai_function_max_output_tokens_per_query = 100000,
  ai_function_request_timeout_sec = {request_timeout_sec},
  ai_function_max_retries = 2
"""
    )
    command(
        f"""
INSERT INTO analytics.llm_reduce_results
SELECT
  i.investigation_id,
  2 AS reduce_level,
  0 AS reduce_group,
  aiGenerate(
    concat('User question:', '\\n', i.user_question, '\\n\\nReduced map summaries:', '\\n', arrayStringConcat(groupArray(r.summary_json), '\\n')),
    'You are the final Reduce LLM for SRE log analysis. Return valid JSON only with executive_summary, root_causes, affected_services, incident_timeline, evidence, preventive_actions, refined_sql, dashboard_hints, confidence. Do not invent data.',
    1.0
  ) AS summary_json,
  JSONExtractString(summary_json, 'refined_sql') AS refined_sql,
  now64(3) AS created_at
FROM analytics.llm_investigations AS i FINAL
INNER JOIN analytics.llm_reduce_results AS r FINAL
  ON r.investigation_id = i.investigation_id
WHERE i.investigation_id = {sql_string(investigation_id)}
  AND r.reduce_level = 1
GROUP BY i.investigation_id, i.user_question
SETTINGS
  allow_experimental_ai_functions = 1,
  ai_function_credentials = 'llm_reduce',
  ai_function_max_api_calls_per_query = 1,
  ai_function_max_input_tokens_per_query = 1000000,
  ai_function_max_output_tokens_per_query = 100000,
  ai_function_request_timeout_sec = {request_timeout_sec},
  ai_function_max_retries = 2
"""
    )
    return get_log_investigation_status(investigation_id)


def get_log_investigation_status(investigation_id: str) -> Dict[str, Any]:
    """Return queue, map, and reduce status for an investigation."""
    map_counts = rows(
        f"""
SELECT
  count() AS map_rows,
  sum(rows_read) AS mapped_rows_read,
  min(batch_no) AS first_batch_no,
  max(batch_no) AS last_batch_no
FROM analytics.llm_map_results FINAL
WHERE investigation_id = {sql_string(investigation_id)}
"""
    )[0]
    reduce_counts = rows(
        f"""
SELECT reduce_level, count() AS rows
FROM analytics.llm_reduce_results FINAL
WHERE investigation_id = {sql_string(investigation_id)}
GROUP BY reduce_level
ORDER BY reduce_level
"""
    )
    return {"investigation_id": investigation_id, "queue": queue_status(investigation_id), "map": map_counts, "reduce": reduce_counts}


def get_log_investigation_results(investigation_id: str, preview_chars: int = 4000) -> Dict[str, Any]:
    """Return final reduce previews, or map previews if reduce is not available."""
    preview_chars = max(500, min(int(preview_chars), 12000))
    reduce_rows = rows(
        f"""
SELECT
  reduce_level,
  reduce_group,
  left(summary_json, {preview_chars}) AS summary_json_preview,
  left(refined_sql, {preview_chars}) AS refined_sql_preview,
  created_at
FROM analytics.llm_reduce_results FINAL
WHERE investigation_id = {sql_string(investigation_id)}
ORDER BY reduce_level DESC, reduce_group ASC
LIMIT 5
"""
    )
    if reduce_rows:
        return {"investigation_id": investigation_id, "reduce_results": reduce_rows}
    return {
        "investigation_id": investigation_id,
        "map_results": rows(
            f"""
SELECT
  batch_no,
  event_time_from,
  event_time_to,
  rows_read,
  left(map_summary_json, {preview_chars}) AS map_summary_json_preview
FROM analytics.llm_map_results FINAL
WHERE investigation_id = {sql_string(investigation_id)}
ORDER BY batch_no
LIMIT 10
"""
        ),
    }


mcp.add_tool(Tool.from_function(create_log_investigation))
mcp.add_tool(Tool.from_function(enqueue_log_map_batches))
mcp.add_tool(Tool.from_function(run_log_map_step))
mcp.add_tool(Tool.from_function(run_log_reduce))
mcp.add_tool(Tool.from_function(get_log_investigation_status))
mcp.add_tool(Tool.from_function(get_log_investigation_results))


if __name__ == "__main__":
    mcp.run(
        transport=os.getenv("ADS_MCP_TRANSPORT", "sse"),
        host=os.getenv("ADS_MCP_BIND_HOST", "0.0.0.0"),
        port=int(os.getenv("ADS_MCP_PORT", "8000")),
    )
