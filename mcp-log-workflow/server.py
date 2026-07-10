import os
import json
import socket
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import clickhouse_connect
from fastmcp import FastMCP
from fastmcp.tools import Tool


EPOCH = "toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC')"
DEFAULT_SOURCE_NAME = os.getenv("ADS_LLM_LOG_SOURCE_NAME", os.getenv("LOGS_SOURCE_NAME", "elasticsearch-demo"))
DEFAULT_INDEX_LIKE = os.getenv("ADS_LLM_LOG_INDEX_LIKE", os.getenv("LOGS_INDEX_LIKE", "nginx-logs-%"))
DEFAULT_MAP_CONTEXT_TOKENS = int(os.getenv("LLM_MAP_CONTEXT_TOKENS", "132000"))
DEFAULT_REDUCE_CONTEXT_TOKENS = int(os.getenv("LLM_REDUCE_CONTEXT_TOKENS", "256000"))
DEFAULT_WORKFLOW_MAX_RUNTIME_SEC = int(os.getenv("ADS_LLM_WORKFLOW_MAX_RUNTIME_SEC", "1200"))
DEFAULT_WORKFLOW_MAP_CLAIM_SIZE = int(os.getenv("ADS_LLM_WORKFLOW_MAP_CLAIM_SIZE", "4"))
DEFAULT_WORKFLOW_MAP_MAX_ATTEMPTS = int(os.getenv("ADS_LLM_WORKFLOW_MAP_MAX_ATTEMPTS", "2"))

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


def queue_totals(status_rows: List[Dict[str, Any]]) -> Dict[str, int]:
    totals = {"total": 0, "done": 0, "failed": 0, "pending": 0, "in_progress": 0}
    for row in status_rows:
        status = str(row.get("status") or "")
        batches = int(row.get("batches") or 0)
        totals["total"] += batches
        if status in totals:
            totals[status] += batches
    totals["terminal"] = totals["done"] + totals["failed"]
    totals["unfinished"] = totals["pending"] + totals["in_progress"]
    return totals


def claimable_map_batches(investigation_id: str, max_attempts: int) -> int:
    return int(
        scalar(
            f"""
SELECT count()
FROM analytics.llm_map_queue FINAL
WHERE investigation_id = {sql_string(investigation_id)}
  AND attempt_count < {int(max_attempts)}
  AND
  (
    status IN ('pending', 'failed')
    OR (status = 'in_progress' AND locked_until < now64(3))
  )
  AND (investigation_id, batch_id) NOT IN
  (
    SELECT investigation_id, batch_id
    FROM analytics.llm_map_results FINAL
    WHERE investigation_id = {sql_string(investigation_id)}
      AND isValidJSON(map_summary_json)
  )
"""
        )
        or 0
    )


def reduce_schema_context() -> str:
    schema_rows = rows(
        """
SELECT
  table,
  name,
  type
FROM system.columns
WHERE database = currentDatabase()
  AND table IN
  (
    'es_raw_logs',
    'es_log_compressed_batches',
    'v_es_log_map_batch_inputs',
    'llm_investigations',
    'llm_map_queue',
    'v_llm_map_queue_status',
    'llm_map_results',
    'v_llm_map_results_preview',
    'llm_reduce_results'
  )
ORDER BY table, position
"""
    )
    grouped: Dict[str, List[str]] = {}
    for row in schema_rows:
        grouped.setdefault(row["table"], []).append(f"{row['name']} {row['type']}")
    lines = ["Actual ClickHouse ADS-2 schema. Use only these tables/views and columns:"]
    for table_name in sorted(grouped):
        columns = ", ".join(grouped[table_name])
        lines.append(f"- analytics.{table_name}({columns})")
    lines.extend(
        [
            "",
            "Reduce-required artifacts:",
            "- analytics.llm_map_queue: use investigation_id, batch_id, batch_no, status, rows_read, event_time_from, event_time_to to verify every queued batch is done before reducing.",
            "- analytics.llm_map_results: use investigation_id, batch_id, batch_no, rows_read, event_time_from, event_time_to, map_summary_json as the only semantic input for reduce.",
            "- analytics.llm_reduce_results: store and read reduce output; reduce must not summarize directly from raw logs.",
            "- analytics.es_log_compressed_batches: use only for compact coverage checks, batch counts, time windows, and dashboard queries.",
            "- analytics.v_es_log_map_batch_inputs: this is the compact LLM-facing batch input view; use it for explaining what map-LLM saw, not for replacing map results during reduce.",
            "- analytics.es_raw_logs: not needed for reduce. Use it only for a separate bounded verification query when explicitly requested.",
            "",
            "SQL rules:",
            "- Never reference a table named logs.",
            "- Prefer analytics.llm_map_results, analytics.llm_reduce_results, analytics.llm_map_queue, analytics.v_llm_map_queue_status, and analytics.es_log_compressed_batches.",
            "- For compressed-log verification, query analytics.es_log_compressed_batches or analytics.v_es_log_map_batch_inputs.",
            "- If raw-log verification is explicitly necessary, use analytics.es_raw_logs and JSONExtract* over document_json, with tight time filters and LIMIT.",
            "- refined_sql must be executable ClickHouse SQL for this schema, without FORMAT clauses.",
        ]
    )
    return "\n".join(lines)


def compact_reduce_schema_context() -> str:
    return "\n".join(
        [
            "Actual ADS-2 ClickHouse schema for reduce:",
            "[REDUCE INPUT] analytics.llm_map_results(investigation_id String, batch_id String, batch_no UInt64, event_time_from DateTime64(3,'UTC'), event_time_to DateTime64(3,'UTC'), rows_read UInt64, map_summary_json String, created_at DateTime64(3,'UTC'))",
            "[REDUCE COMPLETENESS] analytics.llm_map_queue(investigation_id String, batch_id String, batch_no UInt64, event_time_from DateTime64(3,'UTC'), event_time_to DateTime64(3,'UTC'), rows_read UInt64, status LowCardinality(String), last_error String, created_at DateTime64(3,'UTC'), updated_at DateTime64(3,'UTC'))",
            "[REDUCE OUTPUT] analytics.llm_reduce_results(investigation_id String, reduce_level UInt8, reduce_group UInt64, summary_json String, refined_sql String, created_at DateTime64(3,'UTC'))",
            "[COVERAGE/DASHBOARD] analytics.es_log_compressed_batches(batch_id String, source_name LowCardinality(String), index_name String, batch_no UInt64, event_time_from DateTime64(3,'UTC'), event_time_to DateTime64(3,'UTC'), rows_read UInt64, raw_chars UInt64, compressed_chars UInt64, compressed_json String, created_at DateTime64(3,'UTC'))",
            "[STATUS VIEW] analytics.v_llm_map_queue_status(investigation_id String, status LowCardinality(String), batches UInt64, rows_read UInt64, first_batch_no UInt64, last_batch_no UInt64, event_time_from DateTime64(3,'UTC'), event_time_to DateTime64(3,'UTC'))",
            "[RAW LOGS - not for reduce] analytics.es_raw_logs(source_name LowCardinality(String), index_name String, document_id String, event_time DateTime64(3,'UTC'), ingest_time DateTime64(3,'UTC'), document_json String, version UInt64)",
            "Rules: reduce uses analytics.llm_map_results.map_summary_json only; never use a table named logs; refined_sql must use only columns listed above and no FORMAT clause.",
        ]
    )


def reduce_json_instruction() -> str:
    return (
        "Return minified valid JSON only. "
        "Keys: executive_summary, root_causes, top_services, latency_findings, recommendations, confidence. "
        "Arrays max 3 strings. No markdown."
    )


def normalize_reduce_question(user_question: str) -> str:
    question = (user_question or "").strip()
    if question.isascii():
        return question

    lowered = question.casefold()
    intents: List[str] = []
    if any(token in lowered for token in ("пад", "ошиб", "сбо", "failed", "error")):
        intents.append("identify which services failed most often and why")
    if any(token in lowered for token in ("латен", "задерж", "timeout", "latency")):
        intents.append("explain whether latency increased and what caused it")
    if any(token in lowered for token in ("рекомен", "исправ", "предотврат", "avoid", "recommend")):
        intents.append("recommend how to fix the issues and prevent recurrence")
    if any(token in lowered for token in ("граф", "дашборд", "grafana", "dashboard")):
        intents.append("provide Grafana dashboard hints when useful")
    if not intents:
        intents.append("answer the user's log-analysis question using the map summaries")
    return "User question was normalized for the reduce model: " + "; ".join(intents) + "."


def refined_sql_for_investigation(investigation_id: str) -> str:
    safe_id = investigation_id.replace("'", "''")
    return (
        "SELECT batch_no, batch_id, event_time_from, event_time_to, rows_read, "
        "map_summary_json, created_at "
        "FROM analytics.llm_map_results "
        f"WHERE investigation_id = '{safe_id}' "
        "ORDER BY batch_no"
    )


def has_valid_final_reduce(investigation_id: str) -> bool:
    return bool(
        scalar(
            f"""
SELECT count()
FROM analytics.llm_reduce_results FINAL
WHERE investigation_id = {sql_string(investigation_id)}
  AND reduce_level = 2
  AND reduce_group = 0
  AND length(summary_json) > 0
  AND isValidJSON(summary_json)
"""
        )
        or 0
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
      AND isValidJSON(map_summary_json)
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
    max_input_tokens: int = DEFAULT_MAP_CONTEXT_TOKENS,
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
        AND isValidJSON(map_summary_json)
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
  AND isValidJSON(r.map_summary_json)
"""
    )
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
  'failed',
  q.locked_by,
  q.locked_until,
  q.attempt_count,
  'Map LLM returned invalid JSON',
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
  AND NOT isValidJSON(r.map_summary_json)
"""
    )
    return {"investigation_id": investigation_id, "claimed": claimed, "status": "done", "queue": queue_status(investigation_id)}


def run_log_reduce(investigation_id: str, group_size: int = 50, request_timeout_sec: int = 240) -> Dict[str, Any]:
    """Run reduce over stored map results and store reduce summaries."""
    group_size = max(1, min(int(group_size), 200))
    status_rows = queue_status(investigation_id)
    totals = queue_totals(status_rows)
    if totals["total"] == 0 or totals["unfinished"] > 0:
        return {
            "investigation_id": investigation_id,
            "status": "not_ready",
            "error": "Reduce is allowed only after every queued map batch is terminal: done or failed.",
            "done_batches": totals["done"],
            "failed_batches": totals["failed"],
            "total_batches": totals["total"],
            "queue": status_rows,
        }
    schema_context = reduce_schema_context()
    map_rows = int(
        scalar(
            f"""
SELECT count()
FROM analytics.llm_map_results FINAL
WHERE investigation_id = {sql_string(investigation_id)}
  AND isValidJSON(map_summary_json)
"""
        )
        or 0
    )
    if map_rows == 0:
        return {
            "investigation_id": investigation_id,
            "status": "not_ready",
            "error": "No valid map results are available for reduce.",
            "done_batches": totals["done"],
            "failed_batches": totals["failed"],
            "total_batches": totals["total"],
            "queue": status_rows,
        }
    stored_question = str(
        scalar(
            f"""
SELECT user_question
FROM analytics.llm_investigations FINAL
WHERE investigation_id = {sql_string(investigation_id)}
"""
        )
        or ""
    )
    question_context = normalize_reduce_question(stored_question)
    if map_rows <= group_size:
        refined_sql = refined_sql_for_investigation(investigation_id)
        command(
            f"""
INSERT INTO analytics.llm_reduce_results
SELECT
  i.investigation_id,
  2 AS reduce_level,
  0 AS reduce_group,
  aiGenerate(
    concat(
      'User wants log incident analysis and recommendations for the investigation period. Normalized request:',
      '\\n',
      {sql_string(question_context)},
      '\\n\\nUse only these compact batch notes:',
      '\\n',
      arrayStringConcat(
        groupArray(
          concat(
            'batch ', toString(r.batch_no), ': ',
            left(JSONExtractString(r.map_summary_json, 'executive_summary'), 450),
            ' roots=', left(JSONExtractRaw(r.map_summary_json, 'root_cause_hypotheses'), 500),
            ' latency=', left(JSONExtractRaw(r.map_summary_json, 'latency_findings'), 350),
            ' recs=', left(JSONExtractRaw(r.map_summary_json, 'recommendations'), 350)
          )
        ),
        '\\n'
      )
    ),
    {sql_string(reduce_json_instruction())},
    1.0
  ) AS summary_json,
  {sql_string(refined_sql)} AS refined_sql,
  now64(3) AS created_at
FROM analytics.llm_investigations AS i FINAL
INNER JOIN analytics.llm_map_results AS r FINAL
  ON r.investigation_id = i.investigation_id
WHERE i.investigation_id = {sql_string(investigation_id)}
  AND isValidJSON(r.map_summary_json)
GROUP BY i.investigation_id
SETTINGS
  allow_experimental_ai_functions = 1,
  ai_function_credentials = 'llm_reduce',
  ai_function_max_api_calls_per_query = 1,
  ai_function_max_input_tokens_per_query = {DEFAULT_REDUCE_CONTEXT_TOKENS},
  ai_function_max_output_tokens_per_query = 4096,
  ai_function_request_timeout_sec = {request_timeout_sec},
  ai_function_max_retries = 2
"""
        )
        status = get_log_investigation_status(investigation_id)
        if not has_valid_final_reduce(investigation_id):
            status["status"] = "reduce_failed"
            status["error"] = "Reduce LLM returned empty or invalid JSON."
        return status

    command(
        f"""
INSERT INTO analytics.llm_reduce_results
SELECT
  investigation_id,
  1 AS reduce_level,
  reduce_group,
  aiGenerate(
    concat(
      'ClickHouse schema:',
      '\\n',
      {sql_string(schema_context)},
      '\\n\\nMap summaries:',
      '\\n',
      arrayStringConcat(groupArray(map_summary_json), '\\n')
    ),
    'You are a level-1 Reduce LLM for SRE log analysis. Compress Map-LLM results into valid JSON only. Keep only root causes, affected services, time windows, ClickHouse filters, evidence, missing data, and confidence. Do not invent data. If you mention SQL filters or tables, use only the supplied ClickHouse schema.',
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
    AND isValidJSON(map_summary_json)
)
GROUP BY investigation_id, reduce_group
SETTINGS
  allow_experimental_ai_functions = 1,
  ai_function_credentials = 'llm_reduce',
  ai_function_max_api_calls_per_query = 16,
  ai_function_max_input_tokens_per_query = {DEFAULT_REDUCE_CONTEXT_TOKENS},
  ai_function_max_output_tokens_per_query = 4096,
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
    concat(
      'User question:',
      '\\n',
      {sql_string(question_context)},
      '\\n\\nClickHouse schema:',
      '\\n',
      {sql_string(schema_context)},
      '\\n\\nReduced map summaries:',
      '\\n',
      arrayStringConcat(groupArray(r.summary_json), '\\n')
    ),
    {sql_string(reduce_json_instruction())},
    1.0
  ) AS summary_json,
  JSONExtractString(summary_json, 'refined_sql') AS refined_sql,
  now64(3) AS created_at
FROM analytics.llm_investigations AS i FINAL
INNER JOIN analytics.llm_reduce_results AS r FINAL
  ON r.investigation_id = i.investigation_id
WHERE i.investigation_id = {sql_string(investigation_id)}
  AND r.reduce_level = 1
  AND isValidJSON(r.summary_json)
  AND length(r.summary_json) > 0
GROUP BY i.investigation_id, i.user_question
SETTINGS
  allow_experimental_ai_functions = 1,
  ai_function_credentials = 'llm_reduce',
  ai_function_max_api_calls_per_query = 1,
  ai_function_max_input_tokens_per_query = {DEFAULT_REDUCE_CONTEXT_TOKENS},
  ai_function_max_output_tokens_per_query = 4096,
  ai_function_request_timeout_sec = {request_timeout_sec},
  ai_function_max_retries = 2
"""
    )
    status = get_log_investigation_status(investigation_id)
    if not has_valid_final_reduce(investigation_id):
        status["status"] = "reduce_failed"
        status["error"] = "Reduce LLM returned empty or invalid JSON."
    return status


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


def get_log_analysis_status(investigation_id: str) -> Dict[str, Any]:
    """Return high-level ADS-2 log analysis status without exposing batch workflow controls."""
    status_rows = queue_status(investigation_id)
    totals = queue_totals(status_rows)
    map_counts = rows(
        f"""
SELECT
  count() AS map_rows,
  sum(isValidJSON(map_summary_json)) AS valid_map_rows,
  sum(rows_read) AS mapped_rows_read,
  min(batch_no) AS first_batch_no,
  max(batch_no) AS last_batch_no
FROM analytics.llm_map_results FINAL
WHERE investigation_id = {sql_string(investigation_id)}
"""
    )[0]
    reduce_counts = rows(
        f"""
SELECT
  reduce_level,
  count() AS rows,
  max(isValidJSON(summary_json)) AS has_valid_json,
  max(length(summary_json)) AS max_summary_chars,
  max(created_at) AS last_created_at
FROM analytics.llm_reduce_results FINAL
WHERE investigation_id = {sql_string(investigation_id)}
GROUP BY reduce_level
ORDER BY reduce_level
"""
    )
    if totals["total"] == 0:
        status = "empty"
    elif totals["unfinished"] > 0 or claimable_map_batches(investigation_id, DEFAULT_WORKFLOW_MAP_MAX_ATTEMPTS) > 0:
        status = "running"
    elif has_valid_final_reduce(investigation_id):
        status = "reduced"
    elif reduce_counts:
        status = "reduce_failed"
    elif totals["done"] > 0:
        status = "mapped"
    else:
        status = "failed"
    return {
        "investigation_id": investigation_id,
        "status": status,
        "batches": totals,
        "queue": status_rows,
        "map": map_counts,
        "reduce": reduce_counts,
    }


def get_log_analysis_results(investigation_id: str, preview_chars: int = 8000) -> Dict[str, Any]:
    """Return high-level ADS-2 log analysis results for Kimi's final answer."""
    preview_chars = max(1000, min(int(preview_chars), 20000))
    status = get_log_analysis_status(investigation_id)
    reduce_rows = rows(
        f"""
SELECT
  reduce_level,
  reduce_group,
  isValidJSON(summary_json) AS valid_json,
  left(summary_json, {preview_chars}) AS summary_json,
  refined_sql,
  created_at
FROM analytics.llm_reduce_results FINAL
WHERE investigation_id = {sql_string(investigation_id)}
  AND length(summary_json) > 0
  AND isValidJSON(summary_json)
ORDER BY reduce_level DESC, reduce_group ASC
LIMIT 5
"""
    )
    failed_batches = rows(
        f"""
SELECT
  batch_no,
  rows_read,
  attempt_count,
  left(last_error, 500) AS last_error
FROM analytics.llm_map_queue FINAL
WHERE investigation_id = {sql_string(investigation_id)}
  AND status = 'failed'
ORDER BY batch_no
LIMIT 20
"""
    )
    map_previews = []
    if not reduce_rows:
        map_previews = rows(
            f"""
SELECT
  batch_no,
  event_time_from,
  event_time_to,
  rows_read,
  isValidJSON(map_summary_json) AS valid_json,
  left(map_summary_json, {preview_chars}) AS map_summary_json
FROM analytics.llm_map_results FINAL
WHERE investigation_id = {sql_string(investigation_id)}
ORDER BY batch_no
LIMIT 10
"""
        )
    return {
        "investigation_id": investigation_id,
        "status": status,
        "reduce_results": reduce_rows,
        "failed_batches": failed_batches,
        "map_previews": map_previews,
    }


def run_log_analysis(
    user_question: str,
    time_from: Optional[str] = None,
    time_to: Optional[str] = None,
    source_name: str = DEFAULT_SOURCE_NAME,
    index_like: str = DEFAULT_INDEX_LIKE,
    investigation_id: Optional[str] = None,
    map_claim_size: int = DEFAULT_WORKFLOW_MAP_CLAIM_SIZE,
    map_max_attempts: int = DEFAULT_WORKFLOW_MAP_MAX_ATTEMPTS,
    max_runtime_sec: int = DEFAULT_WORKFLOW_MAX_RUNTIME_SEC,
    map_request_timeout_sec: int = 240,
    reduce_request_timeout_sec: int = 360,
    reduce_group_size: int = 50,
) -> Dict[str, Any]:
    """Run the complete ADS-2 log MapReduce analysis as one high-level workflow trigger.

    Kimi should call this once for a broad log-analysis question. The MCP service
    keeps the map queue internal: ClickHouse stores the queue and executes the
    aiGenerate map/reduce SQL, while this MCP function repeats bounded steps
    until the workflow is done, blocked, or the runtime limit is reached.
    """
    started = time.monotonic()
    map_claim_size = max(1, min(int(map_claim_size), 10))
    map_max_attempts = max(1, min(int(map_max_attempts), 5))
    max_runtime_sec = max(60, min(int(max_runtime_sec), 7200))

    investigation = create_log_investigation(
        user_question=user_question,
        time_from=time_from,
        time_to=time_to,
        source_name=source_name,
        index_like=index_like,
        investigation_id=investigation_id,
    )
    current_id = investigation["investigation_id"]
    enqueue = enqueue_log_map_batches(current_id)

    map_steps = 0
    last_step: Dict[str, Any] = {}
    while True:
        status_rows = queue_status(current_id)
        totals = queue_totals(status_rows)
        claimable = claimable_map_batches(current_id, map_max_attempts)

        if totals["total"] == 0:
            workflow_status = "empty"
            break
        if totals["unfinished"] == 0 and claimable == 0:
            workflow_status = "mapped"
            break
        if claimable == 0:
            workflow_status = "blocked"
            break
        if time.monotonic() - started >= max_runtime_sec:
            workflow_status = "running"
            break

        last_step = run_log_map_step(
            current_id,
            claim_size=map_claim_size,
            max_attempts=map_max_attempts,
            request_timeout_sec=map_request_timeout_sec,
        )
        map_steps += 1

    reduce_result: Dict[str, Any] = {}
    current_status = get_log_analysis_status(current_id)
    current_totals = current_status["batches"]
    if current_totals["unfinished"] == 0 and current_totals["done"] > 0:
        reduce_result = run_log_reduce(
            current_id,
            group_size=reduce_group_size,
            request_timeout_sec=reduce_request_timeout_sec,
        )
        workflow_status = "reduced" if has_valid_final_reduce(current_id) else "reduce_failed"

    results = get_log_analysis_results(current_id)
    results.update(
        {
            "workflow_status": workflow_status,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "map_steps_executed_inside_mcp": map_steps,
            "investigation": investigation,
            "enqueue": enqueue,
            "last_map_step": last_step,
            "reduce": reduce_result,
            "note": "Map queue processing was orchestrated inside ads-log-workflow MCP; Kimi did not need to call per-batch map tools.",
        }
    )
    return results


mcp.add_tool(Tool.from_function(run_log_analysis))
mcp.add_tool(Tool.from_function(get_log_analysis_status))
mcp.add_tool(Tool.from_function(get_log_analysis_results))


if __name__ == "__main__":
    mcp.run(
        transport=os.getenv("ADS_MCP_TRANSPORT", "sse"),
        host=os.getenv("ADS_MCP_BIND_HOST", "0.0.0.0"),
        port=int(os.getenv("ADS_MCP_PORT", "8000")),
    )
