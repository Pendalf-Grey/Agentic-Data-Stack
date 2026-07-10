#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import socket
import time
import uuid
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


EPOCH = "toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC')"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MAP_PROMPT_NAME = "map_compressed_logs_en"
DEFAULT_MAP_PROMPT_FILE = "prompts/map_compressed_logs.en.txt"


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def http_query(url: str, user: str, password: str, database: str, query: str, timeout: int = 900) -> str:
    request = Request(
        f"{url}/?{urlencode({'database': database})}",
        data=query.encode("utf-8"),
        method="POST",
    )
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    request.add_header("Authorization", "Basic " + token)
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(body.strip() or str(exc)) from exc


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key, value)


def version_expr(offset: str = "batch_no") -> str:
    return f"toUInt64(toUnixTimestamp64Milli(now64(3))) * 1000000 + toUInt64({offset})"


def create_prompts_table_sql() -> str:
    return """
CREATE TABLE IF NOT EXISTS analytics.llm_prompts
(
  prompt_name String,
  prompt String,
  updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY prompt_name
"""


def resolve_project_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    cwd_path = Path.cwd() / path
    if cwd_path.exists():
        return cwd_path
    return REPO_ROOT / path


def read_prompt_file(path_value: str) -> str:
    prompt_path = resolve_project_path(path_value)
    if not prompt_path.exists():
        raise SystemExit(f"Map prompt file not found: {prompt_path}")
    return prompt_path.read_text(encoding="utf-8").strip()


def upsert_prompt_sql(prompt_name: str, prompt: str) -> str:
    return f"""
INSERT INTO analytics.llm_prompts
(
  prompt_name,
  prompt,
  updated_at
)
VALUES
(
  {sql_string(prompt_name)},
  {sql_string(prompt)},
  now64(3)
)
"""


def create_queue_table_sql() -> str:
    return """
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


def create_queue_status_view_sql() -> str:
    return """
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


def create_map_batch_inputs_view_sql() -> str:
    return (REPO_ROOT / "clickhouse/init/006_create_map_batch_inputs.sql").read_text(encoding="utf-8")


def require_investigation_id(args: argparse.Namespace) -> str:
    if not args.investigation_id:
        raise SystemExit("Set --investigation-id or INVESTIGATION_ID.")
    return args.investigation_id


def enqueue_sql(args: argparse.Namespace) -> str:
    investigation_id = sql_string(require_investigation_id(args))
    return f"""
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
  {EPOCH} AS locked_until,
  0 AS attempt_count,
  '' AS last_error,
  {version_expr("b.batch_no")} AS version,
  now64(3) AS created_at,
  now64(3) AS updated_at
FROM analytics.llm_investigations AS i FINAL
INNER JOIN analytics.es_log_compressed_batches AS b
  ON b.source_name = i.source_name
 AND b.index_name LIKE i.index_like
 AND b.event_time_to >= i.time_from
 AND b.event_time_from < i.time_to
WHERE i.investigation_id = {investigation_id}
  AND (i.investigation_id, b.batch_id) NOT IN
  (
    SELECT investigation_id, batch_id
    FROM analytics.llm_map_queue FINAL
    WHERE investigation_id = {investigation_id}
  )
  AND (i.investigation_id, b.batch_id) NOT IN
  (
    SELECT investigation_id, batch_id
    FROM analytics.llm_map_results FINAL
    WHERE investigation_id = {investigation_id}
  )
ORDER BY b.batch_no
"""


def claim_sql(args: argparse.Namespace, lease_id: str) -> str:
    investigation_id = sql_string(require_investigation_id(args))
    lease_sql = sql_string(lease_id)
    return f"""
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
  'in_progress' AS status,
  {lease_sql} AS locked_by,
  now64(3) + toIntervalSecond({args.lease_seconds}) AS locked_until,
  attempt_count + 1 AS attempt_count,
  last_error,
  {version_expr("batch_no")} AS version,
  created_at,
  now64(3) AS updated_at
FROM
(
  SELECT *
  FROM analytics.llm_map_queue FINAL
  WHERE investigation_id = {investigation_id}
    AND attempt_count < {args.max_attempts}
    AND modulo(batch_no, toUInt64({args.worker_count})) = toUInt64({args.worker_index})
    AND (status IN ('pending', 'failed') OR (status = 'in_progress' AND locked_until < now64(3)))
    AND (investigation_id, batch_id) NOT IN
    (
      SELECT investigation_id, batch_id
      FROM analytics.llm_map_results FINAL
      WHERE investigation_id = {investigation_id}
    )
  ORDER BY batch_no
  LIMIT {args.claim_size}
)
"""


def count_claimed_sql(args: argparse.Namespace, lease_id: str) -> str:
    return f"""
SELECT count()
FROM analytics.llm_map_queue FINAL
WHERE investigation_id = {sql_string(require_investigation_id(args))}
  AND status = 'in_progress'
  AND locked_by = {sql_string(lease_id)}
FORMAT TabSeparatedRaw
"""


def map_sql(args: argparse.Namespace, lease_id: str) -> str:
    investigation_id = sql_string(require_investigation_id(args))
    system_prompt = sql_string(read_prompt_file(args.map_prompt_file))
    return f"""
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
    {system_prompt},
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
WHERE q.investigation_id = {investigation_id}
  AND q.status = 'in_progress'
  AND q.locked_by = {sql_string(lease_id)}
  AND q.locked_until >= now64(3)
  AND (q.investigation_id, q.batch_id) NOT IN
  (
    SELECT investigation_id, batch_id
    FROM analytics.llm_map_results FINAL
    WHERE investigation_id = {investigation_id}
  )
ORDER BY q.batch_no
SETTINGS
  allow_experimental_ai_functions = 1,
  ai_function_credentials = 'llm_map',
  ai_function_max_api_calls_per_query = {args.claim_size},
  ai_function_max_input_tokens_per_query = {args.max_input_tokens},
  ai_function_max_output_tokens_per_query = {args.max_output_tokens},
  ai_function_request_timeout_sec = {args.request_timeout_sec},
  ai_function_max_retries = {args.ai_retries}
"""


def mark_done_sql(args: argparse.Namespace, lease_id: str) -> str:
    investigation_id = sql_string(require_investigation_id(args))
    return f"""
INSERT INTO analytics.llm_map_queue
SELECT
  q.investigation_id,
  q.batch_id,
  q.batch_no,
  q.event_time_from,
  q.event_time_to,
  q.rows_read,
  'done' AS status,
  q.locked_by,
  q.locked_until,
  q.attempt_count,
  q.last_error,
  {version_expr("q.batch_no")} AS version,
  q.created_at,
  now64(3) AS updated_at
FROM analytics.llm_map_queue AS q FINAL
INNER JOIN analytics.llm_map_results AS r FINAL
  ON r.investigation_id = q.investigation_id
 AND r.batch_id = q.batch_id
WHERE q.investigation_id = {investigation_id}
  AND q.status = 'in_progress'
  AND q.locked_by = {sql_string(lease_id)}
"""


def mark_failed_sql(args: argparse.Namespace, lease_id: str, error: str) -> str:
    investigation_id = sql_string(require_investigation_id(args))
    error = error[:1000]
    return f"""
INSERT INTO analytics.llm_map_queue
SELECT
  investigation_id,
  batch_id,
  batch_no,
  event_time_from,
  event_time_to,
  rows_read,
  'failed' AS status,
  locked_by,
  locked_until,
  attempt_count,
  {sql_string(error)} AS last_error,
  {version_expr("batch_no")} AS version,
  created_at,
  now64(3) AS updated_at
FROM analytics.llm_map_queue FINAL
WHERE investigation_id = {investigation_id}
  AND status = 'in_progress'
  AND locked_by = {sql_string(lease_id)}
"""


def status_sql(args: argparse.Namespace) -> str:
    return f"""
SELECT
  status,
  count() AS batches,
  sum(rows_read) AS rows_read,
  min(batch_no) AS first_batch_no,
  max(batch_no) AS last_batch_no
FROM analytics.llm_map_queue FINAL
WHERE investigation_id = {sql_string(require_investigation_id(args))}
GROUP BY status
ORDER BY status
FORMAT JSONEachRow
"""


def run_query(args: argparse.Namespace, query: str) -> str:
    return http_query(args.clickhouse_url, args.user, args.password, args.database, query, args.http_timeout)


def sync_map_prompt(args: argparse.Namespace) -> None:
    prompt_path = resolve_project_path(args.map_prompt_file)
    prompt = read_prompt_file(args.map_prompt_file)
    run_query(args, create_prompts_table_sql())
    run_query(args, upsert_prompt_sql(args.map_prompt_name, prompt))
    print(json.dumps({"prompt_name": args.map_prompt_name, "prompt_file": str(prompt_path), "status": "synced"}, ensure_ascii=False), flush=True)


def print_status(args: argparse.Namespace) -> None:
    out = run_query(args, status_sql(args)).strip()
    if not out:
        print(json.dumps({"investigation_id": require_investigation_id(args), "queue": []}, ensure_ascii=False))
        return
    rows = [json.loads(line) for line in out.splitlines()]
    print(json.dumps({"investigation_id": require_investigation_id(args), "queue": rows}, ensure_ascii=False))


def main() -> int:
    load_dotenv(Path(".env"))
    parser = argparse.ArgumentParser(description="Lease-based ClickHouse Map-LLM queue worker.")
    parser.add_argument("--clickhouse-url", default=os.getenv("CLICKHOUSE_URL", "http://localhost:8123"))
    parser.add_argument("--database", default=os.getenv("CLICKHOUSE_DB", "analytics"))
    parser.add_argument("--user", default=os.getenv("CLICKHOUSE_USER", "analytics"))
    parser.add_argument("--password", default=os.getenv("CLICKHOUSE_PASSWORD", "analytics_password"))
    parser.add_argument("--investigation-id", default=os.getenv("INVESTIGATION_ID"))
    parser.add_argument("--worker-id", default=os.getenv("MAP_WORKER_ID", f"{socket.gethostname()}-{os.getpid()}"))
    parser.add_argument("--worker-index", type=int, default=int(os.getenv("MAP_WORKER_INDEX", "0")))
    parser.add_argument("--worker-count", type=int, default=int(os.getenv("MAP_WORKER_COUNT", "1")))
    parser.add_argument("--claim-size", type=int, default=int(os.getenv("MAP_CLAIM_SIZE", "10")))
    parser.add_argument("--lease-seconds", type=int, default=int(os.getenv("MAP_LEASE_SECONDS", "900")))
    parser.add_argument("--max-attempts", type=int, default=int(os.getenv("MAP_MAX_ATTEMPTS", "3")))
    parser.add_argument("--max-iterations", type=int, default=int(os.getenv("MAP_MAX_ITERATIONS", "1")))
    parser.add_argument("--max-input-tokens", type=int, default=int(os.getenv("AI_FUNCTION_MAX_INPUT_TOKENS_PER_QUERY", "1000000")))
    parser.add_argument("--max-output-tokens", type=int, default=int(os.getenv("AI_FUNCTION_MAX_OUTPUT_TOKENS_PER_QUERY", "100000")))
    parser.add_argument("--request-timeout-sec", type=int, default=int(os.getenv("AI_FUNCTION_REQUEST_TIMEOUT_SEC", "180")))
    parser.add_argument("--ai-retries", type=int, default=int(os.getenv("AI_FUNCTION_MAX_RETRIES", "2")))
    parser.add_argument("--http-timeout", type=int, default=1200)
    parser.add_argument("--map-prompt-name", default=os.getenv("MAP_PROMPT_NAME", DEFAULT_MAP_PROMPT_NAME))
    parser.add_argument("--map-prompt-file", default=os.getenv("MAP_PROMPT_FILE", DEFAULT_MAP_PROMPT_FILE))
    parser.add_argument("--create-schema", action="store_true")
    parser.add_argument("--sync-prompts", action="store_true")
    parser.add_argument("--prompts-only", action="store_true")
    parser.add_argument("--enqueue", action="store_true")
    parser.add_argument("--enqueue-only", action="store_true")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()

    if args.worker_count < 1:
        raise SystemExit("--worker-count must be >= 1")
    if args.worker_index < 0 or args.worker_index >= args.worker_count:
        raise SystemExit("--worker-index must be between 0 and worker-count - 1")

    if args.create_schema:
        run_query(args, create_prompts_table_sql())
        run_query(args, create_queue_table_sql())
        run_query(args, create_queue_status_view_sql())
        run_query(args, create_map_batch_inputs_view_sql())
    if args.create_schema or args.sync_prompts or args.prompts_only:
        sync_map_prompt(args)
    if args.prompts_only:
        return 0
    if args.enqueue or args.enqueue_only:
        run_query(args, enqueue_sql(args))
    if args.status or args.enqueue_only:
        print_status(args)
        return 0

    iteration = 0
    total_claimed = 0
    while args.max_iterations == 0 or iteration < args.max_iterations:
        iteration += 1
        lease_id = f"{args.worker_id}-{uuid.uuid4().hex[:12]}"
        run_query(args, claim_sql(args, lease_id))
        claimed = int((run_query(args, count_claimed_sql(args, lease_id)).strip() or "0").splitlines()[0])
        if claimed == 0:
            print(json.dumps({"ok": True, "claimed": total_claimed, "done": "queue_empty"}, ensure_ascii=False))
            break
        total_claimed += claimed
        print(json.dumps({"iteration": iteration, "lease_id": lease_id, "claimed": claimed}, ensure_ascii=False), flush=True)
        try:
            run_query(args, map_sql(args, lease_id))
        except Exception as exc:
            run_query(args, mark_failed_sql(args, lease_id, str(exc)))
            print(json.dumps({"iteration": iteration, "lease_id": lease_id, "status": "failed", "error": str(exc)[:300]}, ensure_ascii=False), flush=True)
            if args.max_iterations == 1:
                raise
            continue
        run_query(args, mark_done_sql(args, lease_id))
        print(json.dumps({"iteration": iteration, "lease_id": lease_id, "status": "done"}, ensure_ascii=False), flush=True)

    print_status(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
