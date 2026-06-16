import json
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from airflow.decorators import dag, task


REFINEMENT_MODE = os.getenv(
    "ADS_LLM_SQL_REFINEMENT_MODE",
    os.getenv("LLM_LOG_REFINEMENT_MODE", "always"),
).strip().lower()
DAG_PAUSED = os.getenv(
    "AIRFLOW_LLM_SQL_REFINEMENT_PAUSED",
    os.getenv("LLM_LOG_REFINEMENT_DAG_PAUSED", "false"),
).strip().lower() == "true"
DAG_CRON = os.getenv("AIRFLOW_LLM_SQL_REFINEMENT_CRON", os.getenv("LLM_LOG_REFINEMENT_CRON", "manual")).strip()
DAG_SCHEDULE = None if DAG_CRON.lower() in {"", "manual", "none", "null"} else DAG_CRON

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "http://clickhouse:8123").rstrip("/")
if "://" not in CLICKHOUSE_HOST:
    CLICKHOUSE_HOST = f"http://{CLICKHOUSE_HOST}:8123"
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "analytics")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "analytics_password")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", os.getenv("CLICKHOUSE_DB", "analytics"))

KIMI_BASE_URL = os.getenv("KIMI_BASE_URL", "https://api.moonshot.ai/v1").rstrip("/")
KIMI_API_KEY = os.getenv("KIMI_API_KEY") or os.getenv("MOONSHOT_API_KEY", "")
KIMI_MODEL = os.getenv("KIMI_MODEL", os.getenv("KIMI_MODELS", "kimi-k2.6").split(",")[0].strip() or "kimi-k2.6")
KIMI_THINKING_TYPE = os.getenv("KIMI_THINKING_TYPE", "disabled")
KIMI_TEMPERATURE = float(os.getenv("KIMI_TEMPERATURE", "0.6"))

DEFAULT_SOURCE_TABLE = os.getenv("ADS_LLM_LOG_SOURCE_TABLE", os.getenv("LLM_LOG_SOURCE_TABLE", "analytics.elasticsearch_events_raw"))
DEFAULT_ROW_LIMIT = int(
    os.getenv(
        "ADS_LLM_LOG_BATCH_ROWS",
        os.getenv(
            "LLM_LOG_BATCH_ROWS",
            os.getenv("ADS_LLM_LOG_CHUNK_MAX_ROWS", os.getenv("LLM_LOG_CHUNK_ROW_LIMIT", "20")),
        ),
    )
)
DEFAULT_MAX_CHUNKS = int(os.getenv("ADS_LLM_LOG_MAX_CHUNKS", os.getenv("LLM_LOG_MAX_CHUNKS", "0")))
MIN_CHUNK_SECONDS = int(os.getenv("ADS_LLM_LOG_MIN_CHUNK_SECONDS", os.getenv("LLM_LOG_MIN_CHUNK_SECONDS", "30")))
MAX_RAW_CHARS_PER_CHUNK = int(
    os.getenv("ADS_LLM_LOG_MAX_RAW_CHARS_PER_CHUNK", os.getenv("LLM_LOG_MAX_RAW_CHARS_PER_CHUNK", "600000"))
)
DEFAULT_SOURCE_NAME = os.getenv("ADS_LLM_LOG_SOURCE_NAME", os.getenv("LLM_LOG_SOURCE_NAME", os.getenv("ELASTICSEARCH_SOURCE_NAME", "elasticsearch")))
DEFAULT_INDEX_LIKE = os.getenv("ADS_LLM_LOG_INDEX_LIKE", os.getenv("LLM_LOG_INDEX_LIKE", "nginx-logs-%"))
FINAL_REPORT_MAX_CHARS = int(os.getenv("LLM_LOG_FINAL_REPORT_MAX_CHARS", "180000"))
PROFILE_ENABLED = os.getenv("LLM_LOG_PROFILE_ENABLED", "true").strip().lower() == "true"
PROFILE_LIMIT = int(os.getenv("LLM_LOG_PROFILE_LIMIT", "50"))
RAW_CHUNK_MODE = os.getenv(
    "ADS_LLM_LOG_RAW_CHUNK_MODE",
    os.getenv("LLM_LOG_RAW_CHUNK_MODE", "full_period" if REFINEMENT_MODE == "always" else "sample"),
).strip().lower()
SQL_SHAPE_HINT = os.getenv(
    "LLM_LOG_SQL_SHAPE_HINT",
    "Prefer a concise analytical SELECT. Use CTEs only when they materially improve correctness.",
)
NO_INCIDENT_VALUES = [value.strip() for value in os.getenv("LLM_LOG_NO_INCIDENT_VALUES", "none,").split(",")]
FIELD_SEMANTICS = os.getenv(
    "LLM_LOG_FIELD_SEMANTICS",
    "Incident values listed in no_incident_values mean there is no declared incident. "
    "Log level values should be treated case-insensitively.",
)
RESULT_DATABASE = os.getenv("ADS_LLM_LOG_RESULT_DATABASE", os.getenv("LLM_LOG_RESULT_DATABASE", CLICKHOUSE_DATABASE))
INVESTIGATIONS_TABLE = f"{RESULT_DATABASE}.{os.getenv('ADS_LLM_LOG_INVESTIGATIONS_TABLE', os.getenv('LLM_LOG_INVESTIGATIONS_TABLE', 'llm_log_investigations'))}"
CHUNK_REPORTS_TABLE = f"{RESULT_DATABASE}.{os.getenv('ADS_LLM_LOG_CHUNK_REPORTS_TABLE', os.getenv('LLM_LOG_CHUNK_REPORTS_TABLE', 'llm_log_chunk_reports'))}"
REFINED_SQL_TABLE = f"{RESULT_DATABASE}.{os.getenv('ADS_LLM_LOG_REFINED_SQL_TABLE', os.getenv('LLM_LOG_REFINED_SQL_TABLE', 'llm_log_refined_sql'))}"
SQL_REPAIR_ATTEMPTS = int(os.getenv("LLM_LOG_SQL_REPAIR_ATTEMPTS", "2"))

DEFAULT_FIELD_EXPRESSIONS = {
    "service": "JSONExtractString(document_json, 'service')",
    "incident": "JSONExtractString(document_json, 'incident')",
    "level": "JSONExtractString(document_json, 'level')",
    "http_status": "JSONExtractInt(document_json, 'http', 'status_code')",
    "latency_ms": "JSONExtractInt(document_json, 'http', 'latency_ms')",
    "upstream_latency_ms": "JSONExtractInt(document_json, 'http', 'upstream_latency_ms')",
    "trace_id": "JSONExtractString(document_json, 'trace_id')",
}


def configured_field_expressions(conf=None):
    raw = (conf or {}).get("field_expressions_json") or os.getenv("LLM_LOG_FIELD_EXPRESSIONS_JSON")
    if not raw:
        return dict(DEFAULT_FIELD_EXPRESSIONS)
    parsed = json.loads(raw) if isinstance(raw, str) else raw
    fields = dict(DEFAULT_FIELD_EXPRESSIONS)
    fields.update({str(key): str(value) for key, value in parsed.items()})
    return fields


def field_instructions(fields):
    return "; ".join(f"{name}: {expr}" for name, expr in sorted(fields.items()))


def no_incident_sql(expr):
    values = ", ".join(ch_string(value) for value in NO_INCIDENT_VALUES)
    return f"{expr} NOT IN ({values})"


def semantic_instructions():
    return f"{FIELD_SEMANTICS} no_incident_values={NO_INCIDENT_VALUES}."


def ch_string(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def ch_datetime(value):
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def parse_time(value):
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def clickhouse_query(sql, database=CLICKHOUSE_DATABASE, timeout=240):
    params = urlencode({"database": database, "query": sql})
    request = Request(
        f"{CLICKHOUSE_HOST}/?{params}",
        headers={"X-ClickHouse-User": CLICKHOUSE_USER, "X-ClickHouse-Key": CLICKHOUSE_PASSWORD},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8")
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(detail or str(error)) from error


def insert_rows(table, rows):
    if not rows:
        return
    body = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
    params = urlencode({"database": CLICKHOUSE_DATABASE, "query": f"INSERT INTO {table} FORMAT JSONEachRow"})
    request = Request(
        f"{CLICKHOUSE_HOST}/?{params}",
        data=body.encode("utf-8"),
        headers={
            "X-ClickHouse-User": CLICKHOUSE_USER,
            "X-ClickHouse-Key": CLICKHOUSE_PASSWORD,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=240) as response:
            response.read()
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(detail or str(error)) from error


def ensure_tables():
    clickhouse_query(
        f"""
CREATE TABLE IF NOT EXISTS {INVESTIGATIONS_TABLE}
(
  investigation_id String,
  user_question String,
  source_table String,
  time_from DateTime64(3, 'UTC'),
  time_to DateTime64(3, 'UTC'),
  status LowCardinality(String),
  refined_sql String DEFAULT '',
  final_report String DEFAULT '',
  error String DEFAULT '',
  created_at DateTime64(3, 'UTC') DEFAULT now64(3),
  updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY investigation_id
"""
    )
    clickhouse_query(
        f"""
CREATE TABLE IF NOT EXISTS {CHUNK_REPORTS_TABLE}
(
  investigation_id String,
  chunk_id UInt32,
  chunk_from DateTime64(3, 'UTC'),
  chunk_to DateTime64(3, 'UTC'),
  rows_read UInt64,
  chars_read UInt64,
  kimi_summary_json String,
  candidate_filters_json String DEFAULT '',
  evidence_json String DEFAULT '',
  error String DEFAULT '',
  created_at DateTime64(3, 'UTC') DEFAULT now64(3),
  updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (investigation_id, chunk_id)
"""
    )
    clickhouse_query(
        f"""
CREATE TABLE IF NOT EXISTS {REFINED_SQL_TABLE}
(
  investigation_id String,
  refined_sql String,
  rationale String,
  confidence Float32 DEFAULT 0,
  validation_result String DEFAULT '',
  created_at DateTime64(3, 'UTC') DEFAULT now64(3),
  updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY investigation_id
"""
    )


def call_kimi(messages, temperature=None, timeout=240):
    if not KIMI_API_KEY:
        raise RuntimeError("KIMI_API_KEY is empty; cannot run LLM log refinement.")
    payload = {
        "model": KIMI_MODEL,
        "messages": messages,
        "temperature": KIMI_TEMPERATURE if temperature is None else temperature,
        "thinking": {"type": KIMI_THINKING_TYPE},
    }
    request = Request(
        f"{KIMI_BASE_URL}/chat/completions",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {KIMI_API_KEY}"},
        method="POST",
    )
    started = time.time()
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(detail or str(error)) from error
    payload = json.loads(raw)
    content = payload["choices"][0]["message"].get("content") or ""
    return {"content": content, "seconds": round(time.time() - started, 3), "usage": payload.get("usage") or {}}


def extract_json_object(text):
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```(?:json)?", "", stripped).strip()
        stripped = re.sub(r"```$", "", stripped).strip()
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start >= 0 and end > start:
        stripped = stripped[start : end + 1]
    return json.loads(stripped)


def fetch_log_rows(source_table, start, end, source_name, index_like, limit, extra_filter="", order_by="event_time, document_id"):
    sql = f"""
SELECT
  toString(event_time) AS event_time_text,
  index_name,
  document_id,
  document_json
FROM {source_table}
WHERE event_time >= toDateTime64({ch_string(ch_datetime(start))}, 3, 'UTC')
  AND event_time < toDateTime64({ch_string(ch_datetime(end))}, 3, 'UTC')
  AND source_name = {ch_string(source_name)}
  AND index_name LIKE {ch_string(index_like)}
{extra_filter}
ORDER BY {order_by}
LIMIT {int(limit)}
FORMAT JSONEachRow
"""
    raw = clickhouse_query(sql, timeout=300)
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def count_log_rows(source_table, start, end, source_name, index_like, extra_filter=""):
    sql = f"""
SELECT count() AS rows
FROM {source_table}
WHERE event_time >= toDateTime64({ch_string(ch_datetime(start))}, 3, 'UTC')
  AND event_time < toDateTime64({ch_string(ch_datetime(end))}, 3, 'UTC')
  AND source_name = {ch_string(source_name)}
  AND index_name LIKE {ch_string(index_like)}
{extra_filter}
FORMAT JSONEachRow
"""
    raw = clickhouse_query(sql, timeout=300)
    if not raw.strip():
        return 0
    return int(json.loads(raw.splitlines()[0]).get("rows") or 0)


def trim_rows_for_char_budget(rows, max_chars):
    if max_chars <= 0:
        return rows, False
    trimmed = []
    total = 0
    truncated = False
    for row in rows:
        row_chars = len(row.get("document_json") or "")
        if trimmed and total + row_chars > max_chars:
            truncated = True
            break
        trimmed.append(row)
        total += row_chars
    return trimmed, truncated


def fetch_json_each_row(sql, timeout=300):
    raw = clickhouse_query(f"{sql.rstrip()}\nFORMAT JSONEachRow", timeout=timeout)
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def period_filter(start, end, source_name, index_like):
    return f"""
event_time >= toDateTime64({ch_string(ch_datetime(start))}, 3, 'UTC')
  AND event_time < toDateTime64({ch_string(ch_datetime(end))}, 3, 'UTC')
  AND source_name = {ch_string(source_name)}
  AND index_name LIKE {ch_string(index_like)}
"""


def fetch_period_profile(source_table, start, end, source_name, index_like, fields, limit):
    where = period_filter(start, end, source_name, index_like)
    service_expr = fields.get("service", DEFAULT_FIELD_EXPRESSIONS["service"])
    incident_expr = fields.get("incident", DEFAULT_FIELD_EXPRESSIONS["incident"])
    level_expr = fields.get("level", DEFAULT_FIELD_EXPRESSIONS["level"])
    status_expr = fields.get("http_status", DEFAULT_FIELD_EXPRESSIONS["http_status"])
    latency_expr = fields.get("latency_ms", DEFAULT_FIELD_EXPRESSIONS["latency_ms"])
    upstream_expr = fields.get("upstream_latency_ms", DEFAULT_FIELD_EXPRESSIONS["upstream_latency_ms"])
    trace_expr = fields.get("trace_id", DEFAULT_FIELD_EXPRESSIONS["trace_id"])
    extracted = f"""
WITH extracted AS
(
  SELECT
    event_time,
    nullIf({service_expr}, '') AS service,
    nullIf({incident_expr}, '') AS incident,
    nullIf({level_expr}, '') AS level,
    {status_expr} AS status_code,
    {latency_expr} AS latency_ms,
    {upstream_expr} AS upstream_latency_ms,
    nullIf({trace_expr}, '') AS trace_id
  FROM {source_table}
  WHERE {where}
)
"""
    overview = fetch_json_each_row(
        f"""
{extracted}
SELECT
  count() AS rows,
  min(event_time) AS first_event_time,
  max(event_time) AS last_event_time,
  uniqExact(service) AS services,
  uniqExact(trace_id) AS unique_traces,
  countIf(status_code >= 500) AS http_5xx,
  countIf(status_code >= 400 AND status_code < 500) AS http_4xx,
  countIf(level IN ('warn', 'error', 'critical')) AS warn_or_error_logs,
  round(avg(latency_ms), 3) AS avg_latency_ms,
  quantile(0.95)(latency_ms) AS p95_latency_ms,
  quantile(0.99)(latency_ms) AS p99_latency_ms,
  round(avg(upstream_latency_ms), 3) AS avg_upstream_latency_ms
FROM extracted
""",
        timeout=300,
    )
    by_service = fetch_json_each_row(
        f"""
{extracted}
SELECT
  service,
  count() AS rows,
  countIf(status_code >= 500) AS http_5xx,
  countIf(status_code >= 400 AND status_code < 500) AS http_4xx,
  countIf(level IN ('warn', 'error', 'critical')) AS warn_or_error_logs,
  round(avg(latency_ms), 3) AS avg_latency_ms,
  quantile(0.99)(latency_ms) AS p99_latency_ms,
  round(avg(upstream_latency_ms), 3) AS avg_upstream_latency_ms,
  uniqExact(trace_id) AS unique_traces
FROM extracted
GROUP BY service
ORDER BY http_5xx DESC, p99_latency_ms DESC, rows DESC
LIMIT {int(limit)}
""",
        timeout=300,
    )
    incidents = fetch_json_each_row(
        f"""
{extracted}
SELECT
  incident,
  min(event_time) AS first_event_time,
  max(event_time) AS last_event_time,
  count() AS rows,
  countIf(status_code >= 500) AS http_5xx,
  groupArrayDistinct(service) AS services,
  quantile(0.99)(latency_ms) AS p99_latency_ms
FROM extracted
WHERE {no_incident_sql('incident')}
GROUP BY incident
ORDER BY first_event_time
LIMIT {int(limit)}
""",
        timeout=300,
    )
    hot_windows = fetch_json_each_row(
        f"""
{extracted}
SELECT
  toStartOfInterval(event_time, INTERVAL 1 HOUR) AS hour,
  service,
  count() AS rows,
  countIf(status_code >= 500) AS http_5xx,
  quantile(0.99)(latency_ms) AS p99_latency_ms,
  round(avg(upstream_latency_ms), 3) AS avg_upstream_latency_ms
FROM extracted
GROUP BY hour, service
HAVING http_5xx > 0 OR p99_latency_ms > 1000
ORDER BY http_5xx DESC, p99_latency_ms DESC
LIMIT {int(limit)}
""",
        timeout=300,
    )
    return {
        "overview": overview[0] if overview else {},
        "by_service": by_service,
        "incidents": incidents,
        "hot_windows": hot_windows,
        "field_expressions": fields,
    }


def chunk_specs(start, end, max_chunks, fields, conf=None):
    raw = (conf or {}).get("chunk_selectors_json") or os.getenv("LLM_LOG_CHUNK_SELECTORS_JSON")
    if raw:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
        return [
            {
                "label": str(item.get("label") or item.get("name") or f"selector_{index + 1}"),
                "start": parse_time(item["start"]) if item.get("start") else start,
                "end": parse_time(item["end"]) if item.get("end") else end,
                "filter": str(item.get("filter") or ""),
                "order_by": str(item.get("order_by") or "event_time, document_id"),
            }
            for index, item in enumerate(parsed)
        ][:max_chunks if max_chunks > 0 else None]

    source_table = (conf or {}).get("source_table") or DEFAULT_SOURCE_TABLE
    source_name = (conf or {}).get("source_name") or DEFAULT_SOURCE_NAME
    index_like = (conf or {}).get("index_like") or DEFAULT_INDEX_LIKE
    row_limit = int((conf or {}).get("batch_rows") or (conf or {}).get("chunk_row_limit") or DEFAULT_ROW_LIMIT)
    raw_chunk_mode = str((conf or {}).get("raw_chunk_mode") or RAW_CHUNK_MODE).strip().lower()
    if raw_chunk_mode in {"full", "full_period", "all"}:
        return full_period_chunk_specs(source_table, start, end, source_name, index_like, row_limit, max_chunks)

    incident_expr = fields.get("incident", DEFAULT_FIELD_EXPRESSIONS["incident"])
    status_expr = fields.get("http_status", DEFAULT_FIELD_EXPRESSIONS["http_status"])
    level_expr = fields.get("level", DEFAULT_FIELD_EXPRESSIONS["level"])
    specs = [
        {
            "label": "incident_rows",
            "start": start,
            "end": end,
            "filter": f"  AND {no_incident_sql(incident_expr)}",
            "order_by": "event_time, document_id",
        },
        {
            "label": "error_rows",
            "start": start,
            "end": end,
            "filter": (
                f"  AND ({status_expr} >= 500 "
                f"OR {level_expr} IN ('warn', 'error', 'critical'))"
            ),
            "order_by": "event_time, document_id",
        },
    ]
    if max_chunks > 0 and len(specs) >= max_chunks:
        return specs[:max_chunks]

    remaining = max(1, (max_chunks - len(specs)) if max_chunks > 0 else 4)
    total_seconds = max(1, int((end - start).total_seconds()))
    step = total_seconds / remaining
    for index in range(remaining):
        slice_start = start + timedelta(seconds=int(index * step))
        slice_end = end if index == remaining - 1 else start + timedelta(seconds=int((index + 1) * step))
        specs.append(
            {
                "label": f"time_slice_{index + 1}",
                "start": slice_start,
                "end": slice_end,
                "filter": "",
                "order_by": "event_time, document_id",
            }
        )
    return specs


def full_period_chunk_specs(source_table, start, end, source_name, index_like, row_limit, max_chunks):
    specs = []
    stack = [(start, end)]
    while stack:
        current_start, current_end = stack.pop(0)
        rows = count_log_rows(source_table, current_start, current_end, source_name, index_like)
        if rows <= 0:
            continue
        duration_seconds = max(1, int((current_end - current_start).total_seconds()))
        if rows > row_limit and duration_seconds > MIN_CHUNK_SECONDS:
            midpoint = current_start + timedelta(seconds=duration_seconds // 2)
            stack.insert(0, (midpoint, current_end))
            stack.insert(0, (current_start, midpoint))
            continue
        if max_chunks > 0 and len(specs) >= max_chunks:
            raise RuntimeError(
                "LLM log refinement reached max_chunks before covering the whole period. "
                "Increase ADS_LLM_LOG_MAX_CHUNKS or narrow the requested time range."
            )
        specs.append(
            {
                "label": f"full_period_{len(specs) + 1}",
                "start": current_start,
                "end": current_end,
                "filter": "",
                "order_by": "event_time, document_id",
                "expected_rows": rows,
            }
        )
    return specs


def chunk_prompt(user_question, chunk_id, rows, fields):
    raw_lines = []
    for row in rows:
        raw_lines.append(
            json.dumps(
                {
                    "event_time": row["event_time_text"],
                    "index_name": row["index_name"],
                    "document_id": row["document_id"],
                    "document": json.loads(row["document_json"]),
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
    return [
        {
            "role": "system",
            "content": (
                "You analyze raw web service logs to help refine a later ClickHouse SQL query. "
                "Return strict JSON only. Do not write prose outside JSON. "
                f"Configured ClickHouse field expressions: {field_instructions(fields)}. "
                f"Field semantics: {semantic_instructions()}"
            ),
        },
        {
            "role": "user",
            "content": (
                f"User question:\n{user_question}\n\n"
                f"Chunk id: {chunk_id}\n"
                "Analyze these raw Elasticsearch documents. Extract evidence useful for writing a precise ClickHouse SQL query.\n"
                "Return JSON with keys: summary, observed_patterns, suspected_services, suspected_incidents, "
                "candidate_filters, important_fields, evidence_examples, refined_sql_hints, confidence.\n\n"
                "Raw documents as JSON lines:\n"
                + "\n".join(raw_lines)
            ),
        },
    ]


def final_prompt(user_question, source_table, start, end, reports, fields, period_profile):
    reports_json = json.dumps(reports, ensure_ascii=False, separators=(",", ":"))
    if len(reports_json) > FINAL_REPORT_MAX_CHARS:
        reports_json = reports_json[:FINAL_REPORT_MAX_CHARS] + "\n...[TRUNCATED_REPORTS_FOR_FINAL_SYNTHESIS]"
    return [
        {
            "role": "system",
            "content": (
                "You generate one precise ClickHouse SELECT query after reading chunk reports from raw logs. "
                "Return strict JSON only. Do not use INSERT/UPDATE/DELETE/ALTER/DROP. "
                "The query must read from the provided source table and parse fields from document_json when needed. "
                f"Use only configured field expressions: {field_instructions(fields)}. "
                "If a SELECT groups rows, every selected non-aggregate expression must be in GROUP BY. "
                "Do not reference pre-aggregation aliases inside a grouped SELECT unless they are aggregated first. "
                f"SQL shape preference: {SQL_SHAPE_HINT} "
                f"Field semantics: {semantic_instructions()}"
            ),
        },
        {
            "role": "user",
            "content": (
                f"User question:\n{user_question}\n\n"
                f"Source table: {source_table}\n"
                f"Time range: {start} to {end}\n\n"
                "ClickHouse period profile JSON. Treat this as the compact statistical map of the whole period:\n"
                f"{json.dumps(period_profile, ensure_ascii=False, separators=(',', ':'))}\n\n"
                "Chunk reports JSON:\n"
                f"{reports_json}\n\n"
                "Return JSON with keys: refined_sql, rationale, confidence, expected_result_shape, grafana_hint."
            ),
        },
    ]


def repair_prompt(user_question, source_table, start, end, reports, fields, period_profile, invalid_sql, validation_error):
    reports_json = json.dumps(reports, ensure_ascii=False, separators=(",", ":"))
    if len(reports_json) > FINAL_REPORT_MAX_CHARS:
        reports_json = reports_json[:FINAL_REPORT_MAX_CHARS] + "\n...[TRUNCATED_REPORTS_FOR_REPAIR]"
    return [
        {
            "role": "system",
            "content": (
                "Repair an invalid ClickHouse SELECT query. Return strict JSON only with keys: "
                "refined_sql, rationale, confidence. Do not use INSERT/UPDATE/DELETE/ALTER/DROP. "
                "The repaired SQL must pass ClickHouse syntax and aggregate validation."
            ),
        },
        {
            "role": "user",
            "content": (
                f"User question:\n{user_question}\n\n"
                f"Source table: {source_table}\n"
                f"Time range: {start} to {end}\n"
                f"Configured field expressions: {field_instructions(fields)}\n\n"
                f"Field semantics: {semantic_instructions()}\n\n"
                "ClickHouse period profile JSON:\n"
                f"{json.dumps(period_profile, ensure_ascii=False, separators=(',', ':'))}\n\n"
                f"Invalid SQL:\n{invalid_sql}\n\n"
                f"ClickHouse validation error:\n{validation_error}\n\n"
                f"Chunk reports JSON:\n{reports_json}"
            ),
        },
    ]


def select_only(sql):
    text = sql.strip().rstrip(";")
    text = normalize_clickhouse_sql(text)
    lowered = text.lower()
    forbidden = [" insert ", " update ", " delete ", " alter ", " drop ", " truncate ", " create ", " system "]
    if not lowered.startswith("select") and not lowered.startswith("with"):
        raise ValueError("Refined SQL must start with SELECT or WITH")
    padded = f" {lowered} "
    if any(token in padded for token in forbidden):
        raise ValueError("Refined SQL contains a forbidden statement")
    return text


def normalize_clickhouse_sql(sql):
    text = sql.strip().rstrip(";")
    text = re.sub(
        r"toDateTime\('(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.\d+)?(?:Z|\+00:00)'\)",
        r"toDateTime64('\1 \2.000', 3, 'UTC')",
        text,
    )
    text = re.sub(
        r"'(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.\d+)?(?:Z|\+00:00)'",
        r"'\1 \2.000'",
        text,
    )
    text = re.sub(
        r"toStartOfFiveMinute\(([^)]+)\)",
        r"toStartOfInterval(\1, INTERVAL 5 MINUTE)",
        text,
        flags=re.IGNORECASE,
    )
    return text


def confidence_float(value):
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        match = re.search(r"\d+(?:\.\d+)?", value)
        if match:
            parsed = float(match.group(0))
            return parsed / 100 if parsed > 1 else parsed
    if isinstance(value, dict):
        for key in ("score", "value", "confidence"):
            if key in value:
                return confidence_float(value[key])
    return 0.0


@dag(
    dag_id="llm_guided_log_sql_refinement",
    description="Read raw Elasticsearch logs from ClickHouse in chunks, ask Kimi for chunk reports, and store refined SQL.",
    schedule=DAG_SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=DAG_PAUSED,
    tags=["agentic-data-stack", "kimi", "clickhouse", "elasticsearch", "logs"],
)
def llm_guided_log_sql_refinement():
    @task
    def run_refinement(**context):
        conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
        user_question = conf.get("question") or os.getenv("LLM_LOG_DEFAULT_QUESTION") or "Find the most likely root cause in the selected raw logs."
        source_table = conf.get("source_table") or DEFAULT_SOURCE_TABLE
        source_name = conf.get("source_name") or DEFAULT_SOURCE_NAME
        index_like = conf.get("index_like") or DEFAULT_INDEX_LIKE
        start = parse_time(conf.get("start") or os.getenv("LLM_LOG_START") or "2024-06-16T00:00:00Z")
        end = parse_time(conf.get("end") or os.getenv("LLM_LOG_END") or "2026-06-16T00:00:00Z")
        row_limit = int(conf.get("chunk_row_limit") or DEFAULT_ROW_LIMIT)
        max_chunks = int(conf.get("max_chunks") if conf.get("max_chunks") is not None else DEFAULT_MAX_CHUNKS)
        investigation_id = conf.get("investigation_id") or str(uuid.uuid4())
        fields = configured_field_expressions(conf)
        raw_chunk_mode = str(conf.get("raw_chunk_mode") or RAW_CHUNK_MODE).strip().lower()
        started_at = time.time()

        ensure_tables()
        insert_rows(
            INVESTIGATIONS_TABLE,
            [
                {
                    "investigation_id": investigation_id,
                    "user_question": user_question,
                    "source_table": source_table,
                    "time_from": ch_datetime(start),
                    "time_to": ch_datetime(end),
                    "status": "running",
                }
            ],
        )

        reports = []
        chunk_id = 0
        try:
            profile_started = time.time()
            period_profile = (
                fetch_period_profile(source_table, start, end, source_name, index_like, fields, PROFILE_LIMIT)
                if PROFILE_ENABLED
                else {}
            )
            if period_profile:
                period_profile["_profile_seconds"] = round(time.time() - profile_started, 3)
                reports.append({"chunk_id": 0, "chunk_selector": "clickhouse_period_profile", "report": period_profile})

            seen_documents = set()
            specs = [] if raw_chunk_mode in ("off", "none", "profile_only") else chunk_specs(start, end, max_chunks, fields, conf)
            for spec in specs:
                rows = fetch_log_rows(
                    source_table,
                    spec["start"],
                    spec["end"],
                    source_name,
                    index_like,
                    row_limit,
                    extra_filter=spec["filter"],
                    order_by=spec["order_by"],
                )
                rows = [row for row in rows if row["document_id"] not in seen_documents]
                rows, rows_truncated_by_char_budget = trim_rows_for_char_budget(rows, MAX_RAW_CHARS_PER_CHUNK)
                if not rows:
                    continue
                chunk_id += 1
                for row in rows:
                    seen_documents.add(row["document_id"])
                chunk_from = rows[0]["event_time_text"]
                chunk_to = rows[-1]["event_time_text"]
                chars_read = sum(len(row["document_json"]) for row in rows)
                try:
                    kimi = call_kimi(chunk_prompt(user_question, chunk_id, rows, fields))
                    parsed = extract_json_object(kimi["content"])
                    parsed["_kimi_seconds"] = kimi["seconds"]
                    parsed["_usage"] = kimi["usage"]
                    error = ""
                    summary_json = json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
                except Exception as exc:
                    parsed = {"error": str(exc)}
                    error = str(exc)
                    summary_json = json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
                reports.append(
                    {
                        "chunk_id": chunk_id,
                        "chunk_selector": spec["label"],
                        "expected_rows": spec.get("expected_rows"),
                        "chunk_from": chunk_from,
                        "chunk_to": chunk_to,
                        "rows_read": len(rows),
                        "rows_truncated_by_char_budget": rows_truncated_by_char_budget,
                        "report": parsed,
                    }
                )
                insert_rows(
                    CHUNK_REPORTS_TABLE,
                    [
                        {
                            "investigation_id": investigation_id,
                            "chunk_id": chunk_id,
                            "chunk_from": chunk_from,
                            "chunk_to": chunk_to,
                            "rows_read": len(rows),
                            "chars_read": chars_read,
                            "kimi_summary_json": summary_json,
                            "candidate_filters_json": json.dumps(parsed.get("candidate_filters", {}), ensure_ascii=False),
                            "evidence_json": json.dumps(parsed.get("evidence_examples", []), ensure_ascii=False),
                            "error": error,
                        }
                    ],
                )

            final = call_kimi(final_prompt(user_question, source_table, start.isoformat(), end.isoformat(), reports, fields, period_profile))
            final_json = extract_json_object(final["content"])
            final_json["_kimi_seconds"] = final["seconds"]
            final_json["_usage"] = final["usage"]
            final_json["_profile_seconds"] = period_profile.get("_profile_seconds", 0)
            final_json["_raw_chunk_mode"] = raw_chunk_mode
            final_json["_raw_chunks"] = chunk_id
            final_json["_batch_rows"] = row_limit
            refined_sql = select_only(final_json.get("refined_sql") or "")
            validation_result = ""
            for repair_attempt in range(SQL_REPAIR_ATTEMPTS + 1):
                try:
                    validation_result = clickhouse_query(f"EXPLAIN SYNTAX {refined_sql}", timeout=120)
                    break
                except Exception as exc:
                    if repair_attempt >= SQL_REPAIR_ATTEMPTS:
                        raise
                    repaired = call_kimi(
                        repair_prompt(
                            user_question,
                            source_table,
                            start.isoformat(),
                            end.isoformat(),
                            reports,
                            fields,
                            period_profile,
                            refined_sql,
                            str(exc),
                        )
                    )
                    repaired_json = extract_json_object(repaired["content"])
                    refined_sql = select_only(repaired_json.get("refined_sql") or "")
                    final_json = {
                        **final_json,
                        "refined_sql": refined_sql,
                        "rationale": repaired_json.get("rationale") or final_json.get("rationale") or "",
                        "confidence": repaired_json.get("confidence", final_json.get("confidence")),
                        "repair_attempts": repair_attempt + 1,
                        f"_repair_{repair_attempt + 1}_kimi_seconds": repaired["seconds"],
                    }
            final_json["_dag_total_seconds"] = round(time.time() - started_at, 3)
            insert_rows(
                REFINED_SQL_TABLE,
                [
                    {
                        "investigation_id": investigation_id,
                        "refined_sql": refined_sql,
                        "rationale": final_json.get("rationale") or "",
                        "confidence": confidence_float(final_json.get("confidence")),
                        "validation_result": validation_result,
                    }
                ],
            )
            insert_rows(
                INVESTIGATIONS_TABLE,
                [
                    {
                        "investigation_id": investigation_id,
                        "user_question": user_question,
                        "source_table": source_table,
                        "time_from": ch_datetime(start),
                        "time_to": ch_datetime(end),
                        "status": "completed",
                        "refined_sql": refined_sql,
                        "final_report": json.dumps(final_json, ensure_ascii=False),
                    }
                ],
            )
            return {
                "investigation_id": investigation_id,
                "chunks": chunk_id,
                "batch_rows": row_limit,
                "total_seconds": round(time.time() - started_at, 3),
                "refined_sql": refined_sql,
            }
        except Exception as exc:
            insert_rows(
                INVESTIGATIONS_TABLE,
                [
                    {
                        "investigation_id": investigation_id,
                        "user_question": user_question,
                        "source_table": source_table,
                        "time_from": ch_datetime(start),
                        "time_to": ch_datetime(end),
                        "status": "failed",
                        "error": str(exc),
                    }
                ],
            )
            raise

    run_refinement()


llm_guided_log_sql_refinement()
