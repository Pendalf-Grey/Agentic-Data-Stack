import base64
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
from airflow.exceptions import AirflowFailException


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
KIMI_REASONING_PARAMS_ENABLED = os.getenv("KIMI_REASONING_PARAMS_ENABLED", "auto").strip().lower()
KIMI_REASONING_EFFORT = os.getenv("KIMI_REASONING_EFFORT", "none")
KIMI_INCLUDE_REASONING = os.getenv("KIMI_INCLUDE_REASONING", "false").strip().lower() in {"1", "true", "yes", "on"}
KIMI_REASONING_EXCLUDE = os.getenv("KIMI_REASONING_EXCLUDE", "true").strip().lower() in {"1", "true", "yes", "on"}

DEFAULT_SOURCE_TABLE = os.getenv("ADS_LLM_LOG_SOURCE_TABLE", os.getenv("LLM_LOG_SOURCE_TABLE", "analytics.elasticsearch_events_raw"))
DEFAULT_ROW_LIMIT = int(
    os.getenv(
        "ADS_LLM_LOG_BATCH_ROWS",
        os.getenv(
            "LLM_LOG_BATCH_ROWS",
            os.getenv("ADS_LLM_LOG_CHUNK_MAX_ROWS", os.getenv("LLM_LOG_CHUNK_ROW_LIMIT", "5")),
        ),
    )
)
DEFAULT_MAX_CHUNKS = int(os.getenv("ADS_LLM_LOG_MAX_CHUNKS", os.getenv("LLM_LOG_MAX_CHUNKS", "0")))
MAX_RAW_CHARS_PER_CHUNK = int(
    os.getenv("ADS_LLM_LOG_MAX_RAW_CHARS_PER_CHUNK", os.getenv("LLM_LOG_MAX_RAW_CHARS_PER_CHUNK", "600000"))
)
DEFAULT_SOURCE_NAME = os.getenv("ADS_LLM_LOG_SOURCE_NAME", os.getenv("LLM_LOG_SOURCE_NAME", os.getenv("ELASTICSEARCH_SOURCE_NAME", "elasticsearch")))
DEFAULT_INDEX_LIKE = os.getenv("ADS_LLM_LOG_INDEX_LIKE", os.getenv("LLM_LOG_INDEX_LIKE", "nginx-logs-%"))
FINAL_REPORT_MAX_CHARS = int(os.getenv("LLM_LOG_FINAL_REPORT_MAX_CHARS", "180000"))
PROFILE_ENABLED = os.getenv("LLM_LOG_PROFILE_ENABLED", "true").strip().lower() == "true"
PROFILE_LIMIT = int(os.getenv("LLM_LOG_PROFILE_LIMIT", "50"))
ANALYSIS_STRATEGY = os.getenv(
    "ADS_LLM_LOG_ANALYSIS_STRATEGY",
    os.getenv("LLM_LOG_ANALYSIS_STRATEGY", "context"),
).strip().lower()
RAW_CHUNK_MODE = os.getenv(
    "ADS_LLM_LOG_RAW_CHUNK_MODE",
    os.getenv("LLM_LOG_RAW_CHUNK_MODE", "full_period" if REFINEMENT_MODE == "always" else "sample"),
).strip().lower()
CONTEXT_WINDOW_LIMIT = int(os.getenv("ADS_LLM_CONTEXT_WINDOW_LIMIT", os.getenv("LLM_LOG_CONTEXT_WINDOW_LIMIT", "120")))
CONTEXT_RARE_EVENT_LIMIT = int(os.getenv("ADS_LLM_CONTEXT_RARE_EVENT_LIMIT", os.getenv("LLM_LOG_CONTEXT_RARE_EVENT_LIMIT", "80")))
CONTEXT_TOP_TRACE_LIMIT = int(os.getenv("ADS_LLM_CONTEXT_TOP_TRACE_LIMIT", os.getenv("LLM_LOG_CONTEXT_TOP_TRACE_LIMIT", "60")))
CONTEXT_RAW_SAMPLE_LIMIT = int(os.getenv("ADS_LLM_CONTEXT_RAW_SAMPLE_LIMIT", os.getenv("LLM_LOG_CONTEXT_RAW_SAMPLE_LIMIT", "80")))
CONTEXT_RAW_SAMPLE_CHARS = int(os.getenv("ADS_LLM_CONTEXT_RAW_SAMPLE_CHARS", os.getenv("LLM_LOG_CONTEXT_RAW_SAMPLE_CHARS", "2500")))
CONTEXT_SLOW_LATENCY_MS = int(os.getenv("ADS_LLM_CONTEXT_SLOW_LATENCY_MS", os.getenv("LLM_LOG_CONTEXT_SLOW_LATENCY_MS", "1000")))
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


def env_bool(name, default=False):
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


LANGFUSE_AIRFLOW_TRACING_ENABLED = env_bool("LANGFUSE_AIRFLOW_TRACING_ENABLED", env_bool("LANGFUSE_ENABLED", False))
LANGFUSE_BASE_URL = os.getenv(
    "LANGFUSE_BASE_URL",
    os.getenv("LANGFUSE_INTERNAL_URL", "http://langfuse-web:3000"),
).rstrip("/")
LANGFUSE_PUBLIC_KEY = os.getenv("LANGFUSE_PUBLIC_KEY", "")
LANGFUSE_SECRET_KEY = os.getenv("LANGFUSE_SECRET_KEY", "")
LANGFUSE_ENVIRONMENT = os.getenv("LANGFUSE_ENVIRONMENT", "local")
LANGFUSE_TRACE_INPUT_MAX_CHARS = int(os.getenv("LANGFUSE_TRACE_INPUT_MAX_CHARS", "20000"))
LANGFUSE_TRACE_OUTPUT_MAX_CHARS = int(os.getenv("LANGFUSE_TRACE_OUTPUT_MAX_CHARS", "40000"))

DEFAULT_FIELD_EXPRESSIONS = {
    "service": "JSONExtractString(document_json, 'service')",
    "incident": "JSONExtractString(document_json, 'incident')",
    "level": "JSONExtractString(document_json, 'level')",
    "host": "JSONExtractString(document_json, 'host')",
    "message": "JSONExtractString(document_json, 'message')",
    "error_code": "JSONExtractString(document_json, 'error_code')",
    "release": "JSONExtractString(document_json, 'release')",
    "http_method": "if(JSONExtractString(document_json, 'http', 'method') != '', JSONExtractString(document_json, 'http', 'method'), JSONExtractString(document_json, 'nginx', 'method'))",
    "http_path": "if(JSONExtractString(document_json, 'http', 'path') != '', JSONExtractString(document_json, 'http', 'path'), JSONExtractString(document_json, 'nginx', 'path'))",
    "http_status": "if(JSONExtractInt(document_json, 'http', 'status_code') != 0, JSONExtractInt(document_json, 'http', 'status_code'), JSONExtractInt(document_json, 'nginx', 'status'))",
    "latency_ms": "if(JSONExtractInt(document_json, 'http', 'latency_ms') != 0, JSONExtractInt(document_json, 'http', 'latency_ms'), toInt64(round(JSONExtractFloat(document_json, 'nginx', 'request_time') * 1000)))",
    "upstream_latency_ms": "if(JSONExtractInt(document_json, 'http', 'upstream_latency_ms') != 0, JSONExtractInt(document_json, 'http', 'upstream_latency_ms'), toInt64(round(JSONExtractFloat(document_json, 'nginx', 'upstream_response_time') * 1000)))",
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
    return f"ifNull(toString({expr}), '') NOT IN ({values})"


def semantic_instructions():
    return f"{FIELD_SEMANTICS} no_incident_values={NO_INCIDENT_VALUES}."


def analysis_strategy_name(conf=None):
    # Основной переключатель архитектуры:
    # context - ClickHouse строит полный аналитический контекст периода;
    # raw_chunks - Kimi последовательно читает batch'и сырых строк.
    return str((conf or {}).get("analysis_strategy") or ANALYSIS_STRATEGY or "context").strip().lower()


def is_context_strategy(value):
    # Поддерживаем несколько человекочитаемых имен одного режима,
    # чтобы менять стратегию из .env или MCP-вызова без правки DAG.
    return value in {"context", "analytical_context", "profile_guided", "context_first"}


def context_window_interval(start, end, conf=None):
    # Автоокно выбирается по длине периода: для двух лет день, для недель часы,
    # для коротких расследований 15 минут. Можно переопределить через .env/conf.
    configured = str(
        (conf or {}).get("context_window_interval")
        or os.getenv("ADS_LLM_CONTEXT_WINDOW_INTERVAL")
        or os.getenv("LLM_LOG_CONTEXT_WINDOW_INTERVAL")
        or "auto"
    ).strip()
    if configured and configured.lower() != "auto":
        return configured.upper()
    days = max(1, int((end - start).total_seconds() // 86400) + 1)
    if days > 90:
        return "1 DAY"
    if days > 7:
        return "1 HOUR"
    return "15 MINUTE"


def interesting_log_condition(slow_latency_ms):
    # Candidate events для raw-доказательств: ошибки, warn/error уровни,
    # объявленные инциденты и заметно медленные запросы. Все пороги настраиваются.
    return (
        "(status_code >= 400 "
        "OR lower(ifNull(level, '')) IN ('warn', 'error', 'critical', 'fatal') "
        f"OR {no_incident_sql('incident')} "
        f"OR latency_ms >= {int(slow_latency_ms)} "
        f"OR upstream_latency_ms >= {int(slow_latency_ms)})"
    )


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


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def clamp_text(value, max_chars):
    if value is None:
        return value
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    if max_chars > 0 and len(text) > max_chars:
        return text[:max_chars] + "\n...[TRUNCATED_FOR_LANGFUSE]"
    return text


def langfuse_trace_id(investigation_id):
    # Langfuse хорошо работает с hex-id. Делаем детерминированный id, чтобы
    # все Airflow generation по одному расследованию попадали в один trace.
    return uuid.uuid5(uuid.NAMESPACE_URL, f"ads-log-refinement:{investigation_id}").hex


def langfuse_ingest(events):
    # Наблюдаемость не должна ломать сам анализ: если Langfuse недоступен,
    # DAG продолжает работу, а проблему видно по логам контейнера Airflow.
    if not (LANGFUSE_AIRFLOW_TRACING_ENABLED and LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY):
        return
    body = {
        "batch": events,
        "metadata": {
            "batch_size": len(events),
            "sdk_name": "agentic-data-stack-airflow",
            "sdk_version": "local",
            "sdk_variant": "python-stdlib",
            "sdk_integration": "airflow-dag",
            "public_key": LANGFUSE_PUBLIC_KEY,
        },
    }
    token = base64.b64encode(f"{LANGFUSE_PUBLIC_KEY}:{LANGFUSE_SECRET_KEY}".encode("utf-8")).decode("ascii")
    request = Request(
        f"{LANGFUSE_BASE_URL}/api/public/ingestion",
        data=json.dumps(body, ensure_ascii=False, default=str).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {token}",
            "X-Langfuse-Public-Key": LANGFUSE_PUBLIC_KEY,
            "X-Langfuse-Sdk-Name": "agentic-data-stack-airflow",
            "X-Langfuse-Sdk-Version": "local",
            "X-Langfuse-Sdk-Variant": "python-stdlib",
            "X-Langfuse-Sdk-Integration": "airflow-dag",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=15) as response:
            response.read()
    except Exception as exc:
        print(f"[langfuse] ingestion skipped after error: {type(exc).__name__}: {exc}")


def langfuse_trace_create(trace_id, investigation_id, user_question, start, end, metadata):
    if not (LANGFUSE_AIRFLOW_TRACING_ENABLED and LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY):
        return
    langfuse_ingest(
        [
            {
                "id": str(uuid.uuid4()),
                "type": "trace-create",
                "timestamp": utc_now_iso(),
                "body": {
                    "id": trace_id,
                    "name": "Airflow LLM log refinement",
                    "timestamp": utc_now_iso(),
                    "environment": LANGFUSE_ENVIRONMENT,
                    "sessionId": investigation_id,
                    "userId": "airflow",
                    "input": clamp_text(
                        {
                            "question": user_question,
                            "time_from": start.isoformat(),
                            "time_to": end.isoformat(),
                        },
                        LANGFUSE_TRACE_INPUT_MAX_CHARS,
                    ),
                    "metadata": metadata,
                },
            }
        ]
    )


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


def call_kimi(messages, temperature=None, timeout=240, trace_id=None, generation_name="AirflowKimi", metadata=None, input_summary=None):
    if not KIMI_API_KEY:
        raise RuntimeError("KIMI_API_KEY is empty; cannot run LLM log refinement.")
    payload = {
        "model": KIMI_MODEL,
        "messages": messages,
        "temperature": KIMI_TEMPERATURE if temperature is None else temperature,
        "thinking": {"type": KIMI_THINKING_TYPE},
    }
    reasoning_enabled = (
        KIMI_REASONING_PARAMS_ENABLED == "true"
        or (KIMI_REASONING_PARAMS_ENABLED == "auto" and "openrouter.ai" in KIMI_BASE_URL)
    )
    if reasoning_enabled:
        payload.update(
            {
                "include_reasoning": KIMI_INCLUDE_REASONING,
                "reasoning_effort": KIMI_REASONING_EFFORT,
                "reasoning": {
                    "effort": KIMI_REASONING_EFFORT,
                    "exclude": KIMI_REASONING_EXCLUDE,
                },
            }
        )
    request = Request(
        f"{KIMI_BASE_URL}/chat/completions",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {KIMI_API_KEY}",
            "HTTP-Referer": "http://localhost:3080",
            "X-Title": "Agentic Data Stack",
        },
        method="POST",
    )
    started = time.time()
    started_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(detail or str(error)) from error
    ended_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    payload = json.loads(raw)
    content = payload["choices"][0]["message"].get("content") or ""
    usage = payload.get("usage") or {}
    if trace_id:
        langfuse_ingest(
            [
                {
                    "id": str(uuid.uuid4()),
                    "type": "generation-create",
                    "timestamp": ended_iso,
                    "body": {
                        "id": str(uuid.uuid4()),
                        "traceId": trace_id,
                        "name": generation_name,
                        "model": KIMI_MODEL,
                        "startTime": started_iso,
                        "endTime": ended_iso,
                        "environment": LANGFUSE_ENVIRONMENT,
                        "input": clamp_text(input_summary or messages, LANGFUSE_TRACE_INPUT_MAX_CHARS),
                        "output": clamp_text(content, LANGFUSE_TRACE_OUTPUT_MAX_CHARS),
                        "usage": usage,
                        "metadata": {
                            "base_url": KIMI_BASE_URL,
                            "seconds": round(time.time() - started, 3),
                            **(metadata or {}),
                        },
                    },
                }
            ]
        )
    return {"content": content, "seconds": round(time.time() - started, 3), "usage": usage}


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


def fetch_log_rows(
    source_table,
    start,
    end,
    source_name,
    index_like,
    limit,
    extra_filter="",
    order_by="event_time, document_id",
    offset=0,
):
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
OFFSET {int(offset)}
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
  uniq(service) AS services,
  uniq(trace_id) AS unique_traces,
  countIf(status_code >= 500) AS http_5xx,
  countIf(status_code >= 400 AND status_code < 500) AS http_4xx,
  countIf(lower(ifNull(level, '')) IN ('warn', 'error', 'critical', 'fatal')) AS warn_or_error_logs,
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
  countIf(lower(ifNull(level, '')) IN ('warn', 'error', 'critical', 'fatal')) AS warn_or_error_logs,
  round(avg(latency_ms), 3) AS avg_latency_ms,
  quantile(0.99)(latency_ms) AS p99_latency_ms,
  round(avg(upstream_latency_ms), 3) AS avg_upstream_latency_ms,
  uniq(trace_id) AS unique_traces
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
HAVING http_5xx > 0 OR p99_latency_ms >= {CONTEXT_SLOW_LATENCY_MS}
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


def fetch_analytical_context(source_table, start, end, source_name, index_like, fields, limit, conf=None):
    # Context-first режим: ClickHouse читает все matching rows за период и
    # строит карту расследования. Kimi получает эту карту и несколько raw-
    # фрагментов как доказательства, а не весь поток логов построчно.
    where = period_filter(start, end, source_name, index_like)
    service_expr = fields.get("service", DEFAULT_FIELD_EXPRESSIONS["service"])
    incident_expr = fields.get("incident", DEFAULT_FIELD_EXPRESSIONS["incident"])
    level_expr = fields.get("level", DEFAULT_FIELD_EXPRESSIONS["level"])
    status_expr = fields.get("http_status", DEFAULT_FIELD_EXPRESSIONS["http_status"])
    latency_expr = fields.get("latency_ms", DEFAULT_FIELD_EXPRESSIONS["latency_ms"])
    upstream_expr = fields.get("upstream_latency_ms", DEFAULT_FIELD_EXPRESSIONS["upstream_latency_ms"])
    trace_expr = fields.get("trace_id", DEFAULT_FIELD_EXPRESSIONS["trace_id"])
    host_expr = fields.get("host", DEFAULT_FIELD_EXPRESSIONS["host"])
    message_expr = fields.get("message", DEFAULT_FIELD_EXPRESSIONS["message"])
    error_code_expr = fields.get("error_code", DEFAULT_FIELD_EXPRESSIONS["error_code"])
    release_expr = fields.get("release", DEFAULT_FIELD_EXPRESSIONS["release"])
    method_expr = fields.get("http_method", DEFAULT_FIELD_EXPRESSIONS["http_method"])
    path_expr = fields.get("http_path", DEFAULT_FIELD_EXPRESSIONS["http_path"])
    window_interval = context_window_interval(start, end, conf)
    window_limit = int((conf or {}).get("context_window_limit") or CONTEXT_WINDOW_LIMIT)
    rare_limit = int((conf or {}).get("context_rare_event_limit") or CONTEXT_RARE_EVENT_LIMIT)
    trace_limit = int((conf or {}).get("context_top_trace_limit") or CONTEXT_TOP_TRACE_LIMIT)
    sample_limit = int((conf or {}).get("context_raw_sample_limit") or CONTEXT_RAW_SAMPLE_LIMIT)
    sample_chars = int((conf or {}).get("context_raw_sample_chars") or CONTEXT_RAW_SAMPLE_CHARS)
    slow_latency_ms = int((conf or {}).get("context_slow_latency_ms") or CONTEXT_SLOW_LATENCY_MS)
    candidate_condition = interesting_log_condition(slow_latency_ms)
    extracted = f"""
WITH extracted AS
(
  SELECT
    event_time,
    index_name,
    document_id,
    document_json,
    nullIf({service_expr}, '') AS service,
    nullIf({host_expr}, '') AS host,
    nullIf({incident_expr}, '') AS incident,
    nullIf({level_expr}, '') AS level,
    nullIf({message_expr}, '') AS message,
    nullIf({error_code_expr}, '') AS error_code,
    nullIf({release_expr}, '') AS release,
    nullIf({method_expr}, '') AS http_method,
    nullIf({path_expr}, '') AS http_path,
    {status_expr} AS status_code,
    {latency_expr} AS latency_ms,
    {upstream_expr} AS upstream_latency_ms,
    nullIf({trace_expr}, '') AS trace_id
  FROM {source_table}
  WHERE {where}
)
"""

    base_profile = fetch_period_profile(source_table, start, end, source_name, index_like, fields, limit)
    by_status = fetch_json_each_row(
        f"""
{extracted}
SELECT
  multiIf(status_code >= 500, '5xx', status_code >= 400, '4xx', status_code >= 300, '3xx', status_code >= 200, '2xx', 'other') AS status_family,
  status_code,
  count() AS rows,
  uniq(service) AS services,
  uniq(trace_id) AS unique_traces,
  quantile(0.99)(latency_ms) AS p99_latency_ms
FROM extracted
GROUP BY status_family, status_code
ORDER BY status_family DESC, rows DESC
LIMIT {int(limit)}
""",
        timeout=300,
    )
    by_level = fetch_json_each_row(
        f"""
{extracted}
SELECT
  lower(ifNull(level, '')) AS level,
  count() AS rows,
  countIf(status_code >= 500) AS http_5xx,
  uniq(service) AS services,
  uniq(trace_id) AS unique_traces,
  quantile(0.99)(latency_ms) AS p99_latency_ms
FROM extracted
GROUP BY level
ORDER BY rows DESC
LIMIT {int(limit)}
""",
        timeout=300,
    )
    time_windows = fetch_json_each_row(
        f"""
{extracted}
SELECT
  toStartOfInterval(event_time, INTERVAL {window_interval}) AS window_start,
  count() AS rows,
  countIf(status_code >= 500) AS http_5xx,
  countIf(status_code >= 400 AND status_code < 500) AS http_4xx,
  countIf(lower(ifNull(level, '')) IN ('warn', 'error', 'critical', 'fatal')) AS warn_or_error_logs,
  uniq(service) AS services,
  uniq(trace_id) AS unique_traces,
  quantile(0.95)(latency_ms) AS p95_latency_ms,
  quantile(0.99)(latency_ms) AS p99_latency_ms
FROM extracted
GROUP BY window_start
ORDER BY window_start
LIMIT {int(window_limit)}
""",
        timeout=300,
    )
    rare_events = fetch_json_each_row(
        f"""
{extracted}
SELECT
  service,
  lower(ifNull(level, '')) AS level,
  status_code,
  error_code,
  incident,
  message,
  release,
  min(event_time) AS first_event_time,
  max(event_time) AS last_event_time,
  count() AS rows,
  uniq(trace_id) AS unique_traces,
  quantile(0.99)(latency_ms) AS p99_latency_ms
FROM extracted
WHERE {candidate_condition}
GROUP BY service, level, status_code, error_code, incident, message, release
ORDER BY rows ASC, status_code DESC, p99_latency_ms DESC
LIMIT {int(rare_limit)}
""",
        timeout=300,
    )
    top_traces = fetch_json_each_row(
        f"""
{extracted}
SELECT
  trace_id,
  min(event_time) AS first_event_time,
  max(event_time) AS last_event_time,
  count() AS rows,
  countIf(status_code >= 500) AS http_5xx,
  countIf(status_code >= 400 AND status_code < 500) AS http_4xx,
  groupArrayDistinct(service) AS services,
  groupArrayDistinct(error_code) AS error_codes,
  groupArrayDistinct(incident) AS incidents,
  max(latency_ms) AS max_latency_ms,
  quantile(0.99)(latency_ms) AS p99_latency_ms
FROM extracted
WHERE trace_id IS NOT NULL
  AND {candidate_condition}
GROUP BY trace_id
ORDER BY http_5xx DESC, http_4xx DESC, max_latency_ms DESC, rows DESC
LIMIT {int(trace_limit)}
""",
        timeout=300,
    )
    release_summary = fetch_json_each_row(
        f"""
{extracted}
SELECT
  service,
  release,
  min(event_time) AS first_event_time,
  max(event_time) AS last_event_time,
  count() AS rows,
  countIf(status_code >= 500) AS http_5xx,
  countIf(status_code >= 400 AND status_code < 500) AS http_4xx,
  quantile(0.99)(latency_ms) AS p99_latency_ms
FROM extracted
GROUP BY service, release
HAVING http_5xx > 0 OR http_4xx > 0 OR p99_latency_ms >= {int(slow_latency_ms)}
ORDER BY http_5xx DESC, http_4xx DESC, p99_latency_ms DESC
LIMIT {int(limit)}
""",
        timeout=300,
    )
    raw_error_samples = fetch_json_each_row(
        f"""
{extracted}
SELECT
  toString(event_time) AS event_time,
  index_name,
  document_id,
  service,
  host,
  level,
  status_code,
  latency_ms,
  upstream_latency_ms,
  trace_id,
  incident,
  error_code,
  release,
  message,
  http_method,
  http_path,
  substring(document_json, 1, {int(sample_chars)}) AS document_preview
FROM extracted
WHERE {candidate_condition}
ORDER BY status_code DESC, latency_ms DESC, event_time
LIMIT {int(sample_limit)}
""",
        timeout=300,
    )
    raw_latency_samples = fetch_json_each_row(
        f"""
{extracted}
SELECT
  toString(event_time) AS event_time,
  index_name,
  document_id,
  service,
  host,
  level,
  status_code,
  latency_ms,
  upstream_latency_ms,
  trace_id,
  incident,
  error_code,
  release,
  message,
  http_method,
  http_path,
  substring(document_json, 1, {int(sample_chars)}) AS document_preview
FROM extracted
WHERE {candidate_condition}
ORDER BY latency_ms DESC, upstream_latency_ms DESC, event_time
LIMIT {int(sample_limit)}
""",
        timeout=300,
    )
    # Важно: raw_fragments ниже не являются всей выборкой. Все счетчики,
    # квантили, окна и top traces выше посчитаны ClickHouse по полному периоду.
    # Raw-фрагменты нужны Kimi для понимания реальных сообщений и структуры JSON.
    context = {
        **base_profile,
        "analysis_strategy": "context",
        "context_contract": {
            "full_period_scanned_by": "ClickHouse",
            "llm_receives": "aggregated analytical context plus selected raw fragments",
            "raw_fragments_are_samples": True,
            "counts_and_quantiles_use_all_matching_rows": True,
        },
        "limits": {
            "profile_limit": limit,
            "window_limit": window_limit,
            "rare_event_limit": rare_limit,
            "top_trace_limit": trace_limit,
            "raw_sample_limit": sample_limit,
            "raw_sample_chars": sample_chars,
            "slow_latency_ms": slow_latency_ms,
        },
        "window_interval": window_interval,
        "by_status": by_status,
        "by_level": by_level,
        "time_windows": time_windows,
        "rare_events": rare_events,
        "top_traces": top_traces,
        "release_summary": release_summary,
        "raw_fragments": {
            "error_samples": raw_error_samples,
            "high_latency_samples": raw_latency_samples,
        },
    }
    return context


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
    # full_period обязан покрывать весь период, но без рекурсивного дробления
    # времени: иначе preflight в MCP считает одно число batch'ей, а DAG делает
    # больше Kimi-вызовов. Поэтому для полного периода строим предсказуемые
    # страницы по стабильному ORDER BY event_time, document_id.
    total_rows = count_log_rows(source_table, start, end, source_name, index_like)
    if total_rows <= 0:
        return []

    total_chunks = (total_rows + row_limit - 1) // row_limit
    if max_chunks > 0 and total_chunks > max_chunks:
        raise RuntimeError(
            "LLM log refinement reached max_chunks before covering the whole period. "
            "Increase ADS_LLM_LOG_MAX_CHUNKS or narrow the requested time range."
        )

    return [
        {
            "label": f"full_period_{index + 1}",
            "start": start,
            "end": end,
            "filter": "",
            "order_by": "event_time, document_id",
            "offset": index * row_limit,
            "limit": min(row_limit, total_rows - index * row_limit),
            "expected_rows": min(row_limit, total_rows - index * row_limit),
        }
        for index in range(total_chunks)
    ]

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
                "You generate one precise ClickHouse SELECT query after reading ADS log-analysis context. "
                "Return strict JSON only. Do not use INSERT/UPDATE/DELETE/ALTER/DROP. "
                "The query must read from the provided source table and parse fields from document_json when needed. "
                f"Use only configured field expressions: {field_instructions(fields)}. "
                "If the context says full_period_scanned_by=ClickHouse, then counts, frequencies, windows, "
                "quantiles, rare events, and top traces were computed over the whole matching period. "
                "Selected raw fragments are evidence samples, not the whole population; do not treat sample size as total row count. "
                "ClickHouse aggregate rules: countIf takes exactly one boolean condition; avgIf takes value and condition; "
                "quantileIf takes value and condition. Do not pass extra arguments into countIf. "
                "If a SELECT groups rows, every selected non-aggregate expression must be in GROUP BY. "
                "Do not reference pre-aggregation aliases inside a grouped SELECT unless they are aggregated first. "
                "Do not group by aggregate aliases or boolean aliases derived from aggregate conditions. "
                "Avoid NaN in user-facing aggregates: if a conditional average can be empty, use nullable-safe expressions or group filters that match the aggregate condition. "
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
                "ClickHouse analytical context JSON. Treat this as the compact statistical map of the whole period:\n"
                f"{json.dumps(period_profile, ensure_ascii=False, separators=(',', ':'))}\n\n"
                "Context/raw chunk reports JSON:\n"
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
                "The repaired SQL must pass ClickHouse syntax and aggregate validation. "
                "ClickHouse aggregate rules: countIf(condition), avgIf(value, condition), quantileIf(level)(value, condition). "
                "Do not group by aggregate aliases; do not return NaN-producing conditional aggregates."
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
                "ClickHouse analytical context JSON:\n"
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
    description="Build ClickHouse log-analysis context, ask Kimi for refined SQL, and store the result.",
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
        strategy = analysis_strategy_name(conf)
        raw_chunk_mode = str(conf.get("raw_chunk_mode") or RAW_CHUNK_MODE).strip().lower()
        started_at = time.time()
        dag_run = context.get("dag_run")
        trace_id = langfuse_trace_id(investigation_id)

        def ensure_not_cancelled(stage):
            # mcp-airflow по timeout переводит DagRun в failed. Running task сам
            # проверяет это состояние между batch'ами, чтобы не продолжать Kimi
            # вызовы в фоне после того, как LibreChat уже получил timeout.
            if not dag_run:
                return
            try:
                dag_run.refresh_from_db()
                state = str(dag_run.state or "").lower()
            except Exception:
                return
            if state == "failed":
                raise AirflowFailException(f"DagRun was cancelled externally during {stage}; stopping Kimi refinement.")

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
        langfuse_trace_create(
            trace_id,
            investigation_id,
            user_question,
            start,
            end,
            {
                "source_table": source_table,
                "source_name": source_name,
                "index_like": index_like,
                "analysis_strategy": strategy,
                "raw_chunk_mode": raw_chunk_mode,
                "batch_rows": row_limit,
                "max_chunks": max_chunks,
            },
        )

        reports = []
        chunk_id = 0
        try:
            ensure_not_cancelled("ClickHouse context build")
            profile_started = time.time()
            if is_context_strategy(strategy):
                period_profile = fetch_analytical_context(
                    source_table,
                    start,
                    end,
                    source_name,
                    index_like,
                    fields,
                    PROFILE_LIMIT,
                    conf,
                )
                period_profile["_profile_seconds"] = round(time.time() - profile_started, 3)
                context_json = json.dumps(period_profile, ensure_ascii=False, separators=(",", ":"))
                overview = period_profile.get("overview") or {}
                reports.append(
                    {
                        "chunk_id": 0,
                        "chunk_selector": "clickhouse_analytical_context",
                        "report": {
                            "analysis_strategy": strategy,
                            "context_contract": period_profile.get("context_contract"),
                            "overview": overview,
                            "sections": sorted(period_profile.keys()),
                            "limits": period_profile.get("limits"),
                            "profile_seconds": period_profile.get("_profile_seconds"),
                        },
                    }
                )
                insert_rows(
                    CHUNK_REPORTS_TABLE,
                    [
                        {
                            "investigation_id": investigation_id,
                            "chunk_id": 0,
                            "chunk_from": ch_datetime(start),
                            "chunk_to": ch_datetime(end),
                            "rows_read": int(overview.get("rows") or 0),
                            "chars_read": len(context_json),
                            "kimi_summary_json": context_json,
                            "candidate_filters_json": json.dumps(period_profile.get("rare_events", []), ensure_ascii=False),
                            "evidence_json": json.dumps(period_profile.get("raw_fragments", {}), ensure_ascii=False),
                            "error": "",
                        }
                    ],
                )
            else:
                period_profile = (
                    fetch_period_profile(source_table, start, end, source_name, index_like, fields, PROFILE_LIMIT)
                    if PROFILE_ENABLED
                    else {}
                )
                if period_profile:
                    period_profile["_profile_seconds"] = round(time.time() - profile_started, 3)
                    reports.append({"chunk_id": 0, "chunk_selector": "clickhouse_period_profile", "report": period_profile})

            seen_documents = set()
            specs = (
                []
                if is_context_strategy(strategy) or raw_chunk_mode in ("off", "none", "profile_only")
                else chunk_specs(start, end, max_chunks, fields, conf)
            )
            for spec in specs:
                ensure_not_cancelled(f"chunk {chunk_id + 1} fetch")
                rows = fetch_log_rows(
                    source_table,
                    spec["start"],
                    spec["end"],
                    source_name,
                    index_like,
                    spec.get("limit", row_limit),
                    extra_filter=spec["filter"],
                    order_by=spec["order_by"],
                    offset=spec.get("offset", 0),
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
                    ensure_not_cancelled(f"chunk {chunk_id} Kimi call")
                    kimi = call_kimi(
                        chunk_prompt(user_question, chunk_id, rows, fields),
                        trace_id=trace_id,
                        generation_name="Airflow raw log chunk analysis",
                        metadata={
                            "investigation_id": investigation_id,
                            "chunk_id": chunk_id,
                            "chunk_selector": spec["label"],
                            "rows_read": len(rows),
                            "chars_read": chars_read,
                            "chunk_from": chunk_from,
                            "chunk_to": chunk_to,
                        },
                        input_summary={
                            "question": user_question,
                            "chunk_id": chunk_id,
                            "chunk_selector": spec["label"],
                            "rows_read": len(rows),
                            "chars_read": chars_read,
                            "chunk_from": chunk_from,
                            "chunk_to": chunk_to,
                            "document_ids": [row["document_id"] for row in rows[:20]],
                        },
                    )
                    ensure_not_cancelled(f"chunk {chunk_id} report parse")
                    parsed = extract_json_object(kimi["content"])
                    parsed["_kimi_seconds"] = kimi["seconds"]
                    parsed["_usage"] = kimi["usage"]
                    error = ""
                    summary_json = json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
                except AirflowFailException:
                    raise
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

            ensure_not_cancelled("final Kimi call")
            final = call_kimi(
                final_prompt(user_question, source_table, start.isoformat(), end.isoformat(), reports, fields, period_profile),
                trace_id=trace_id,
                generation_name="Airflow refined SQL synthesis",
                metadata={
                    "investigation_id": investigation_id,
                    "analysis_strategy": strategy,
                    "raw_chunks": chunk_id,
                    "batch_rows": row_limit,
                    "source_table": source_table,
                },
                input_summary={
                    "question": user_question,
                    "source_table": source_table,
                    "time_from": start.isoformat(),
                    "time_to": end.isoformat(),
                    "analysis_strategy": strategy,
                    "raw_chunks": chunk_id,
                    "batch_rows": row_limit,
                },
            )
            ensure_not_cancelled("final report parse")
            final_json = extract_json_object(final["content"])
            final_json["_kimi_seconds"] = final["seconds"]
            final_json["_usage"] = final["usage"]
            final_json["_profile_seconds"] = period_profile.get("_profile_seconds", 0)
            final_json["_analysis_strategy"] = strategy
            final_json["_raw_chunk_mode"] = raw_chunk_mode
            final_json["_raw_chunks"] = chunk_id
            final_json["_batch_rows"] = row_limit
            final_json["_langfuse_trace_id"] = trace_id
            refined_sql = select_only(final_json.get("refined_sql") or "")
            validation_result = ""
            for repair_attempt in range(SQL_REPAIR_ATTEMPTS + 1):
                try:
                    validation_result = clickhouse_query(f"EXPLAIN SYNTAX {refined_sql}", timeout=120)
                    break
                except Exception as exc:
                    if repair_attempt >= SQL_REPAIR_ATTEMPTS:
                        raise
                    ensure_not_cancelled(f"SQL repair {repair_attempt + 1} Kimi call")
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
                        ),
                        trace_id=trace_id,
                        generation_name="Airflow refined SQL repair",
                        metadata={
                            "investigation_id": investigation_id,
                            "repair_attempt": repair_attempt + 1,
                            "validation_error": str(exc)[:4000],
                        },
                        input_summary={
                            "question": user_question,
                            "repair_attempt": repair_attempt + 1,
                            "invalid_sql": refined_sql,
                            "validation_error": str(exc)[:4000],
                        },
                    )
                    ensure_not_cancelled(f"SQL repair {repair_attempt + 1} parse")
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
                "analysis_strategy": strategy,
                "chunks": chunk_id,
                "batch_rows": row_limit,
                "total_seconds": round(time.time() - started_at, 3),
                "refined_sql": refined_sql,
            }
        except Exception as exc:
            cancelled = isinstance(exc, AirflowFailException)
            insert_rows(
                INVESTIGATIONS_TABLE,
                [
                    {
                        "investigation_id": investigation_id,
                        "user_question": user_question,
                        "source_table": source_table,
                        "time_from": ch_datetime(start),
                        "time_to": ch_datetime(end),
                        "status": "cancelled" if cancelled else "failed",
                        "error": str(exc),
                    }
                ],
            )
            raise

    run_refinement()


llm_guided_log_sql_refinement()
