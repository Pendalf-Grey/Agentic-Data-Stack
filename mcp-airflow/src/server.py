from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP


AIRFLOW_BASE_URL = os.getenv("AIRFLOW_API_BASE_URL", os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")).rstrip("/")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", os.getenv("AIRFLOW_ADMIN_USER", "admin"))
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("AIRFLOW_MCP_REQUEST_TIMEOUT_SECONDS", "30"))
POLL_INTERVAL_SECONDS = float(os.getenv("AIRFLOW_MCP_POLL_INTERVAL_SECONDS", "5"))
DEFAULT_WAIT_TIMEOUT_SECONDS = int(os.getenv("AIRFLOW_MCP_DEFAULT_WAIT_TIMEOUT_SECONDS", "1800"))
MIN_WAIT_TIMEOUT_SECONDS = int(os.getenv("AIRFLOW_MCP_MIN_WAIT_TIMEOUT_SECONDS", str(DEFAULT_WAIT_TIMEOUT_SECONDS)))
MAX_RESPONSE_CHARS = int(os.getenv("AIRFLOW_MCP_MAX_RESPONSE_CHARS", "20000"))
MAX_LOG_BATCHES_PER_REQUEST = int(os.getenv("AIRFLOW_MCP_MAX_LOG_BATCHES_PER_REQUEST", "500"))
TARGET_LOG_BATCHES_PER_REQUEST = int(os.getenv("AIRFLOW_MCP_TARGET_LOG_BATCHES_PER_REQUEST", "12"))


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


CANCEL_ON_TIMEOUT = env_bool("AIRFLOW_MCP_CANCEL_ON_TIMEOUT", True)
AUTO_SCALE_BATCH_ROWS = env_bool("AIRFLOW_MCP_AUTO_SCALE_BATCH_ROWS", True)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "http://clickhouse:8123").rstrip("/")
if "://" not in CLICKHOUSE_HOST:
    CLICKHOUSE_HOST = f"http://{CLICKHOUSE_HOST}:8123"
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "analytics")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "analytics_password")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", os.getenv("CLICKHOUSE_DB", "analytics"))

# Основные дефолты не зашиты в код жестко: LibreChat/Kimi могут вызвать MCP
# без параметров, а конкретный источник логов и размер batch берутся из .env.
DEFAULT_REFINEMENT_DAG_ID = os.getenv(
    "ADS_LLM_LOG_REFINEMENT_DAG_ID",
    os.getenv("AIRFLOW_LLM_SQL_REFINEMENT_DAG_ID", "llm_guided_log_sql_refinement"),
)
DEFAULT_SOURCE_TABLE = os.getenv("ADS_LLM_LOG_SOURCE_TABLE", os.getenv("LLM_LOG_SOURCE_TABLE", "analytics.elasticsearch_events_raw"))
DEFAULT_SOURCE_NAME = os.getenv("ADS_LLM_LOG_SOURCE_NAME", os.getenv("LLM_LOG_SOURCE_NAME", "elasticsearch-demo"))
DEFAULT_INDEX_LIKE = os.getenv("ADS_LLM_LOG_INDEX_LIKE", os.getenv("LLM_LOG_INDEX_LIKE", "nginx-logs-%"))
DEFAULT_START = os.getenv("LLM_LOG_START", "2024-06-16T00:00:00Z")
DEFAULT_END = os.getenv("LLM_LOG_END", "2026-06-16T00:00:00Z")
# Стратегия по умолчанию: тяжелое чтение всего периода делает ClickHouse,
# а Kimi получает компактный аналитический контекст и raw-доказательства.
DEFAULT_ANALYSIS_STRATEGY = os.getenv("ADS_LLM_LOG_ANALYSIS_STRATEGY", os.getenv("LLM_LOG_ANALYSIS_STRATEGY", "context"))
DEFAULT_RAW_CHUNK_MODE = os.getenv("ADS_LLM_LOG_RAW_CHUNK_MODE", os.getenv("LLM_LOG_RAW_CHUNK_MODE", "full_period"))
# batch_rows - сколько raw log rows Airflow отдаст Kimi за один chunk-анализ.
# Старые имена переменных оставлены как fallback, чтобы не ломать существующие .env.
DEFAULT_CHUNK_ROW_LIMIT = int(
    os.getenv(
        "ADS_LLM_LOG_BATCH_ROWS",
        os.getenv(
            "LLM_LOG_BATCH_ROWS",
            os.getenv("ADS_LLM_LOG_CHUNK_MAX_ROWS", os.getenv("LLM_LOG_CHUNK_ROW_LIMIT", "5")),
        ),
    )
)
DEFAULT_MAX_CHUNKS = int(os.getenv("ADS_LLM_LOG_MAX_CHUNKS", os.getenv("LLM_LOG_MAX_CHUNKS", "0")))

RESULT_DATABASE = os.getenv("ADS_LLM_LOG_RESULT_DATABASE", os.getenv("LLM_LOG_RESULT_DATABASE", os.getenv("CLICKHOUSE_DB", "analytics")))
INVESTIGATIONS_TABLE = os.getenv("ADS_LLM_LOG_INVESTIGATIONS_TABLE", os.getenv("LLM_LOG_INVESTIGATIONS_TABLE", "llm_log_investigations"))
CHUNK_REPORTS_TABLE = os.getenv("ADS_LLM_LOG_CHUNK_REPORTS_TABLE", os.getenv("LLM_LOG_CHUNK_REPORTS_TABLE", "llm_log_chunk_reports"))
REFINED_SQL_TABLE = os.getenv("ADS_LLM_LOG_REFINED_SQL_TABLE", os.getenv("LLM_LOG_REFINED_SQL_TABLE", "llm_log_refined_sql"))

MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "8000"))
SAFE_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$")


app = FastMCP("airflow", host=MCP_HOST, port=MCP_PORT)


class AirflowClient:
    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(
                timeout=timeout or REQUEST_TIMEOUT_SECONDS,
                auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
                headers={"Accept": "application/json"},
            ) as client:
                response = await client.request(method, f"{AIRFLOW_BASE_URL}{path}", params=params, json=json_body)
            return {
                "ok": 200 <= response.status_code < 300,
                "status_code": response.status_code,
                "data": clamp(parse_response(response)),
            }
        except Exception as exc:
            return {
                "ok": False,
                "status_code": None,
                "error": type(exc).__name__,
                "message": str(exc),
                "airflow_base_url": AIRFLOW_BASE_URL,
            }


client = AirflowClient()


def parse_response(response: httpx.Response) -> Any:
    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            return response.json()
        except ValueError:
            pass
    return response.text


def clamp(value: Any) -> Any:
    # Ответ MCP попадает обратно в LibreChat-контекст, поэтому длинные payload'ы
    # режем на границе инструмента, а полные результаты оставляем в ClickHouse.
    rendered = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    if len(rendered) <= MAX_RESPONSE_CHARS:
        return value
    return {"truncated": True, "max_chars": MAX_RESPONSE_CHARS, "text": rendered[:MAX_RESPONSE_CHARS]}


def now_run_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def sql_string(value: str) -> str:
    # Минимальное экранирование для диагностического SELECT lookup по investigation_id.
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def safe_table_name(value: str) -> str:
    if not SAFE_TABLE_RE.match(value):
        raise ValueError(f"Unsafe ClickHouse table name: {value!r}")
    return value


def clickhouse_datetime(value: str) -> str:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def analysis_strategy_name(value: str | None) -> str:
    return str(value or DEFAULT_ANALYSIS_STRATEGY or "context").strip().lower()


def is_context_strategy(value: str) -> bool:
    # Синонимы оставлены для удобства экспериментов из UI/.env без правок кода.
    return value in {"context", "analytical_context", "profile_guided", "context_first"}


async def estimate_log_batches(conf: dict[str, Any]) -> dict[str, Any]:
    # Быстрый preflight до запуска DAG: считаем строки в ClickHouse и заранее
    # регулируем размер batch, чтобы большой период не порождал сотни/тысячи
    # последовательных Kimi-вызовов и не продолжал работать в фоне часами.
    source_table = safe_table_name(str(conf["source_table"]))
    requested_batch_rows = max(1, int(conf["chunk_row_limit"]))
    sql = f"""
SELECT count() AS rows
FROM {source_table}
WHERE event_time >= toDateTime64({sql_string(clickhouse_datetime(conf["start"]))}, 3, 'UTC')
  AND event_time < toDateTime64({sql_string(clickhouse_datetime(conf["end"]))}, 3, 'UTC')
  AND source_name = {sql_string(conf["source_name"])}
  AND index_name LIKE {sql_string(conf["index_like"])}
FORMAT JSONEachRow
"""
    async with httpx.AsyncClient(
        timeout=REQUEST_TIMEOUT_SECONDS,
        headers={"X-ClickHouse-User": CLICKHOUSE_USER, "X-ClickHouse-Key": CLICKHOUSE_PASSWORD},
    ) as http:
        response = await http.post(f"{CLICKHOUSE_HOST}/", params={"database": CLICKHOUSE_DATABASE, "query": sql})
    response.raise_for_status()
    first_line = response.text.splitlines()[0] if response.text.strip() else "{}"
    rows = int((json.loads(first_line).get("rows") or 0))
    strategy = analysis_strategy_name(conf.get("analysis_strategy"))
    if is_context_strategy(strategy):
        # В context-first режиме число Kimi-вызовов не зависит от количества
        # raw-строк: ClickHouse сам строит профили, окна, редкости и raw samples.
        return {
            "rows": rows,
            "requested_batch_rows": requested_batch_rows,
            "batch_rows": requested_batch_rows,
            "autoscaled_batch_rows": False,
            "estimated_kimi_chunk_calls": 1 if rows > 0 else 0,
            "estimated_raw_chunk_calls": 0,
            "target_kimi_chunk_calls": TARGET_LOG_BATCHES_PER_REQUEST,
            "max_kimi_chunk_calls": MAX_LOG_BATCHES_PER_REQUEST,
            "analysis_strategy": strategy,
            "context_first": True,
        }
    effective_target = TARGET_LOG_BATCHES_PER_REQUEST
    if MAX_LOG_BATCHES_PER_REQUEST > 0 and effective_target > 0:
        effective_target = min(effective_target, MAX_LOG_BATCHES_PER_REQUEST)

    batch_rows = requested_batch_rows
    autoscaled = False
    if AUTO_SCALE_BATCH_ROWS and rows > 0 and effective_target > 0:
        min_batch_rows_for_target = max(1, (rows + effective_target - 1) // effective_target)
        if min_batch_rows_for_target > batch_rows:
            batch_rows = min_batch_rows_for_target
            autoscaled = True

    return {
        "rows": rows,
        "requested_batch_rows": requested_batch_rows,
        "batch_rows": batch_rows,
        "autoscaled_batch_rows": autoscaled,
        "estimated_kimi_chunk_calls": (rows + batch_rows - 1) // batch_rows,
        "estimated_raw_chunk_calls": (rows + batch_rows - 1) // batch_rows,
        "target_kimi_chunk_calls": TARGET_LOG_BATCHES_PER_REQUEST,
        "max_kimi_chunk_calls": MAX_LOG_BATCHES_PER_REQUEST,
        "analysis_strategy": strategy,
        "context_first": False,
    }


def artifacts(investigation_id: str) -> dict[str, str]:
    # MCP не тащит chunk reports в ответ целиком: он возвращает адреса таблиц и
    # готовый lookup-запрос, который Kimi затем выполняет через ClickHouse MCP.
    return {
        "investigation_id": investigation_id,
        "investigations_table": f"{RESULT_DATABASE}.{INVESTIGATIONS_TABLE}",
        "chunk_reports_table": f"{RESULT_DATABASE}.{CHUNK_REPORTS_TABLE}",
        "refined_sql_table": f"{RESULT_DATABASE}.{REFINED_SQL_TABLE}",
        "refined_sql_lookup": (
            f"SELECT investigation_id, refined_sql, rationale, confidence, validation_result "
            f"FROM {RESULT_DATABASE}.{REFINED_SQL_TABLE} FINAL "
            f"WHERE investigation_id = {sql_string(investigation_id)}"
        ),
    }


def iso_or_default(value: str | None, default: str) -> str:
    return (value or default).strip()


async def get_dag_run(dag_id: str, dag_run_id: str) -> dict[str, Any]:
    return await client.request("GET", f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}")


async def cancel_dag_run(dag_id: str, dag_run_id: str) -> dict[str, Any]:
    # Airflow REST API не имеет отдельной кнопки "stop"; самый надежный
    # управляемый сигнал для scheduler/DAG-кода здесь - перевести DagRun в failed.
    # Сам DAG перед каждым Kimi batch дополнительно проверяет это состояние и
    # прерывает цикл, чтобы LLM-вызовы не продолжались в фоне после MCP timeout.
    return await client.request(
        "PATCH",
        f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}",
        json_body={"state": "failed"},
    )


@app.tool()
async def airflow_health() -> dict[str, Any]:
    """Check Airflow REST API availability and authentication."""
    return await client.request("GET", "/api/v1/health")


@app.tool()
async def airflow_get_dag(dag_id: str = DEFAULT_REFINEMENT_DAG_ID) -> dict[str, Any]:
    """Read metadata for an Airflow DAG."""
    return await client.request("GET", f"/api/v1/dags/{dag_id}")


@app.tool()
async def airflow_get_dag_run(dag_run_id: str, dag_id: str = DEFAULT_REFINEMENT_DAG_ID) -> dict[str, Any]:
    """Read an Airflow DAG run by id."""
    return await get_dag_run(dag_id, dag_run_id)


@app.tool()
async def airflow_run_log_refinement(
    question: str,
    start: str | None = None,
    end: str | None = None,
    source_table: str | None = None,
    source_name: str | None = None,
    index_like: str | None = None,
    analysis_strategy: str | None = None,
    raw_chunk_mode: str | None = None,
    batch_rows: int | None = None,
    chunk_row_limit: int | None = None,
    max_chunks: int | None = None,
    investigation_id: str | None = None,
    dag_id: str = DEFAULT_REFINEMENT_DAG_ID,
    wait_for_completion: bool = True,
    wait_timeout_seconds: int | None = None,
) -> dict[str, Any]:
    """Trigger the ADS LLM-guided log SQL refinement DAG and optionally wait for completion."""
    resolved_investigation_id = investigation_id or f"ui-{uuid.uuid4()}"
    dag_run_id = f"mcp__{resolved_investigation_id}__{now_run_suffix()}"
    # В Airflow уходит только задача и параметры чтения логов. Сам analytical
    # context/chunk reports/refined SQL DAG сохранит в ClickHouse, а не вернет
    # большим MCP payload'ом обратно в LibreChat.
    conf: dict[str, Any] = {
        "question": question,
        "start": iso_or_default(start, DEFAULT_START),
        "end": iso_or_default(end, DEFAULT_END),
        "source_table": source_table or DEFAULT_SOURCE_TABLE,
        "source_name": source_name or DEFAULT_SOURCE_NAME,
        "index_like": index_like or DEFAULT_INDEX_LIKE,
        "analysis_strategy": analysis_strategy_name(analysis_strategy),
        "raw_chunk_mode": raw_chunk_mode or DEFAULT_RAW_CHUNK_MODE,
        "batch_rows": batch_rows,
        "chunk_row_limit": (
            batch_rows
            if batch_rows is not None
            else chunk_row_limit
            if chunk_row_limit is not None
            else DEFAULT_CHUNK_ROW_LIMIT
        ),
        "max_chunks": max_chunks if max_chunks is not None else DEFAULT_MAX_CHUNKS,
        "investigation_id": resolved_investigation_id,
    }
    try:
        estimate = await estimate_log_batches(conf)
    except Exception as exc:
        return {
            "ok": False,
            "triggered": False,
            "state": "preflight_failed",
            "message": (
                "Could not estimate raw-log row count before starting Airflow. "
                "Do not poll the DAG; fix ClickHouse/source settings first."
            ),
            "error": type(exc).__name__,
            "detail": str(exc),
            "conf": conf,
        }
    if estimate["rows"] == 0 and source_name and source_name != DEFAULT_SOURCE_NAME:
        fallback_conf = {**conf, "source_name": DEFAULT_SOURCE_NAME}
        try:
            fallback_estimate = await estimate_log_batches(fallback_conf)
        except Exception:
            fallback_estimate = None
        if fallback_estimate and fallback_estimate["rows"] > 0:
            estimate = {
                **fallback_estimate,
                "source_name_normalized_from": source_name,
                "source_name_normalized_to": DEFAULT_SOURCE_NAME,
            }
            conf = fallback_conf
    conf["batch_rows"] = estimate["batch_rows"]
    conf["chunk_row_limit"] = estimate["batch_rows"]
    if (
        MAX_LOG_BATCHES_PER_REQUEST > 0
        and estimate["estimated_kimi_chunk_calls"] > MAX_LOG_BATCHES_PER_REQUEST
    ):
        return {
            "ok": False,
            "triggered": False,
            "state": "budget_exceeded",
            "message": (
                "Requested raw-log interval is too large for interactive raw-chunk LLM refinement. "
                "Use analysis_strategy=context, narrow the time range, or raise AIRFLOW_MCP_MAX_LOG_BATCHES_PER_REQUEST explicitly."
            ),
            "preflight": estimate,
            "conf": conf,
            "next_steps": [
                "Do not call airflow_get_dag_run; no DAG was started.",
                "Ask the user to switch to analysis_strategy=context, narrow the time range, increase batch_rows, or raise AIRFLOW_MCP_MAX_LOG_BATCHES_PER_REQUEST.",
            ],
        }
    trigger = await client.request(
        "POST",
        f"/api/v1/dags/{dag_id}/dagRuns",
        json_body={"dag_run_id": dag_run_id, "conf": conf},
    )
    result: dict[str, Any] = {
        "ok": trigger["ok"],
        "trigger": trigger,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "conf": conf,
        "preflight": estimate,
        "artifacts": artifacts(resolved_investigation_id),
        "next_steps": [
            "If state is success, use ClickHouse MCP to read artifacts.refined_sql_lookup.",
            "Execute the returned refined_sql through ClickHouse MCP.",
            "Use Grafana MCP only after the refined SQL/result shape is known and the user asked for a chart/dashboard.",
        ],
    }
    if not trigger["ok"] or not wait_for_completion:
        return result

    requested_wait_timeout_seconds = wait_timeout_seconds
    effective_wait_timeout_seconds = wait_timeout_seconds if wait_timeout_seconds is not None else DEFAULT_WAIT_TIMEOUT_SECONDS
    if MIN_WAIT_TIMEOUT_SECONDS > 0 and effective_wait_timeout_seconds < MIN_WAIT_TIMEOUT_SECONDS:
        effective_wait_timeout_seconds = MIN_WAIT_TIMEOUT_SECONDS
    result["requested_wait_timeout_seconds"] = requested_wait_timeout_seconds
    result["wait_timeout_seconds"] = effective_wait_timeout_seconds
    result["min_wait_timeout_seconds"] = MIN_WAIT_TIMEOUT_SECONDS
    deadline = time.time() + effective_wait_timeout_seconds
    last_run: dict[str, Any] | None = None
    # Держим MCP-вызов открытым до завершения DAG, чтобы Kimi в UI сразу получила
    # state=success и могла перейти к чтению refined_sql через ClickHouse MCP.
    while time.time() < deadline:
        last_run = await get_dag_run(dag_id, dag_run_id)
        state = ((last_run.get("data") or {}).get("state") or "").lower() if last_run.get("ok") else ""
        if state in {"success", "failed"}:
            result["dag_run"] = last_run
            result["state"] = state
            result["ok"] = state == "success"
            if state != "success":
                result["message"] = "Airflow DAG run finished without success. Inspect dag_run and task logs in Airflow."
            return result
        result["state"] = state or "unknown"
        await asyncio.sleep(POLL_INTERVAL_SECONDS)

    result["ok"] = False
    result["dag_run"] = last_run
    result["requested_wait_timeout_seconds"] = requested_wait_timeout_seconds
    result["wait_timeout_seconds"] = effective_wait_timeout_seconds
    result["min_wait_timeout_seconds"] = MIN_WAIT_TIMEOUT_SECONDS
    result["cancel_on_timeout"] = CANCEL_ON_TIMEOUT
    if CANCEL_ON_TIMEOUT:
        cancel = await cancel_dag_run(dag_id, dag_run_id)
        cancelled_run = await get_dag_run(dag_id, dag_run_id)
        result["state"] = "timeout_cancelled" if cancel.get("ok") else "timeout_cancel_failed"
        result["cancel"] = cancel
        result["dag_run_after_cancel"] = cancelled_run
        result["message"] = (
            "Timed out while waiting for Airflow DAG run completion. "
            "The DagRun was marked failed so the cooperative DAG checks stop further Kimi batch calls."
        )
    else:
        result["state"] = "timeout"
        result["message"] = (
            "Timed out while waiting for Airflow DAG run completion. "
            "AIRFLOW_MCP_CANCEL_ON_TIMEOUT=false, so the DAG may continue running in the background."
        )
    return result


if __name__ == "__main__":
    app.run(transport="sse")
