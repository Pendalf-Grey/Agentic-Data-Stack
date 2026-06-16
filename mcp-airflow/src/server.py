from __future__ import annotations

import asyncio
import json
import os
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
DEFAULT_WAIT_TIMEOUT_SECONDS = int(os.getenv("AIRFLOW_MCP_DEFAULT_WAIT_TIMEOUT_SECONDS", "900"))
MAX_RESPONSE_CHARS = int(os.getenv("AIRFLOW_MCP_MAX_RESPONSE_CHARS", "20000"))

DEFAULT_REFINEMENT_DAG_ID = os.getenv(
    "ADS_LLM_LOG_REFINEMENT_DAG_ID",
    os.getenv("AIRFLOW_LLM_SQL_REFINEMENT_DAG_ID", "llm_guided_log_sql_refinement"),
)
DEFAULT_SOURCE_TABLE = os.getenv("ADS_LLM_LOG_SOURCE_TABLE", os.getenv("LLM_LOG_SOURCE_TABLE", "analytics.elasticsearch_events_raw"))
DEFAULT_SOURCE_NAME = os.getenv("ADS_LLM_LOG_SOURCE_NAME", os.getenv("LLM_LOG_SOURCE_NAME", "elasticsearch-demo"))
DEFAULT_INDEX_LIKE = os.getenv("ADS_LLM_LOG_INDEX_LIKE", os.getenv("LLM_LOG_INDEX_LIKE", "nginx-logs-%"))
DEFAULT_START = os.getenv("LLM_LOG_START", "2024-06-16T00:00:00Z")
DEFAULT_END = os.getenv("LLM_LOG_END", "2026-06-16T00:00:00Z")
DEFAULT_RAW_CHUNK_MODE = os.getenv("ADS_LLM_LOG_RAW_CHUNK_MODE", os.getenv("LLM_LOG_RAW_CHUNK_MODE", "full_period"))
DEFAULT_CHUNK_ROW_LIMIT = int(os.getenv("ADS_LLM_LOG_CHUNK_MAX_ROWS", os.getenv("LLM_LOG_CHUNK_ROW_LIMIT", "5000")))
DEFAULT_MAX_CHUNKS = int(os.getenv("ADS_LLM_LOG_MAX_CHUNKS", os.getenv("LLM_LOG_MAX_CHUNKS", "0")))

RESULT_DATABASE = os.getenv("ADS_LLM_LOG_RESULT_DATABASE", os.getenv("LLM_LOG_RESULT_DATABASE", os.getenv("CLICKHOUSE_DB", "analytics")))
INVESTIGATIONS_TABLE = os.getenv("ADS_LLM_LOG_INVESTIGATIONS_TABLE", os.getenv("LLM_LOG_INVESTIGATIONS_TABLE", "llm_log_investigations"))
CHUNK_REPORTS_TABLE = os.getenv("ADS_LLM_LOG_CHUNK_REPORTS_TABLE", os.getenv("LLM_LOG_CHUNK_REPORTS_TABLE", "llm_log_chunk_reports"))
REFINED_SQL_TABLE = os.getenv("ADS_LLM_LOG_REFINED_SQL_TABLE", os.getenv("LLM_LOG_REFINED_SQL_TABLE", "llm_log_refined_sql"))

MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "8000"))


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
    rendered = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    if len(rendered) <= MAX_RESPONSE_CHARS:
        return value
    return {"truncated": True, "max_chars": MAX_RESPONSE_CHARS, "text": rendered[:MAX_RESPONSE_CHARS]}


def now_run_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def sql_string(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def artifacts(investigation_id: str) -> dict[str, str]:
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
    raw_chunk_mode: str | None = None,
    chunk_row_limit: int | None = None,
    max_chunks: int | None = None,
    investigation_id: str | None = None,
    dag_id: str = DEFAULT_REFINEMENT_DAG_ID,
    wait_for_completion: bool = True,
    wait_timeout_seconds: int | None = None,
) -> dict[str, Any]:
    """Trigger the ADS LLM-guided raw-log SQL refinement DAG and optionally wait for completion."""
    resolved_investigation_id = investigation_id or f"ui-{uuid.uuid4()}"
    dag_run_id = f"mcp__{resolved_investigation_id}__{now_run_suffix()}"
    conf: dict[str, Any] = {
        "question": question,
        "start": iso_or_default(start, DEFAULT_START),
        "end": iso_or_default(end, DEFAULT_END),
        "source_table": source_table or DEFAULT_SOURCE_TABLE,
        "source_name": source_name or DEFAULT_SOURCE_NAME,
        "index_like": index_like or DEFAULT_INDEX_LIKE,
        "raw_chunk_mode": raw_chunk_mode or DEFAULT_RAW_CHUNK_MODE,
        "chunk_row_limit": chunk_row_limit if chunk_row_limit is not None else DEFAULT_CHUNK_ROW_LIMIT,
        "max_chunks": max_chunks if max_chunks is not None else DEFAULT_MAX_CHUNKS,
        "investigation_id": resolved_investigation_id,
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
        "artifacts": artifacts(resolved_investigation_id),
        "next_steps": [
            "If state is success, use ClickHouse MCP to read artifacts.refined_sql_lookup.",
            "Execute the returned refined_sql through ClickHouse MCP.",
            "Use Grafana MCP only after the refined SQL/result shape is known and the user asked for a chart/dashboard.",
        ],
    }
    if not trigger["ok"] or not wait_for_completion:
        return result

    deadline = time.time() + (wait_timeout_seconds if wait_timeout_seconds is not None else DEFAULT_WAIT_TIMEOUT_SECONDS)
    last_run: dict[str, Any] | None = None
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
    result["state"] = "timeout"
    result["dag_run"] = last_run
    result["message"] = "Timed out while waiting for Airflow DAG run completion. Use airflow_get_dag_run with dag_run_id."
    return result


if __name__ == "__main__":
    app.run(transport="sse")
