import base64
import json
import os
import re
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional

from fastmcp import FastMCP


mcp = FastMCP(name="ads-grafana")


def _settings() -> Dict[str, str]:
    return {
        "url": os.getenv("GRAFANA_URL", "http://grafana:3000").rstrip("/"),
        "public_url": os.getenv("GRAFANA_PUBLIC_URL", "http://localhost:3001").rstrip("/"),
        "username": os.getenv("GRAFANA_USERNAME", "admin"),
        "password": os.getenv("GRAFANA_PASSWORD", "admin"),
    }


def _clickhouse_settings() -> Dict[str, str]:
    return {
        "host": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        "port": os.getenv("CLICKHOUSE_PORT", "8123"),
        "secure": os.getenv("CLICKHOUSE_SECURE", "false").lower(),
        "username": os.getenv("CLICKHOUSE_USER", "analytics"),
        "password": os.getenv("CLICKHOUSE_PASSWORD", "analytics_password"),
        "database": os.getenv("CLICKHOUSE_DATABASE", "analytics"),
    }


def _request(method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Any:
    settings = _settings()
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    auth = base64.b64encode(f"{settings['username']}:{settings['password']}".encode("utf-8")).decode("ascii")
    req = urllib.request.Request(
        f"{settings['url']}{path}",
        data=body,
        method=method,
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _external_url(path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return f"{_settings()['public_url']}{path}"


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _clickhouse_query(sql: str) -> list[dict[str, Any]]:
    settings = _clickhouse_settings()
    scheme = "https" if settings["secure"] == "true" else "http"
    query = urllib.parse.urlencode(
        {
            "database": settings["database"],
            "default_format": "JSON",
        }
    )
    req = urllib.request.Request(
        f"{scheme}://{settings['host']}:{settings['port']}/?{query}",
        data=sql.encode("utf-8"),
        method="POST",
        headers={
            "X-ClickHouse-User": settings["username"],
            "X-ClickHouse-Key": settings["password"],
            "Content-Type": "text/plain; charset=utf-8",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw).get("data", []) if raw else []


def _grafana_time(value: str, fallback: str) -> str:
    if not value:
        return fallback
    normalized = str(value).replace(" ", "T")
    if normalized.endswith("Z"):
        return normalized
    return f"{normalized}Z"


def _dashboard_uid(investigation_id: str) -> str:
    safe = re.sub(r"[^a-z0-9-]+", "-", investigation_id.lower()).strip("-")
    return f"ads2-{safe}"[:40] if safe else "ads2-log-analysis"


def _target(raw_sql: str, ref_id: str = "A") -> Dict[str, Any]:
    return {
        "datasource": {"type": "grafana-clickhouse-datasource", "uid": "clickhouse-analytics"},
        "format": 1,
        "queryType": "sql",
        "rawSql": raw_sql,
        "refId": ref_id,
    }


def _panel(panel_id: int, title: str, panel_type: str, raw_sql: str, x: int, y: int, w: int, h: int) -> Dict[str, Any]:
    panel: Dict[str, Any] = {
        "id": panel_id,
        "title": title,
        "type": panel_type,
        "datasource": {"type": "grafana-clickhouse-datasource", "uid": "clickhouse-analytics"},
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [_target(raw_sql)],
        "fieldConfig": {"defaults": {}, "overrides": []},
        "options": {},
    }
    if panel_type == "table":
        panel["options"] = {"showHeader": True}
    if panel_type == "stat":
        panel["options"] = {"colorMode": "value", "graphMode": "area", "justifyMode": "auto", "textMode": "auto"}
    return panel


def _analysis_metadata(investigation_id: str) -> Dict[str, Any]:
    if not investigation_id:
        return {}
    result = _clickhouse_query(
        f"""
SELECT
  investigation_id,
  toString(time_from) AS time_from,
  toString(time_to) AS time_to,
  source_name,
  index_like
FROM analytics.llm_investigations FINAL
WHERE investigation_id = {_sql_string(investigation_id)}
LIMIT 1
"""
    )
    return result[0] if result else {}


def _dashboard_for_analysis(investigation_id: str, title: str, request: str) -> Dict[str, Any]:
    meta = _analysis_metadata(investigation_id)
    inv = _sql_string(investigation_id)
    dashboard_title = title or f"ADS-2 Log Analysis - {investigation_id}"
    if investigation_id not in dashboard_title:
        dashboard_title = f"{dashboard_title} ({investigation_id})"
    time_from = _grafana_time(str(meta.get("time_from", "")), "now-30d")
    time_to = _grafana_time(str(meta.get("time_to", "")), "now")
    tags = sorted({"ads-2", "log-analysis", investigation_id})

    mapped_logs_sql = f"""
SELECT sum(rows_read) AS mapped_logs
FROM analytics.llm_map_results FINAL
WHERE investigation_id = {inv}
"""
    confidence_sql = f"""
SELECT JSONExtractFloat(summary_json, 'confidence') AS reduce_confidence
FROM analytics.llm_reduce_results FINAL
WHERE investigation_id = {inv}
  AND reduce_level = 2
"""
    service_events_sql = f"""
SELECT
  toStartOfHour(greatest(v.event_time_from, i.time_from)) AS time,
  JSONExtractString(t, 'top_service') AS service,
  sum(JSONExtractUInt(t, 'event_count')) AS events
FROM analytics.v_es_log_map_batch_inputs AS v
INNER JOIN analytics.llm_investigations AS i FINAL
  ON i.investigation_id = {inv}
ARRAY JOIN JSONExtractArrayRaw(v.map_input_json, 'important_templates') AS t
WHERE v.batch_id IN
(
  SELECT batch_id
  FROM analytics.llm_map_results FINAL
  WHERE investigation_id = {inv}
)
  AND v.event_time_to >= i.time_from
  AND v.event_time_from < i.time_to
  AND lower(JSONExtractString(t, 'top_level')) IN ('warn', 'error')
GROUP BY time, service
ORDER BY time, service
"""
    top_problems_sql = f"""
SELECT
  JSONExtractString(t, 'top_service') AS service,
  JSONExtractString(t, 'template_text') AS problem,
  lower(JSONExtractString(t, 'top_level')) AS level,
  sum(JSONExtractUInt(t, 'event_count')) AS events
FROM analytics.v_es_log_map_batch_inputs AS v
ARRAY JOIN JSONExtractArrayRaw(v.map_input_json, 'important_templates') AS t
WHERE v.batch_id IN
(
  SELECT batch_id
  FROM analytics.llm_map_results FINAL
  WHERE investigation_id = {inv}
)
  AND lower(JSONExtractString(t, 'top_level')) IN ('warn', 'error')
GROUP BY service, problem, level
ORDER BY events DESC
LIMIT 50
"""
    reduce_summary_sql = f"""
SELECT 'top_service' AS kind, arrayJoin(JSONExtract(summary_json, 'top_services', 'Array(String)')) AS item
FROM analytics.llm_reduce_results FINAL
WHERE investigation_id = {inv} AND reduce_level = 2
UNION ALL
SELECT 'root_cause' AS kind, arrayJoin(JSONExtract(summary_json, 'root_causes', 'Array(String)')) AS item
FROM analytics.llm_reduce_results FINAL
WHERE investigation_id = {inv} AND reduce_level = 2
UNION ALL
SELECT 'recommendation' AS kind, arrayJoin(JSONExtract(summary_json, 'recommendations', 'Array(String)')) AS item
FROM analytics.llm_reduce_results FINAL
WHERE investigation_id = {inv} AND reduce_level = 2
"""
    queue_sql = f"""
SELECT status, batches, rows_read, event_time_from, event_time_to
FROM analytics.v_llm_map_queue_status
WHERE investigation_id = {inv}
ORDER BY status
"""

    return {
        "annotations": {"list": []},
        "editable": True,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 0,
        "id": None,
        "links": [],
        "panels": [
            _panel(1, "Mapped logs", "stat", mapped_logs_sql, 0, 0, 8, 5),
            _panel(2, "Reduce confidence", "stat", confidence_sql, 8, 0, 8, 5),
            _panel(3, "Queue status", "table", queue_sql, 16, 0, 8, 5),
            _panel(4, "Problem events by service", "timeseries", service_events_sql, 0, 5, 24, 9),
            _panel(5, "Top service problems", "table", top_problems_sql, 0, 14, 12, 8),
            _panel(6, "Reduce findings and recommendations", "table", reduce_summary_sql, 12, 14, 12, 8),
        ],
        "refresh": "",
        "schemaVersion": 40,
        "tags": tags,
        "templating": {"list": []},
        "time": {"from": time_from, "to": time_to},
        "timepicker": {},
        "timezone": "browser",
        "title": dashboard_title,
        "uid": _dashboard_uid(investigation_id),
        "version": 1,
        "weekStart": "",
    }


@mcp.tool()
def create_grafana_dashboard_from_analysis(
    investigation_id: str = "",
    title: str = "ADS-2 Log Analysis",
    request: str = "",
) -> str:
    """Create or update the ADS-2 Grafana dashboard for an ADS log analysis and return its public URL."""
    if investigation_id:
        dashboard = _dashboard_for_analysis(investigation_id, title, request)
    else:
        template_path = Path(os.getenv("ADS_GRAFANA_DASHBOARD_TEMPLATE", "/workspace/grafana/dashboards/agentic-data-stack-events.json"))
        dashboard = json.loads(template_path.read_text(encoding="utf-8"))
        dashboard["title"] = title or dashboard.get("title", "ADS-2 Log Analysis")
    result = _request("POST", "/api/dashboards/db", {"dashboard": dashboard, "overwrite": True})
    return _external_url(result.get("url", f"/d/{dashboard.get('uid', 'ads2-log-analysis')}"))


if __name__ == "__main__":
    mcp.run(
        transport=os.getenv("ADS_MCP_TRANSPORT", "sse"),
        host=os.getenv("ADS_MCP_BIND_HOST", "0.0.0.0"),
        port=int(os.getenv("ADS_MCP_PORT", "8000")),
    )
