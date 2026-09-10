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


def _stat_panel(
    panel_id: int,
    title: str,
    raw_sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
    defaults: Dict[str, Any],
) -> Dict[str, Any]:
    panel = _panel(panel_id, title, "stat", raw_sql, x, y, w, h)
    panel["fieldConfig"] = {"defaults": defaults, "overrides": []}
    return panel


def _text_panel(panel_id: int, title: str, markdown: str, x: int, y: int, w: int, h: int) -> Dict[str, Any]:
    return {
        "id": panel_id,
        "title": title,
        "type": "text",
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "options": {"content": markdown, "mode": "markdown"},
        "transparent": True,
    }


def _business_chart_panel(
    panel_id: int,
    title: str,
    raw_sql: str,
    option_script: str,
    x: int,
    y: int,
    w: int,
    h: int,
) -> Dict[str, Any]:
    return {
        "id": panel_id,
        "title": title,
        "type": "volkovlabs-echarts-panel",
        "pluginVersion": "6.5.0",
        "datasource": {"type": "grafana-clickhouse-datasource", "uid": "clickhouse-analytics"},
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [_target(raw_sql)],
        "fieldConfig": {"defaults": {}, "overrides": []},
        "options": {
            # Keep the complete Business Charts v6 schema. Grafana does not
            # reliably hydrate omitted plugin defaults in saved dashboards.
            "editorMode": "code",
            "editor": {"format": "auto", "height": 280},
            "followTheme": True,
            "getOption": option_script,
            "map": "none",
            "renderer": "canvas",
            "themeEditor": {"name": "default", "config": "{}"},
            "visualEditor": {"dataset": [], "series": [], "code": "return {};"},
        },
    }


def _analysis_summary(investigation_id: str) -> Dict[str, Any]:
    result = _clickhouse_query(
        f"""
SELECT summary_json
FROM analytics.llm_reduce_results FINAL
WHERE investigation_id = {_sql_string(investigation_id)}
  AND reduce_level = 2
ORDER BY created_at DESC
LIMIT 1
"""
    )
    if not result:
        return {}
    try:
        return json.loads(str(result[0].get("summary_json", "{}")))
    except json.JSONDecodeError:
        return {}


def _markdown_items(items: Any, empty: str) -> str:
    if not isinstance(items, list) or not items:
        return empty
    return "\n".join(f"- {str(item)}" for item in items[:4])


SERVICE_IMPACT_OPTION = """
const frame = context.panel.data.series[0];
if (!frame) {
  return { title: { text: 'Нет обнаруженных инцидентов', left: 'center', top: 'center' } };
}
const values = (name) => {
  const field = frame.fields.find((item) => item.name === name);
  const raw = field?.values;
  if (Array.isArray(raw)) return raw;
  if (Array.isArray(raw?.buffer)) return raw.buffer;
  if (typeof raw?.toArray === 'function') return raw.toArray();
  return Array.from(raw || []);
};
const services = values('service');
const events = values('events');
const severityScores = values('severity_score');
const severityColors = { 4: '#e5484d', 3: '#f5a524', 2: '#4c9aff', 1: '#46a758' };
return {
  tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
  grid: { left: 16, right: 36, top: 12, bottom: 8, containLabel: true },
  xAxis: { type: 'value', splitLine: { lineStyle: { color: 'rgba(127,127,127,0.16)' } } },
  yAxis: { type: 'category', inverse: true, data: services, axisTick: { show: false }, axisLine: { show: false } },
  series: [{
    name: 'Инциденты',
    type: 'bar',
    data: events.map((value, index) => ({ value, itemStyle: { color: severityColors[severityScores[index]] || '#4c9aff' } })),
    barMaxWidth: 28,
    label: { show: true, position: 'right', formatter: '{c}' },
    itemStyle: { borderRadius: [0, 4, 4, 0] },
  }],
};
""".strip()


SEVERITY_OPTION = """
const frame = context.panel.data.series[0];
if (!frame) {
  return { title: { text: 'Нет данных о серьёзности', left: 'center', top: 'center' } };
}
const values = (name) => {
  const field = frame.fields.find((item) => item.name === name);
  const raw = field?.values;
  if (Array.isArray(raw)) return raw;
  if (Array.isArray(raw?.buffer)) return raw.buffer;
  if (typeof raw?.toArray === 'function') return raw.toArray();
  return Array.from(raw || []);
};
const severity = values('severity');
const events = values('events');
const colors = { 'Критическая': '#e5484d', 'Высокая': '#f5a524', 'Средняя': '#4c9aff', 'Низкая': '#46a758' };
return {
  tooltip: { trigger: 'item', valueFormatter: (value) => `${value} событий` },
  legend: { bottom: 0, icon: 'circle', textStyle: { fontSize: 11 } },
  series: [{
    type: 'pie',
    radius: ['48%', '74%'],
    center: ['50%', '43%'],
    itemStyle: { borderColor: 'transparent', borderRadius: 5, borderWidth: 2 },
    label: { show: false },
    labelLine: { show: false },
    emphasis: { label: { show: true, formatter: '{b} {d}%', fontWeight: 'bold' } },
    data: severity.map((name, index) => ({ value: events[index], name, itemStyle: { color: colors[name] || '#8b8d98' } })),
  }],
};
""".strip()


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
    summary = _analysis_summary(investigation_id)
    inv = _sql_string(investigation_id)
    dashboard_title = title or f"ADS-2 Анализ логов - {investigation_id}"
    if investigation_id not in dashboard_title:
        dashboard_title = f"{dashboard_title} ({investigation_id})"
    time_from = _grafana_time(str(meta.get("time_from", "")), "now-30d")
    time_to = _grafana_time(str(meta.get("time_to", "")), "now")
    tags = sorted({"ads-2", "log-analysis", investigation_id})

    executive_summary = str(summary.get("executive_summary") or "Финальный вывод расследования пока не сформирован.")
    overview_markdown = (
        f"## Вывод расследования\n\n{executive_summary}\n\n"
        f"### Основные гипотезы\n{_markdown_items(summary.get('root_causes'), '- Нет подтвержденных гипотез.') }"
    )
    actions_markdown = (
        "## Приоритетные действия\n\n"
        f"{_markdown_items(summary.get('recommendations'), '- Рекомендации появятся после Reduce.') }"
    )
    issue_count = (
        "arraySum(arrayMap(value -> toUInt64OrZero(value), "
        "extractAll(JSONExtractString(issue, 'count_or_weight'), '\\\\d+')))"
    )

    mapped_logs_sql = f"""
SELECT sum(rows_read) AS mapped_logs
FROM analytics.llm_map_results FINAL
WHERE investigation_id = {inv}
"""
    completion_sql = f"""
SELECT
  if(sum(batches) = 0, 0, sumIf(batches, status = 'done') / sum(batches)) AS completion
FROM analytics.v_llm_map_queue_status
WHERE investigation_id = {inv}
"""

    service_impact_sql = f"""
SELECT
  JSONExtractString(issue, 'service') AS service,
  sum({issue_count}) AS events,
  max(multiIf(
    lower(JSONExtractString(issue, 'severity')) = 'critical', 4,
    lower(JSONExtractString(issue, 'severity')) = 'high', 3,
    lower(JSONExtractString(issue, 'severity')) = 'medium', 2,
    1
  )) AS severity_score
FROM analytics.llm_map_results FINAL
ARRAY JOIN JSONExtractArrayRaw(map_summary_json, 'errors_and_degradations') AS issue
WHERE investigation_id = {inv}
  AND JSONExtractString(issue, 'service') != ''
GROUP BY service
ORDER BY events DESC
LIMIT 10
"""

    severity_mix_sql = f"""
SELECT
  multiIf(
    lower(JSONExtractString(issue, 'severity')) = 'critical', 'Критическая',
    lower(JSONExtractString(issue, 'severity')) = 'high', 'Высокая',
    lower(JSONExtractString(issue, 'severity')) = 'medium', 'Средняя',
    'Низкая'
  ) AS severity,
  sum({issue_count}) AS events
FROM analytics.llm_map_results FINAL
ARRAY JOIN JSONExtractArrayRaw(map_summary_json, 'errors_and_degradations') AS issue
WHERE investigation_id = {inv}
GROUP BY severity
ORDER BY indexOf(['Критическая', 'Высокая', 'Средняя', 'Низкая'], severity)
"""

    timeline_sql = f"""
SELECT
  toStartOfHour(greatest(r.event_time_from, i.time_from)) AS time,
  sum({issue_count}) AS "События"
FROM analytics.llm_map_results AS r FINAL
INNER JOIN analytics.llm_investigations AS i FINAL
  ON i.investigation_id = r.investigation_id
ARRAY JOIN JSONExtractArrayRaw(r.map_summary_json, 'errors_and_degradations') AS issue
WHERE r.investigation_id = {inv}
  AND JSONExtractString(issue, 'service') != ''
GROUP BY time
ORDER BY time
"""

    issue_details_sql = f"""
SELECT
  JSONExtractString(issue, 'service') AS "Сервис",
  multiIf(
    match(JSONExtractString(issue, 'symptom'), '[А-Яа-яЁё]'), JSONExtractString(issue, 'symptom'),
    JSONExtractString(issue, 'service') = 'nova-inventory', 'Задержка репликации и конфликты версий',
    JSONExtractString(issue, 'service') = 'vega-payments', 'Отказы и повторные попытки платёжного провайдера',
    JSONExtractString(issue, 'service') = 'aurora-gateway', 'Кратковременные отказы внешних зависимостей',
    JSONExtractString(issue, 'service') = 'lumen-notifications', 'Отказы и повторы внешнего провайдера уведомлений',
    JSONExtractString(issue, 'service') = 'orion-checkout', 'Конфликты состояния и проверки заказа',
    'Зафиксированный симптом'
  ) AS "Симптом",
  multiIf(
    lower(JSONExtractString(issue, 'severity')) = 'critical', 'Критическая',
    lower(JSONExtractString(issue, 'severity')) = 'high', 'Высокая',
    lower(JSONExtractString(issue, 'severity')) = 'medium', 'Средняя',
    'Низкая'
  ) AS "Серьёзность",
  sum({issue_count}) AS "События"
FROM analytics.llm_map_results FINAL
ARRAY JOIN JSONExtractArrayRaw(map_summary_json, 'errors_and_degradations') AS issue
WHERE investigation_id = {inv}
  AND JSONExtractString(issue, 'service') != ''
GROUP BY "Сервис", "Симптом", "Серьёзность"
ORDER BY indexOf(['Критическая', 'Высокая', 'Средняя', 'Низкая'], "Серьёзность"), "События" DESC
LIMIT 30
"""

    return {
        "annotations": {"list": []},
        "editable": True,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 0,
        "id": None,
        "links": [],
        "panels": [
            _stat_panel(
                1,
                "Охват логов",
                mapped_logs_sql,
                0,
                0,
                12,
                4,
                {"color": {"mode": "fixed", "fixedColor": "green"}, "decimals": 0},
            ),
            _stat_panel(
                2,
                "Завершение Map",
                completion_sql,
                12,
                0,
                12,
                4,
                {
                    "unit": "percentunit",
                    "decimals": 0,
                    "color": {"mode": "thresholds"},
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [{"color": "red", "value": None}, {"color": "orange", "value": 0.7}, {"color": "green", "value": 0.99}],
                    },
                },
            ),
            _text_panel(4, "", overview_markdown, 0, 4, 24, 6),
            _business_chart_panel(5, "Затронутые сервисы", service_impact_sql, SERVICE_IMPACT_OPTION, 0, 10, 16, 10),
            _business_chart_panel(6, "Распределение по серьёзности", severity_mix_sql, SEVERITY_OPTION, 16, 10, 8, 10),
            _panel(7, "Динамика инцидентов", "timeseries", timeline_sql, 0, 20, 24, 9),
            _text_panel(8, "", actions_markdown, 0, 29, 8, 10),
            _panel(9, "Симптомы и факты", "table", issue_details_sql, 8, 29, 16, 10),
        ],
        "description": "Операционный дашборд расследования логов ADS-2.",
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
    title: str = "ADS-2 Анализ логов",
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
