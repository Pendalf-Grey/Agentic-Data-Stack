import base64
import importlib.util
import json
import os
import re
import time
import traceback
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen


# Этот файл заменяет прежний Node.js MCP server.
# Данные идут так:
# LibreChat -> HTTP POST /mcp -> этот Python server -> ClickHouse/Grafana/файлы коннекторов -> ответ обратно в LibreChat.

PORT = int(os.getenv("PORT", "3333"))
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", f"http://localhost:{PORT}").rstrip("/")
GRAFANA_BASE_URL = os.getenv("GRAFANA_BASE_URL", "http://localhost:3001").rstrip("/")
GRAFANA_API_URL = os.getenv("GRAFANA_API_URL", "http://grafana:3000").rstrip("/")
GRAFANA_USER = os.getenv("GRAFANA_USER", "admin")
GRAFANA_PASSWORD = os.getenv("GRAFANA_PASSWORD", "admin")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "http://localhost:8123").rstrip("/")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "analytics")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "analytics_password")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "analytics")
GENERATED_CONNECTORS_DIR = Path(os.getenv("GENERATED_CONNECTORS_DIR", "/user-mcp-connectors")).resolve()
GENERATED_CONNECTORS_PUBLIC_DIR = os.getenv(
    "GENERATED_CONNECTORS_PUBLIC_DIR",
    str(GENERATED_CONNECTORS_DIR),
).rstrip("/")


# SVG-графики, которые создают visualize_* tools, живут в памяти и отдаются через GET /charts/<id>.
CHART_STORE = {}

# Python-модули generated-коннекторов кэшируются, но при create/update конкретный модуль перечитывается.
GENERATED_CONNECTOR_CACHE = {}


def json_rpc(request_id, result):
    """Возвращает успешный JSON-RPC ответ обратно в LibreChat."""
    return json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}, ensure_ascii=False)


def json_rpc_error(request_id, code, message):
    """Возвращает JSON-RPC ошибку. LibreChat показывает ее как tool error."""
    return json.dumps(
        {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}},
        ensure_ascii=False,
    )


def text_result(text):
    """Единый формат MCP tool ответа: текстовый payload для модели."""
    return {"content": [{"type": "text", "text": text}]}


def json_text_result(value):
    """Отдает Python-объект модели как JSON-строку."""
    return text_result(json.dumps(value, ensure_ascii=False, indent=2))


def bounded_limit(value, fallback=50, maximum=500):
    """Ограничивает limit, чтобы модель случайно не вытащила слишком много строк."""
    try:
        parsed = int(value or fallback)
    except (TypeError, ValueError):
        return fallback
    if parsed < 1:
        return fallback
    return min(parsed, maximum)


def safe_choice(value, allowed, fallback):
    """Выбирает только разрешенное значение enum."""
    return value if value in allowed else fallback


def safe_identifier(value, fallback=""):
    """Проверяет identifier-like значения, например Prometheus metric_name."""
    text = str(value or fallback)
    if not re.match(r"^[A-Za-z_:][A-Za-z0-9_:]*$", text):
        raise ValueError(f"Unsafe identifier-like value: {text}")
    return text


def safe_sql_identifier(value, fallback=""):
    """Проверяет SQL identifier: таблицы и колонки без кавычек и спецсимволов."""
    text = str(value or fallback)
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", text):
        raise ValueError(f"Unsafe SQL identifier: {text}")
    return text


def safe_label_name(value, fallback="job"):
    """Проверяет имя Prometheus label перед JSONExtractString."""
    return safe_sql_identifier(value or fallback)


def safe_dashboard_title(value, fallback):
    """Чистит заголовок Grafana dashboard перед отправкой в Grafana API."""
    text = str(value or fallback).strip()
    text = re.sub(r"[^\w\s:().,\-/]", "", text, flags=re.UNICODE)
    return text[:90] or fallback


def quote_ident(value):
    """Экранирует ClickHouse identifier обратными кавычками."""
    return f"`{str(value).replace('`', '``')}`"


def quote_string(value):
    """Экранирует строковый SQL literal."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def sql_literal(value):
    """Преобразует Python-значение в безопасный SQL literal."""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    return quote_string(value)


def normalize_filter_operator(operator):
    """Нормализует операторы фильтров от модели в SQL-операторы."""
    operators = {
        "=": "=",
        "eq": "=",
        "!=": "!=",
        "ne": "!=",
        ">": ">",
        "gt": ">",
        ">=": ">=",
        "gte": ">=",
        "<": "<",
        "lt": "<",
        "<=": "<=",
        "lte": "<=",
    }
    normalized = str(operator or "").strip().lower()
    if normalized not in operators:
        raise ValueError(f"Unsupported filter operator: {operator}")
    return operators[normalized]


def analytics_table_argument(args):
    """Принимает table и table_name, чтобы LibreChat не падал из-за варианта имени аргумента."""
    table = (args or {}).get("table") or (args or {}).get("table_name")
    if not table:
        raise ValueError('Table name is required. Use argument "table" or "table_name".')
    return table


def run_query(query):
    """Выполняет только SELECT в ClickHouse и возвращает JSONEachRow как list[dict]."""
    normalized = query.strip().rstrip(";")
    if not re.match(r"^select\b", normalized, flags=re.IGNORECASE):
        raise ValueError("Only SELECT queries are allowed.")
    if re.search(
        r"\b(insert|update|delete|alter|drop|truncate|create|grant|revoke|optimize)\b",
        normalized,
        flags=re.IGNORECASE,
    ):
        raise ValueError("Only read-only SELECT queries are allowed.")

    url = f"{CLICKHOUSE_HOST}/?{urlencode({'database': CLICKHOUSE_DATABASE})}"
    request = Request(
        url,
        data=f"{normalized}\nFORMAT JSONEachRow".encode("utf-8"),
        headers={
            "X-ClickHouse-User": CLICKHOUSE_USER,
            "X-ClickHouse-Key": CLICKHOUSE_PASSWORD,
            "Content-Type": "text/plain; charset=utf-8",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=90) as response:
            body = response.read().decode("utf-8").strip()
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(detail or str(error)) from error
    if not body:
        return []
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def analytics_table_exists(table):
    """Проверяет существование таблицы/VIEW в analytics и возвращает безопасное имя."""
    table_name = safe_sql_identifier(table)
    rows = run_query(
        f"""
        SELECT name
        FROM system.tables
        WHERE database = 'analytics'
          AND name = {quote_string(table_name)}
        LIMIT 1
        """
    )
    if not rows:
        raise ValueError(f"Unknown analytics table or view: {table_name}")
    return table_name


def analytics_columns(table):
    """Возвращает колонки таблицы из system.columns."""
    table_name = analytics_table_exists(table)
    rows = run_query(
        f"""
        SELECT name, type, position
        FROM system.columns
        WHERE database = 'analytics'
          AND table = {quote_string(table_name)}
        ORDER BY position
        """
    )
    return table_name, rows


def analytics_column_exists(table, column):
    """Проверяет колонку перед тем, как generated-коннектор использует ее в SQL."""
    column_name = safe_sql_identifier(column)
    table_name, columns = analytics_columns(table)
    if not any(row.get("name") == column_name for row in columns):
        raise ValueError(f"Unknown column {column_name} in analytics.{table_name}")
    return table_name, column_name, columns


class HelperBag:
    """Объект helpers, который передается в generated Python-коннекторы."""

    analytics_table_exists = staticmethod(analytics_table_exists)
    analytics_columns = staticmethod(analytics_columns)
    analytics_column_exists = staticmethod(analytics_column_exists)
    bounded_limit = staticmethod(bounded_limit)
    normalize_filter_operator = staticmethod(normalize_filter_operator)
    quote_ident = staticmethod(quote_ident)
    quote_string = staticmethod(quote_string)
    safe_sql_identifier = staticmethod(safe_sql_identifier)
    sql_literal = staticmethod(sql_literal)


HELPERS = HelperBag()


def safe_generated_connector_name(value):
    """Валидирует имя generated-коннектора и запрещает выход из папки хранения."""
    text = str(value or "").strip()
    if not re.match(r"^[a-z][a-z0-9_]{2,80}$", text):
        raise ValueError(f"Unsafe generated connector name: {text}")
    if not text.startswith("clickhouse_"):
        raise ValueError("Generated connector names must start with clickhouse_.")
    return text


def generated_connector_path(connector_name):
    """Каждый generated-коннектор хранится в отдельной папке с файлом connector.py."""
    safe_name = safe_generated_connector_name(connector_name)
    return GENERATED_CONNECTORS_DIR / safe_name / "connector.py"


def public_generated_connector_path(connector_name):
    """Путь, который MCP возвращает модели и который пользователь видит в чате."""
    safe_name = safe_generated_connector_name(connector_name)
    return f"{GENERATED_CONNECTORS_PUBLIC_DIR}/{safe_name}/connector.py"


def validate_generated_connector_source(connector_name, source_code):
    """Проверяет Python-код generated-коннектора до записи на диск."""
    safe_name = safe_generated_connector_name(connector_name)
    source = str(source_code or "").strip()
    if not source:
        raise ValueError("Generated connector source_code is required.")
    if len(source) > 30000:
        raise ValueError("Generated connector source_code is too large.")
    if "CONNECTOR" not in source or "def handler" not in source:
        raise ValueError("Generated Python connector must define CONNECTOR and def handler(...).")
    if not re.search(rf"['\"]name['\"]\s*:\s*['\"]{re.escape(safe_name)}['\"]", source):
        raise ValueError(f"Generated connector source must export name {safe_name}.")
    if "analytics." in source and "analytics_column_exists" not in source and "analytics_columns" not in source:
        raise ValueError(
            "Generated connectors that query analytics.* must validate referenced columns "
            "with helpers.analytics_column_exists or helpers.analytics_columns before run_query."
        )
    forbidden_patterns = [
        r"\bimport\b",
        r"\b__import__\b",
        r"\bopen\s*\(",
        r"\bos\b",
        r"\bsys\b",
        r"\bsubprocess\b",
        r"\bsocket\b",
        r"\beval\s*\(",
        r"\bexec\s*\(",
        r"\bcompile\s*\(",
        r"\brequests\b",
        r"\burllib\b",
    ]
    for pattern in forbidden_patterns:
        if re.search(pattern, source):
            raise ValueError(f"Generated connector source contains forbidden pattern: {pattern}")
    return source + "\n"


def ensure_generated_connectors_dir():
    """Создает внешнюю папку коннекторов, если ее нет."""
    GENERATED_CONNECTORS_DIR.mkdir(parents=True, exist_ok=True)


def ensure_generated_connector_dir(connector_name):
    """Создает отдельную папку конкретного коннектора."""
    generated_connector_path(connector_name).parent.mkdir(parents=True, exist_ok=True)


def validate_generated_connector_module(module, expected_name=""):
    """Проверяет контракт импортированного Python-коннектора."""
    connector = getattr(module, "CONNECTOR", None)
    handler = getattr(module, "handler", None)
    if not isinstance(connector, dict):
        raise ValueError("Generated connector must define CONNECTOR dict.")
    connector_name = safe_generated_connector_name(connector.get("name"))
    if expected_name and connector_name != safe_generated_connector_name(expected_name):
        raise ValueError(f"Generated connector name mismatch: expected {expected_name}, got {connector_name}.")
    if not isinstance(connector.get("description"), str) or len(connector.get("description", "").strip()) < 10:
        raise ValueError(f"Generated connector {connector_name} must have a useful description.")
    if not isinstance(connector.get("input_schema"), dict) or connector["input_schema"].get("type") != "object":
        raise ValueError(f"Generated connector {connector_name} must expose object input_schema.")
    if not callable(handler):
        raise ValueError(f"Generated connector {connector_name} must define handler(args, run_query, helpers).")
    return connector_name


def load_generated_connector(connector_name, force=False):
    """Импортирует generated connector.py как Python-модуль."""
    safe_name = safe_generated_connector_name(connector_name)
    if not force and safe_name in GENERATED_CONNECTOR_CACHE:
        return GENERATED_CONNECTOR_CACHE[safe_name]
    file_path = generated_connector_path(safe_name)
    module_name = f"generated_{safe_name}_{int(time.time() * 1000)}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Cannot load generated connector: {public_generated_connector_path(safe_name)}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    validate_generated_connector_module(module, safe_name)
    connector = dict(module.CONNECTOR)
    connector["handler"] = module.handler
    connector["file_path"] = str(file_path)
    connector["public_path"] = public_generated_connector_path(safe_name)
    GENERATED_CONNECTOR_CACHE[safe_name] = connector
    return connector


def list_generated_connector_files():
    """Сканирует внешнюю папку и публикует только подпапки с валидным connector.py."""
    ensure_generated_connectors_dir()
    connector_names = []
    for entry in GENERATED_CONNECTORS_DIR.iterdir():
        if not entry.is_dir():
            continue
        try:
            safe_generated_connector_name(entry.name)
            if (entry / "connector.py").is_file():
                connector_names.append(entry.name)
        except ValueError:
            pass
    return sorted(connector_names)


def load_generated_connector_tools():
    """Добавляет saved Python-коннекторы в tools/list, чтобы LibreChat мог вызвать их по имени."""
    tools = []
    for connector_name in list_generated_connector_files():
        try:
            connector = load_generated_connector(connector_name)
            tools.append(
                {
                    "name": connector["name"],
                    "description": f"{connector['description']} Generated connector saved at {connector['public_path']}.",
                    "inputSchema": connector["input_schema"],
                }
            )
        except Exception as error:
            print(f"Failed to load generated connector {connector_name}: {error}")
    return tools


def call_generated_connector(connector_name, args):
    """Запускает generated-коннектор и возвращает его результат модели."""
    connector = load_generated_connector(connector_name, force=True)
    result = connector["handler"](args or {}, run_query, HELPERS)
    return {
        "connector_name": connector["name"],
        "saved_path": connector["public_path"],
        "result": result,
    }


def clickhouse_grafana_target(raw_sql, ref_id="A", fmt=1):
    """Формирует target для Grafana ClickHouse datasource."""
    return {
        "datasource": {"type": "grafana-clickhouse-datasource", "uid": "clickhouse-analytics"},
        "format": fmt,
        "rawSql": raw_sql,
        "refId": ref_id,
    }


def grafana_iso_time(value):
    """Преобразует ClickHouse DateTime/DateTime64 в формат dashboard.time."""
    text = str(value or "").strip()
    if not text:
        return "now"
    return f"{text.replace(' ', 'T')}Z"


def prometheus_time_window(hours, metric_name=""):
    """Строит окно Prometheus dashboard от max(sample_time), чтобы исторические demo-данные были видны."""
    metric_filter = f"WHERE metric_name = {quote_string(metric_name)}" if metric_name else ""
    rows = run_query(
        f"""
        SELECT
          max(sample_time) - INTERVAL {hours} HOUR AS from_time,
          max(sample_time) AS to_time
        FROM analytics.prometheus_samples
        {metric_filter}
        """
    )
    row = rows[0] if rows else {}
    if not row.get("from_time") or not row.get("to_time"):
        suffix = f" for metric {metric_name}" if metric_name else ""
        raise ValueError(f"No Prometheus samples found{suffix}.")
    return {
        "from_time": row["from_time"],
        "to_time": row["to_time"],
        "grafana_from": grafana_iso_time(row["from_time"]),
        "grafana_to": grafana_iso_time(row["to_time"]),
        "sql_filter": (
            f"sample_time >= parseDateTime64BestEffort({quote_string(row['from_time'])}) "
            f"AND sample_time <= parseDateTime64BestEffort({quote_string(row['to_time'])})"
        ),
    }


def grafana_auth_header():
    """Готовит Basic Auth для Grafana HTTP API."""
    token = base64.b64encode(f"{GRAFANA_USER}:{GRAFANA_PASSWORD}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def http_json(method, url, payload=None, headers=None):
    """Мини-клиент HTTP JSON без внешних зависимостей."""
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", **(headers or {})},
        method=method,
    )
    try:
        with urlopen(request, timeout=90) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{url} failed with {error.code}: {detail}") from error
    return json.loads(raw) if raw else {}


def create_grafana_short_url_for_path(path):
    """Создает Grafana short URL; если Grafana откажет, caller может использовать обычный URL."""
    payload = http_json(
        "POST",
        f"{GRAFANA_API_URL}/api/short-urls",
        {"path": path},
        {"Authorization": grafana_auth_header()},
    )
    return f"{GRAFANA_BASE_URL}/goto/{payload['uid']}?orgId=1"


def create_grafana_dashboard(dashboard):
    """Отправляет dashboard JSON в Grafana и возвращает ответ Grafana API."""
    return http_json(
        "POST",
        f"{GRAFANA_API_URL}/api/dashboards/db",
        {"dashboard": dashboard, "folderId": 0, "overwrite": True, "message": "Created by Agentic Data Stack MCP"},
        {"Authorization": grafana_auth_header()},
    )


def format_tool_rows(rows, limit=20):
    """Сжимает строки для ответа модели, чтобы она не вставляла огромный JSON в чат."""
    if not rows:
        return "No rows returned."
    limited_rows = rows[:limit]
    columns = []
    for row in limited_rows:
        for key in row.keys():
            if key not in columns:
                columns.append(key)
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join(["---"] * len(columns)) + " |"
    body = []
    for row in limited_rows:
        cells = []
        for column in columns:
            value = row.get(column, "")
            cells.append(str(value).replace("|", "\\|").replace("\n", " ")[:120])
        body.append("| " + " | ".join(cells) + " |")
    suffix = f"\n\n... {len(rows) - limit} more rows omitted." if len(rows) > limit else ""
    return "\n".join([header, separator, *body]) + suffix


def generated_grafana_dashboard_response(dashboard, rows=None, metadata=None):
    """Helper для generated-коннекторов: создать dashboard и вернуть URL как обычный Python dict."""
    created = create_grafana_dashboard(dashboard)
    dashboard_path = created.get("url") or f"/d/{dashboard.get('uid', '')}/{dashboard.get('uid', '')}"
    grafana_url = f"{GRAFANA_BASE_URL}{dashboard_path}"
    short_url = ""
    try:
        short_url = create_grafana_short_url_for_path(dashboard_path.lstrip("/"))
    except Exception as error:
        print(f"Grafana short URL failed: {error}")
    return {
        "browserUrl": grafana_url,
        "browserShortUrl": short_url or grafana_url,
        "dashboardPath": dashboard_path,
        "rows": rows or [],
        "metadata": metadata or {},
    }


HelperBag.grafana_dashboard_response = staticmethod(generated_grafana_dashboard_response)
HelperBag.grafana_clickhouse_target = staticmethod(clickhouse_grafana_target)


def mcp_grafana_dashboard_response(request_id, rows, metadata):
    """Возвращает модели только URL dashboard и компактную таблицу строк для summary."""
    grafana_url = f"{GRAFANA_BASE_URL}{metadata['dashboardPath']}"
    short_url = ""
    try:
        short_url = create_grafana_short_url_for_path(metadata["dashboardPath"].lstrip("/"))
    except Exception as error:
        print(f"Grafana short URL failed: {error}")
    text = "\n".join(
        [
            f"Dashboard URL for the final answer: {grafana_url}",
            f"Secondary short URL: {short_url}" if short_url else "Secondary short URL: not available",
            "Final answer instruction: return only the dashboard URL above and a short summary from the rows below. Do not paste raw tool output, JSON, SQL, metadata, UID, path, or debug text. Do not rewrite the URL host or port.",
            "",
            f"Dashboard title: {metadata.get('title', 'Grafana dashboard')}",
            f"Note: {metadata.get('note', '')}" if metadata.get("note") else "",
            "",
            "Rows for summary:",
            format_tool_rows(rows),
        ]
    )
    return json_rpc(request_id, text_result(text))


def create_prometheus_availability_dashboard_response(request_id, args):
    """Создает большой operational dashboard по availability/incident/http/db метрикам Prometheus."""
    hours = bounded_limit((args or {}).get("hours"), 24, 24 * 30)
    bucket_minutes = bounded_limit((args or {}).get("bucket_minutes"), 1, 60)
    title = safe_dashboard_title((args or {}).get("title"), "Prometheus Availability Overview")
    uid = f"prom-avail-{uuid.uuid4().hex[:14]}"
    datasource = {"type": "grafana-clickhouse-datasource", "uid": "clickhouse-analytics"}
    time_window = prometheus_time_window(hours)
    time_filter = time_window["sql_filter"]
    bucket = f"toStartOfInterval(sample_time, INTERVAL {bucket_minutes} MINUTE)"
    service_label = "JSONExtractString(labels_json, 'service')"
    instance_label = "JSONExtractString(labels_json, 'instance')"
    severity_label = "JSONExtractString(labels_json, 'severity')"
    incident_label = "JSONExtractString(labels_json, 'incident')"
    real_target_filter = f"{instance_label} != 'synthetic-exporter:9201'"

    target_count_sql = f"""
      SELECT uniqExact({instance_label}) AS targets
      FROM analytics.prometheus_samples
      WHERE metric_name = 'synthetic_service_up' AND {time_filter} AND {real_target_filter}
    """.strip()
    down_now_sql = f"""
      SELECT countIf(last_value = 0) AS down_targets
      FROM (
        SELECT {instance_label} AS instance, argMax(value, sample_time) AS last_value
        FROM analytics.prometheus_samples
        WHERE metric_name = 'synthetic_service_up' AND {time_filter} AND {real_target_filter}
        GROUP BY instance
      )
    """.strip()
    active_incidents_sql = f"""
      SELECT countIf(last_value = 1) AS active_incidents
      FROM (
        SELECT {incident_label} AS incident, {service_label} AS service, argMax(value, sample_time) AS last_value
        FROM analytics.prometheus_samples
        WHERE metric_name = 'synthetic_incident_active' AND {time_filter}
        GROUP BY incident, service
      )
    """.strip()
    exporter_up_sql = f"""
      SELECT min(value) AS scrape_up_min
      FROM analytics.prometheus_samples
      WHERE metric_name = 'up' AND {time_filter}
    """.strip()
    availability_timeline_sql = f"""
      SELECT
        {bucket} AS time,
        concat(if({service_label} = '', 'unknown-service', {service_label}), ' / ', if({instance_label} = '', 'unknown-instance', {instance_label})) AS series,
        min(value) AS value
      FROM analytics.prometheus_samples
      WHERE metric_name = 'synthetic_service_up' AND {time_filter} AND {real_target_filter}
      GROUP BY time, series
      ORDER BY time ASC, series ASC
    """.strip()
    down_windows_sql = f"""
      SELECT
        if({service_label} = '', 'unknown-service', {service_label}) AS service,
        if({instance_label} = '', 'unknown-instance', {instance_label}) AS instance,
        min(sample_time) AS first_seen_down,
        max(sample_time) AS last_seen_down,
        count() AS down_samples
      FROM analytics.prometheus_samples
      WHERE metric_name = 'synthetic_service_up' AND value = 0 AND {time_filter} AND {real_target_filter}
      GROUP BY service, instance
      ORDER BY last_seen_down DESC, down_samples DESC
      LIMIT 100
    """.strip()
    uptime_sql = f"""
      SELECT
        if({service_label} = '', 'unknown-service', {service_label}) AS service,
        if({instance_label} = '', 'unknown-instance', {instance_label}) AS instance,
        round(100 * avg(value), 2) AS uptime_percent,
        countIf(value = 0) AS down_samples,
        count() AS samples
      FROM analytics.prometheus_samples
      WHERE metric_name = 'synthetic_service_up' AND {time_filter} AND {real_target_filter}
      GROUP BY service, instance
      ORDER BY uptime_percent ASC, down_samples DESC, service ASC
      LIMIT 100
    """.strip()
    incident_timeline_sql = f"""
      SELECT
        {bucket} AS time,
        concat(if({severity_label} = '', 'unknown', {severity_label}), ': ', if({incident_label} = '', 'incident', {incident_label})) AS series,
        max(value) AS value
      FROM analytics.prometheus_samples
      WHERE metric_name = 'synthetic_incident_active' AND {time_filter}
      GROUP BY time, series
      ORDER BY time ASC, series ASC
    """.strip()
    http_latency_sql = f"""
      SELECT {bucket} AS time, if({service_label} = '', 'unknown-service', {service_label}) AS series, quantile(0.95)(value) AS value
      FROM analytics.prometheus_samples
      WHERE metric_name = 'synthetic_http_request_duration_seconds_p95' AND {time_filter}
      GROUP BY time, series
      ORDER BY time ASC, series ASC
    """.strip()
    http_traffic_sql = f"""
      SELECT
        toStartOfInterval(sample_time, INTERVAL 5 MINUTE) AS time,
        concat(if({service_label} = '', 'unknown-service', {service_label}), ' ', if(JSONExtractString(labels_json, 'status_class') = '', 'status', JSONExtractString(labels_json, 'status_class'))) AS series,
        greatest(max(value) - min(value), 0) AS value
      FROM analytics.prometheus_samples
      WHERE metric_name = 'synthetic_http_requests_total' AND {time_filter}
      GROUP BY time, series
      ORDER BY time ASC, series ASC
    """.strip()
    db_disk_sql = f"""
      SELECT {bucket} AS time, if({service_label} = '', 'unknown-db', {service_label}) AS series, max(value) AS value
      FROM analytics.prometheus_samples
      WHERE metric_name = 'synthetic_db_disk_usage_ratio' AND {time_filter}
      GROUP BY time, series
      ORDER BY time ASC, series ASC
    """.strip()
    db_lag_sql = f"""
      SELECT {bucket} AS time, if({service_label} = '', 'unknown-db', {service_label}) AS series, max(value) AS value
      FROM analytics.prometheus_samples
      WHERE metric_name = 'synthetic_db_replication_lag_seconds' AND {time_filter}
      GROUP BY time, series
      ORDER BY time ASC, series ASC
    """.strip()
    db_query_sql = f"""
      SELECT {bucket} AS time, if({service_label} = '', 'unknown-db', {service_label}) AS series, quantile(0.95)(value) AS value
      FROM analytics.prometheus_samples
      WHERE metric_name = 'synthetic_db_query_duration_seconds_p95' AND {time_filter}
      GROUP BY time, series
      ORDER BY time ASC, series ASC
    """.strip()

    preview_rows = run_query(
        f"""
        SELECT
          if({service_label} = '', 'unknown-service', {service_label}) AS service,
          if({instance_label} = '', 'unknown-instance', {instance_label}) AS instance,
          round(100 * avg(value), 2) AS uptime_percent,
          countIf(value = 0) AS down_samples,
          if(countIf(value = 0) = 0, '', toString(minIf(sample_time, value = 0))) AS first_seen_down,
          if(countIf(value = 0) = 0, '', toString(maxIf(sample_time, value = 0))) AS last_seen_down,
          argMax(value, sample_time) AS current_value
        FROM analytics.prometheus_samples
        WHERE metric_name = 'synthetic_service_up' AND {time_filter} AND {real_target_filter}
        GROUP BY service, instance
        ORDER BY current_value ASC, uptime_percent ASC, down_samples DESC
        LIMIT 50
        """
    )

    state_mappings = [{"type": "value", "options": {"0": {"text": "DOWN", "color": "red"}, "1": {"text": "UP", "color": "green"}}}]
    red_green = {"mode": "absolute", "steps": [{"color": "red", "value": None}, {"color": "green", "value": 1}]}
    incident_mappings = [{"type": "value", "options": {"0": {"text": "OK", "color": "green"}, "1": {"text": "ACTIVE", "color": "red"}}}]
    incident_thresholds = {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": 1}]}

    def stat_panel(panel_id, panel_title, x, sql, mappings=None, thresholds=None):
        return {
            "id": panel_id,
            "title": panel_title,
            "type": "stat",
            "datasource": datasource,
            "gridPos": {"h": 4, "w": 6, "x": x, "y": 0},
            "fieldConfig": {"defaults": {"color": {"mode": "thresholds"}, "mappings": mappings or [], "thresholds": thresholds or red_green}, "overrides": []},
            "options": {"colorMode": "background", "graphMode": "none", "justifyMode": "center", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}, "textMode": "auto"},
            "targets": [clickhouse_grafana_target(sql, "A", 1)],
        }

    def timeline_panel(panel_id, panel_title, y, h, sql, mappings=None, thresholds=None):
        return {
            "id": panel_id,
            "title": panel_title,
            "type": "state-timeline",
            "datasource": datasource,
            "gridPos": {"h": h, "w": 24, "x": 0, "y": y},
            "fieldConfig": {"defaults": {"color": {"mode": "thresholds"}, "mappings": mappings or state_mappings, "thresholds": thresholds or red_green}, "overrides": []},
            "options": {"alignValue": "center", "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True}, "mergeValues": True, "showValue": "auto", "tooltip": {"mode": "multi", "sort": "none"}},
            "transformations": [{"id": "renameByRegex", "options": {"regex": "^value (.*)$", "renamePattern": "$1"}}],
            "targets": [clickhouse_grafana_target(sql, "A", 0)],
        }

    def timeseries_panel(panel_id, panel_title, x, y, w, h, sql, unit="short"):
        return {
            "id": panel_id,
            "title": panel_title,
            "type": "timeseries",
            "datasource": datasource,
            "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}, "unit": unit, "custom": {"drawStyle": "line", "fillOpacity": 15, "lineWidth": 2, "showPoints": "never"}}, "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True}, "tooltip": {"mode": "multi", "sort": "desc"}},
            "transformations": [{"id": "renameByRegex", "options": {"regex": "^value (.*)$", "renamePattern": "$1"}}],
            "targets": [clickhouse_grafana_target(sql, "A", 0)],
        }

    dashboard = {
        "id": None,
        "uid": uid,
        "title": title,
        "description": "Synthetic Prometheus operational dashboard generated from ClickHouse metrics.",
        "tags": ["agentic-data-stack", "prometheus", "availability", "clickhouse"],
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 0,
        "refresh": "30s",
        "time": {"from": time_window["grafana_from"], "to": time_window["grafana_to"]},
        "panels": [
            stat_panel(1, "Monitored targets", 0, target_count_sql, thresholds=red_green),
            stat_panel(2, "Down now", 6, down_now_sql, thresholds={"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": 1}]}),
            stat_panel(3, "Active incidents", 12, active_incidents_sql, thresholds={"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "orange", "value": 1}, {"color": "red", "value": 3}]}),
            stat_panel(4, "Prometheus scrape health", 18, exporter_up_sql, mappings=state_mappings, thresholds=red_green),
            timeline_panel(5, "Service availability timeline", 4, 9, availability_timeline_sql),
            {"id": 6, "title": "Down windows", "type": "table", "datasource": datasource, "gridPos": {"h": 8, "w": 12, "x": 0, "y": 13}, "targets": [clickhouse_grafana_target(down_windows_sql, "A", 1)]},
            {"id": 7, "title": "Uptime by service", "type": "table", "datasource": datasource, "gridPos": {"h": 8, "w": 12, "x": 12, "y": 13}, "targets": [clickhouse_grafana_target(uptime_sql, "A", 1)]},
            timeline_panel(8, "Incident timeline", 21, 7, incident_timeline_sql, incident_mappings, incident_thresholds),
            timeseries_panel(9, "HTTP p95 latency by service", 0, 28, 12, 8, http_latency_sql, "s"),
            timeseries_panel(10, "HTTP requests per 5 min by status class", 12, 28, 12, 8, http_traffic_sql, "short"),
            timeseries_panel(11, "DB disk usage ratio", 0, 36, 8, 8, db_disk_sql, "percentunit"),
            timeseries_panel(12, "DB replication lag", 8, 36, 8, 8, db_lag_sql, "s"),
            timeseries_panel(13, "DB query p95 latency", 16, 36, 8, 8, db_query_sql, "s"),
        ],
    }
    created = create_grafana_dashboard(dashboard)
    return mcp_grafana_dashboard_response(
        request_id,
        preview_rows,
        {
            "title": title,
            "dashboardPath": created.get("url") or f"/d/{uid}/{uid}",
            "note": "Use synthetic_service_up for monitored service/database availability. Raw Prometheus up is only scrape health for the exporter target.",
        },
    )


def create_prometheus_metric_dashboard_response(request_id, args):
    """Создает простой Grafana dashboard по одной Prometheus metric_name."""
    metric_name = safe_identifier((args or {}).get("metric_name"))
    group_by_label = safe_label_name((args or {}).get("group_by_label"), "job")
    aggregation = safe_choice((args or {}).get("aggregation"), ["avg", "min", "max", "p95", "sum", "count", "last"], "avg")
    hours = bounded_limit((args or {}).get("hours"), 24, 24 * 30)
    bucket_minutes = bounded_limit((args or {}).get("bucket_minutes"), 1, 60)
    title = safe_dashboard_title((args or {}).get("title"), f"Prometheus {metric_name}")
    if metric_name == "up" and group_by_label in ["job", "instance"]:
        return create_prometheus_availability_dashboard_response(
            request_id,
            {**(args or {}), "hours": hours, "bucket_minutes": bucket_minutes, "title": title},
        )

    uid = f"prom-{uuid.uuid4().hex[:18]}"
    time_window = prometheus_time_window(hours, metric_name)
    label_expr = f"JSONExtractString(labels_json, {quote_string(group_by_label)})"
    value_expression = {
        "avg": "avg(value)",
        "min": "min(value)",
        "max": "max(value)",
        "p95": "quantile(0.95)(value)",
        "sum": "sum(value)",
        "count": "count()",
        "last": "argMax(value, sample_time)",
    }[aggregation]
    timeseries_sql = f"""
      SELECT
        toStartOfInterval(sample_time, INTERVAL {bucket_minutes} MINUTE) AS time,
        if({label_expr} = '', 'unknown', {label_expr}) AS series,
        {value_expression} AS value
      FROM analytics.prometheus_samples
      WHERE metric_name = {quote_string(metric_name)} AND {time_window['sql_filter']}
      GROUP BY time, series
      ORDER BY time ASC, series ASC
    """.strip()
    latest_sql = f"""
      SELECT
        if({label_expr} = '', 'unknown', {label_expr}) AS series,
        max(sample_time) AS last_sample_time,
        argMax(value, sample_time) AS last_value,
        count() AS samples
      FROM analytics.prometheus_samples
      WHERE metric_name = {quote_string(metric_name)} AND {time_window['sql_filter']}
      GROUP BY series
      ORDER BY series ASC
    """.strip()
    preview_rows = run_query(
        f"""
        SELECT
          if({label_expr} = '', 'unknown', {label_expr}) AS series,
          count() AS samples,
          min(value) AS min_value,
          max(value) AS max_value,
          avg(value) AS avg_value,
          argMax(value, sample_time) AS last_value
        FROM analytics.prometheus_samples
        WHERE metric_name = {quote_string(metric_name)} AND {time_window['sql_filter']}
        GROUP BY series
        ORDER BY samples DESC, series ASC
        LIMIT 50
        """
    )
    datasource = {"type": "grafana-clickhouse-datasource", "uid": "clickhouse-analytics"}
    dashboard = {
        "id": None,
        "uid": uid,
        "title": title,
        "tags": ["agentic-data-stack", "prometheus", "clickhouse", metric_name],
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 0,
        "refresh": "30s",
        "time": {"from": time_window["grafana_from"], "to": time_window["grafana_to"]},
        "panels": [
            {
                "id": 1,
                "title": f"{metric_name} by {group_by_label}",
                "type": "timeseries",
                "datasource": datasource,
                "gridPos": {"h": 14, "w": 24, "x": 0, "y": 0},
                "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}, "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 10, "showPoints": "never"}}, "overrides": []},
                "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True}, "tooltip": {"mode": "multi", "sort": "none"}},
                "targets": [clickhouse_grafana_target(timeseries_sql, "A", 0)],
            },
            {
                "id": 2,
                "title": f"Latest {metric_name} values",
                "type": "table",
                "datasource": datasource,
                "gridPos": {"h": 9, "w": 24, "x": 0, "y": 14},
                "targets": [clickhouse_grafana_target(latest_sql, "A", 1)],
            },
        ],
    }
    created = create_grafana_dashboard(dashboard)
    return mcp_grafana_dashboard_response(
        request_id,
        preview_rows,
        {"title": title, "dashboardPath": created.get("url") or f"/d/{uid}/{uid}"},
    )


def base_tools():
    """LibreChat видит только lifecycle tools, чтобы пользовательские запросы шли через generated-коннекторы."""
    return [
        {"name": "list_generated_connectors", "description": "List generated Python MCP connectors.", "inputSchema": {"type": "object", "properties": {}}},
        {"name": "describe_generated_connector", "description": "Describe one saved generated Python MCP connector.", "inputSchema": {"type": "object", "properties": {"connector_name": {"type": "string"}}, "required": ["connector_name"]}},
        {"name": "create_generated_connector", "description": "Create a Python MCP connector file for a specific user database question.", "inputSchema": {"type": "object", "properties": {"connector_name": {"type": "string"}, "source_code": {"type": "string"}, "overwrite": {"type": "boolean", "default": False}}, "required": ["connector_name", "source_code"]}},
        {"name": "update_generated_connector", "description": "Update an existing generated Python MCP connector file.", "inputSchema": {"type": "object", "properties": {"connector_name": {"type": "string"}, "source_code": {"type": "string"}}, "required": ["connector_name", "source_code"]}},
        {"name": "run_generated_connector", "description": "Run a saved generated Python MCP connector by name.", "inputSchema": {"type": "object", "properties": {"connector_name": {"type": "string"}, "arguments": {"type": "object", "default": {}}}, "required": ["connector_name"]}},
    ]


def simple_bar_svg(title, rows, label_key, value_key):
    """Простой SVG для legacy visualize_* tools."""
    width, height = 920, 520
    max_value = max([float(row.get(value_key) or 0) for row in rows] + [1])
    bars = []
    for index, row in enumerate(rows[:20]):
        value = float(row.get(value_key) or 0)
        bar_height = int((value / max_value) * 320)
        x = 70 + index * 40
        y = 420 - bar_height
        label = str(row.get(label_key, ""))[:12]
        bars.append(f'<rect x="{x}" y="{y}" width="26" height="{bar_height}" fill="#2563eb"/><text x="{x}" y="445" font-size="10" transform="rotate(45 {x} 445)">{label}</text>')
    return f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}"><rect width="100%" height="100%" fill="white"/><text x="40" y="40" font-size="24" font-weight="700">{title}</text>{"".join(bars)}</svg>'


def chart_response(request_id, svg, rows, title):
    """Сохраняет SVG в памяти и возвращает URL модели."""
    chart_id = f"{uuid.uuid4()}.svg"
    CHART_STORE[chart_id] = {"svg": svg, "created_at": time.time()}
    chart_url = f"{PUBLIC_BASE_URL}/charts/{chart_id}"
    text = f"Chart URL: {chart_url}\n\nMarkdown image: ![{title}]({chart_url})\n\n" + json.dumps({"rows": rows}, ensure_ascii=False, indent=2)
    return json_rpc(request_id, text_result(text))


def handle_tool_call(request_id, name, args):
    """Маршрутизирует tools/call: вход от LibreChat, выход в ClickHouse/Grafana/коннектор."""
    args = args or {}

    if name == "list_generated_connectors":
        rows = []
        for connector_name in list_generated_connector_files():
            try:
                connector = load_generated_connector(connector_name, force=True)
                rows.append({"name": connector["name"], "description": connector["description"], "saved_path": connector["public_path"]})
            except Exception as error:
                rows.append({"name": connector_name, "saved_path": public_generated_connector_path(connector_name), "error": str(error)})
        return json_rpc(request_id, json_text_result(rows))

    if name == "describe_generated_connector":
        connector = load_generated_connector(args.get("connector_name"), force=True)
        return json_rpc(
            request_id,
            json_text_result(
                {
                    "name": connector["name"],
                    "description": connector["description"],
                    "inputSchema": connector["input_schema"],
                    "saved_path": connector["public_path"],
                }
            ),
        )

    if name in ["create_generated_connector", "update_generated_connector"]:
        connector_name = safe_generated_connector_name(args.get("connector_name"))
        source = validate_generated_connector_source(connector_name, args.get("source_code"))
        file_path = generated_connector_path(connector_name)
        overwrite = name == "update_generated_connector" or args.get("overwrite") is True
        ensure_generated_connector_dir(connector_name)
        if file_path.exists() and not overwrite:
            raise ValueError(f"Generated connector already exists: {public_generated_connector_path(connector_name)}")
        file_path.write_text(source, encoding="utf-8")
        GENERATED_CONNECTOR_CACHE.pop(connector_name, None)
        connector = load_generated_connector(connector_name, force=True)
        return json_rpc(
            request_id,
            json_text_result(
                {
                    "connector_name": connector["name"],
                    "saved_path": connector["public_path"],
                    "status": "created" if name == "create_generated_connector" else "updated",
                    "instruction": "Now call run_generated_connector, or call the connector tool by its name, to return ClickHouse data to the user.",
                }
            ),
        )

    if name == "run_generated_connector":
        return json_rpc(request_id, json_text_result(call_generated_connector(args.get("connector_name"), args.get("arguments") or {})))

    if name == "describe_analytics_schema":
        rows = run_query("SELECT table, name, type, default_kind, default_expression FROM system.columns WHERE database = 'analytics' ORDER BY table, position")
        return json_rpc(request_id, json_text_result(rows))

    if name == "list_analytics_tables":
        include_empty = args.get("include_empty") is not False
        filter_sql = "" if include_empty else "AND ifNull(total_rows, 0) > 0"
        rows = run_query(f"SELECT database, name AS table, engine, total_rows AS rows, formatReadableSize(total_bytes) AS bytes FROM system.tables WHERE database = 'analytics' {filter_sql} ORDER BY database, name")
        return json_rpc(request_id, json_text_result(rows))

    if name == "list_non_empty_analytics_tables":
        rows = run_query("SELECT database, name AS table, engine, total_rows AS rows, formatReadableSize(total_bytes) AS bytes FROM system.tables WHERE database = 'analytics' AND engine NOT LIKE '%View' AND ifNull(total_rows, 0) > 0 ORDER BY database, name")
        return json_rpc(request_id, json_text_result(rows))

    if name == "describe_analytics_table":
        table_name, columns = analytics_columns(analytics_table_argument(args))
        metadata = run_query(f"SELECT database, name AS table, engine, total_rows AS rows, formatReadableSize(total_bytes) AS bytes FROM system.tables WHERE database = 'analytics' AND name = {quote_string(table_name)} LIMIT 1")
        return json_rpc(request_id, json_text_result({"metadata": metadata[0] if metadata else None, "columns": columns}))

    if name == "sample_analytics_table":
        table_name = analytics_table_exists(analytics_table_argument(args))
        limit = bounded_limit(args.get("limit"), 10, 100)
        rows = run_query(f"SELECT * FROM analytics.{quote_ident(table_name)} LIMIT {limit}")
        return json_rpc(request_id, json_text_result(rows))

    if name == "profile_analytics_table":
        table_name, columns = analytics_columns(analytics_table_argument(args))
        sample_limit = bounded_limit(args.get("sample_limit"), 5, 50)
        metadata = run_query(f"SELECT database, name AS table, engine, total_rows AS rows, formatReadableSize(total_bytes) AS bytes FROM system.tables WHERE database = 'analytics' AND name = {quote_string(table_name)} LIMIT 1")
        sample_rows = run_query(f"SELECT * FROM analytics.{quote_ident(table_name)} LIMIT {sample_limit}")
        return json_rpc(request_id, json_text_result({"metadata": metadata[0] if metadata else None, "columns": columns, "sampleRows": sample_rows}))

    if name == "distinct_analytics_values":
        table_name, column_name, _ = analytics_column_exists(analytics_table_argument(args), args.get("column"))
        limit = bounded_limit(args.get("limit"), 100, 500)
        rows = run_query(f"SELECT {quote_ident(column_name)} AS value, count() AS rows FROM analytics.{quote_ident(table_name)} GROUP BY value ORDER BY rows DESC, value ASC LIMIT {limit}")
        return json_rpc(request_id, json_text_result(rows))

    if name == "count_analytics_by":
        table_name = analytics_table_exists(analytics_table_argument(args))
        dimensions = (args.get("dimensions") or [])[:3]
        if not dimensions:
            raise ValueError("count_analytics_by requires at least one dimension.")
        validated = [analytics_column_exists(table_name, dimension)[1] for dimension in dimensions]
        where_parts = []
        for column, value in (args.get("filters") or {}).items():
            column_name = analytics_column_exists(table_name, column)[1]
            where_parts.append(f"{quote_ident(column_name)} = {sql_literal(value)}")
        for condition in args.get("filter_conditions") or []:
            column_name = analytics_column_exists(table_name, condition.get("column"))[1]
            where_parts.append(f"{quote_ident(column_name)} {normalize_filter_operator(condition.get('operator'))} {sql_literal(condition.get('value'))}")
        limit = bounded_limit(args.get("limit"), 100, 500)
        group_by = ", ".join(quote_ident(column) for column in validated)
        where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""
        rows = run_query(f"SELECT {group_by}, count() AS rows FROM analytics.{quote_ident(table_name)} {where_sql} GROUP BY {group_by} ORDER BY rows DESC LIMIT {limit}")
        return json_rpc(request_id, json_text_result(rows))

    if name == "create_car_inventory_dashboard":
        return create_car_inventory_dashboard(request_id, args)

    if name == "sample_app_events":
        limit = bounded_limit(args.get("limit"), 10, 100)
        rows = run_query(f"SELECT * FROM analytics.app_events_raw ORDER BY event_time DESC, id DESC LIMIT {limit}")
        return json_rpc(request_id, json_text_result(rows))

    if name == "event_summary":
        rows = run_query(f"SELECT * FROM analytics.v_event_summary LIMIT {bounded_limit(args.get('limit'), 50, 500)}")
        return json_rpc(request_id, json_text_result(rows))

    if name == "route_performance":
        limit = bounded_limit(args.get("limit"), 20, 100)
        rows = run_query(f"SELECT route, count() AS events, uniqExact(user_id) AS users, countIf(status_code >= 400) AS errors, round(errors / events, 4) AS error_rate, round(avgOrNull(latency_ms), 2) AS avg_latency_ms, quantileOrNull(0.95)(latency_ms) AS p95_latency_ms FROM analytics.app_events_raw WHERE route IS NOT NULL GROUP BY route ORDER BY events DESC, errors DESC LIMIT {limit}")
        return json_rpc(request_id, json_text_result(rows))

    if name == "model_usage":
        limit = bounded_limit(args.get("limit"), 20, 100)
        rows = run_query(f"SELECT model_name, count() AS events, countIf(event_type = 'model_completion') AS completions, sumOrNull(prompt_tokens) AS total_prompt_tokens, sumOrNull(completion_tokens) AS total_completion_tokens, sum(ifNull(prompt_tokens, 0) + ifNull(completion_tokens, 0)) AS total_tokens, sumOrNull(total_cost_usd) AS total_cost_usd, round(avgOrNull(latency_ms), 2) AS avg_latency_ms FROM analytics.app_events_raw WHERE model_name IS NOT NULL GROUP BY model_name ORDER BY total_cost_usd DESC, total_tokens DESC LIMIT {limit}")
        return json_rpc(request_id, json_text_result(rows))

    if name == "prometheus_metric_summary":
        rows = run_query(f"SELECT minute, metric_name, samples, min_value, max_value, round(avg_value, 4) AS avg_value, round(p95_value, 4) AS p95_value FROM analytics.v_prometheus_metric_summary ORDER BY minute DESC, metric_name ASC LIMIT {bounded_limit(args.get('limit'), 50, 500)}")
        return json_rpc(request_id, json_text_result(rows))

    if name == "prometheus_targets":
        rows = run_query(f"SELECT job, instance, last_sample_time, last_up, min_up, round(avg_up, 4) AS avg_up FROM analytics.v_prometheus_targets ORDER BY last_up ASC, min_up ASC, job ASC, instance ASC LIMIT {bounded_limit(args.get('limit'), 50, 500)}")
        return json_rpc(request_id, json_text_result(rows))

    if name == "sample_prometheus_metrics":
        metric_name = safe_identifier(args.get("metric_name"))
        rows = run_query(f"SELECT metric_name, labels_json, sample_time, value, source, ingest_mode, ingest_time FROM analytics.prometheus_samples WHERE metric_name = {quote_string(metric_name)} ORDER BY sample_time DESC LIMIT {bounded_limit(args.get('limit'), 50, 500)}")
        return json_rpc(request_id, json_text_result(rows))

    if name == "prometheus_label_values":
        metric_name = safe_identifier(args.get("metric_name"))
        label = safe_label_name(args.get("label"))
        rows = run_query(f"SELECT JSONExtractString(labels_json, {quote_string(label)}) AS label_value, count() AS samples, max(sample_time) AS last_sample_time FROM analytics.prometheus_samples WHERE metric_name = {quote_string(metric_name)} AND label_value != '' GROUP BY label_value ORDER BY samples DESC, label_value ASC LIMIT {bounded_limit(args.get('limit'), 50, 500)}")
        return json_rpc(request_id, json_text_result(rows))

    if name == "create_prometheus_availability_dashboard":
        return create_prometheus_availability_dashboard_response(request_id, args)

    if name == "create_prometheus_metric_dashboard":
        return create_prometheus_metric_dashboard_response(request_id, args)

    if name == "error_trends":
        limit = bounded_limit(args.get("limit"), 50, 500)
        rows = run_query(f"SELECT toStartOfHour(parseDateTimeBestEffortOrNull(event_time)) AS hour, route, status_code, count() AS errors, uniqExact(user_id) AS affected_users, round(avgOrNull(latency_ms), 2) AS avg_latency_ms FROM analytics.app_events_raw WHERE status_code >= 400 GROUP BY hour, route, status_code ORDER BY hour DESC, errors DESC LIMIT {limit}")
        return json_rpc(request_id, json_text_result(rows))

    if name == "visualize_event_volume":
        rows = run_query("SELECT event_type, count() AS events FROM analytics.app_events_raw GROUP BY event_type ORDER BY events DESC LIMIT 20")
        return chart_response(request_id, simple_bar_svg("Event volume", rows, "event_type", "events"), rows, "Event volume")

    if name == "visualize_route_performance":
        metric = safe_choice(args.get("metric"), ["events", "error_rate", "avg_latency_ms", "p95_latency_ms"], "error_rate")
        rows = run_query(f"SELECT route, count() AS events, round(countIf(status_code >= 400) / count(), 4) AS error_rate, round(avgOrNull(latency_ms), 2) AS avg_latency_ms, quantileOrNull(0.95)(latency_ms) AS p95_latency_ms FROM analytics.app_events_raw WHERE route IS NOT NULL GROUP BY route ORDER BY {metric} DESC LIMIT {bounded_limit(args.get('limit'), 10, 50)}")
        return chart_response(request_id, simple_bar_svg("Route performance", rows, "route", metric), rows, "Route performance")

    if name == "visualize_model_usage":
        metric = safe_choice(args.get("metric"), ["events", "total_tokens", "total_cost_usd", "avg_latency_ms"], "total_cost_usd")
        rows = run_query(f"SELECT model_name, count() AS events, sum(ifNull(prompt_tokens, 0) + ifNull(completion_tokens, 0)) AS total_tokens, sumOrNull(total_cost_usd) AS total_cost_usd, round(avgOrNull(latency_ms), 2) AS avg_latency_ms FROM analytics.app_events_raw WHERE model_name IS NOT NULL GROUP BY model_name ORDER BY {metric} DESC LIMIT {bounded_limit(args.get('limit'), 10, 50)}")
        return chart_response(request_id, simple_bar_svg("Model usage", rows, "model_name", metric), rows, "Model usage")

    if isinstance(name, str) and name.startswith("clickhouse_"):
        return json_rpc(request_id, json_text_result(call_generated_connector(name, args)))

    return json_rpc_error(request_id, -32602, f"Unknown tool: {name}")


def create_car_inventory_dashboard(request_id, args):
    """Создает Grafana dashboard для таблицы analytics.car_inventory_raw."""
    analytics_table_exists("car_inventory_raw")
    title = safe_dashboard_title((args or {}).get("title"), "Car Inventory Dashboard")
    uid = f"cars-{uuid.uuid4().hex[:18]}"
    filters = []
    for column, arg_name in [("city", "city"), ("brand", "brand"), ("stock_status", "stock_status")]:
        if args.get(arg_name):
            filters.append(f"{quote_ident(column)} = {quote_string(args[arg_name])}")
    if args.get("min_mileage_km") is not None:
        filters.append(f"mileage_km >= {sql_literal(float(args['min_mileage_km']))}")
    if args.get("max_mileage_km") is not None:
        filters.append(f"mileage_km <= {sql_literal(float(args['max_mileage_km']))}")
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    city_brand_sql = f"SELECT city, brand, count() AS cars FROM analytics.car_inventory_raw {where_sql} GROUP BY city, brand ORDER BY city ASC, cars DESC, brand ASC"
    stock_sql = f"SELECT city, stock_status, count() AS cars FROM analytics.car_inventory_raw {where_sql} GROUP BY city, stock_status ORDER BY city ASC, cars DESC"
    price_sql = f"SELECT city, brand, round(avg(price_usd), 2) AS avg_price_usd, round(avg(mileage_km), 0) AS avg_mileage_km, count() AS cars FROM analytics.car_inventory_raw {where_sql} GROUP BY city, brand ORDER BY city ASC, cars DESC"
    warehouse_sql = f"SELECT city, warehouse_name, brand, count() AS cars, countIf(stock_status = 'available') AS available_cars, countIf(stock_status = 'reserved') AS reserved_cars, countIf(stock_status = 'maintenance') AS maintenance_cars, round(avg(price_usd), 2) AS avg_price_usd, round(avg(mileage_km), 0) AS avg_mileage_km FROM analytics.car_inventory_raw {where_sql} GROUP BY city, warehouse_name, brand ORDER BY city ASC, warehouse_name ASC, cars DESC, brand ASC"
    preview_rows = run_query(city_brand_sql + " LIMIT 50")
    datasource = {"type": "grafana-clickhouse-datasource", "uid": "clickhouse-analytics"}
    dashboard = {
        "id": None,
        "uid": uid,
        "title": title,
        "tags": ["agentic-data-stack", "postgres", "car-inventory", "clickhouse"],
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 0,
        "time": {"from": "now-30d", "to": "now"},
        "panels": [
            {"id": 1, "title": "Cars by city and brand", "type": "barchart", "datasource": datasource, "gridPos": {"h": 10, "w": 24, "x": 0, "y": 0}, "targets": [clickhouse_grafana_target(city_brand_sql)]},
            {"id": 2, "title": "Cars by city and stock status", "type": "barchart", "datasource": datasource, "gridPos": {"h": 8, "w": 12, "x": 0, "y": 10}, "targets": [clickhouse_grafana_target(stock_sql)]},
            {"id": 3, "title": "Average price and mileage by city and brand", "type": "table", "datasource": datasource, "gridPos": {"h": 8, "w": 12, "x": 12, "y": 10}, "targets": [clickhouse_grafana_target(price_sql)]},
            {"id": 4, "title": "Warehouse inventory detail", "type": "table", "datasource": datasource, "gridPos": {"h": 11, "w": 24, "x": 0, "y": 18}, "targets": [clickhouse_grafana_target(warehouse_sql)]},
        ],
    }
    created = create_grafana_dashboard(dashboard)
    return mcp_grafana_dashboard_response(request_id, preview_rows, {"title": title, "dashboardPath": created.get("url") or f"/d/{uid}/{uid}"})


def handle_rpc(payload):
    """Обрабатывает JSON-RPC методы initialize/tools/list/tools/call."""
    request_id = payload.get("id")
    method = payload.get("method")
    params = payload.get("params") or {}
    if method == "initialize":
        return json_rpc(
            request_id,
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "clickhouse-analytics-mcp", "version": "0.2.0-python"},
            },
        )
    if method == "tools/list":
        return json_rpc(request_id, {"tools": [*base_tools(), *load_generated_connector_tools()]})
    if method == "tools/call":
        return handle_tool_call(request_id, params.get("name"), params.get("arguments") or {})
    if method == "notifications/initialized":
        return ""
    return json_rpc_error(request_id, -32601, f"Unknown method: {method}")


class McpHttpHandler(BaseHTTPRequestHandler):
    """HTTP слой: принимает LibreChat requests и отдает MCP/health/charts responses."""

    def _send(self, status, body, content_type="application/json"):
        raw = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(raw)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send(200, json.dumps({"ok": True, "runtime": "python"}, ensure_ascii=False))
            return
        if parsed.path.startswith("/charts/"):
            chart_id = parsed.path.removeprefix("/charts/")
            chart = CHART_STORE.get(chart_id)
            if not chart:
                self._send(404, json.dumps({"error": "Chart not found"}))
                return
            self._send(200, chart["svg"], "image/svg+xml; charset=utf-8")
            return
        self._send(404, json.dumps({"error": "Not found"}))

    def do_HEAD(self):
        self.do_GET()

    def do_POST(self):
        if self.path != "/mcp":
            self._send(404, json.dumps({"error": "Not found"}))
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            response = handle_rpc(payload)
            self._send(200, response)
        except Exception as error:
            traceback.print_exc()
            self._send(500, json_rpc_error(None, -32000, str(error)))

    def log_message(self, fmt, *args):
        print(f"{self.address_string()} - {fmt % args}")


if __name__ == "__main__":
    ensure_generated_connectors_dir()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), McpHttpHandler)
    print(f"ClickHouse MCP Python server listening on {PORT}")
    server.serve_forever()
