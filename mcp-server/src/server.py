import base64
import importlib.util
import json
import os
import re
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


# Этот файл заменяет прежний Node.js MCP server.
# Данные идут так:
# LibreChat -> HTTP POST /mcp -> этот Python server -> generated Python-коннектор -> ClickHouse/Grafana -> ответ обратно в LibreChat.

PORT = int(os.getenv("PORT", "3333"))
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


def safe_sql_identifier(value, fallback=""):
    """Проверяет SQL identifier: таблицы и колонки без кавычек и спецсимволов."""
    text = str(value or fallback)
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", text):
        raise ValueError(f"Unsafe SQL identifier: {text}")
    return text


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


def base_tools():
    """LibreChat видит только lifecycle tools, чтобы пользовательские запросы шли через generated-коннекторы."""
    return [
        {"name": "list_generated_connectors", "description": "List generated Python MCP connectors.", "inputSchema": {"type": "object", "properties": {}}},
        {"name": "describe_generated_connector", "description": "Describe one saved generated Python MCP connector.", "inputSchema": {"type": "object", "properties": {"connector_name": {"type": "string"}}, "required": ["connector_name"]}},
        {"name": "create_generated_connector", "description": "Create a Python MCP connector file for a specific user database question.", "inputSchema": {"type": "object", "properties": {"connector_name": {"type": "string"}, "source_code": {"type": "string"}, "overwrite": {"type": "boolean", "default": False}}, "required": ["connector_name", "source_code"]}},
        {"name": "update_generated_connector", "description": "Update an existing generated Python MCP connector file.", "inputSchema": {"type": "object", "properties": {"connector_name": {"type": "string"}, "source_code": {"type": "string"}}, "required": ["connector_name", "source_code"]}},
        {"name": "run_generated_connector", "description": "Run a saved generated Python MCP connector by name.", "inputSchema": {"type": "object", "properties": {"connector_name": {"type": "string"}, "arguments": {"type": "object", "default": {}}}, "required": ["connector_name"]}},
    ]


def handle_tool_call(request_id, name, args):
    """Маршрутизирует tools/call: lifecycle tools или запуск saved generated-коннектора."""
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

    if isinstance(name, str) and name.startswith("clickhouse_"):
        return json_rpc(request_id, json_text_result(call_generated_connector(name, args)))

    return json_rpc_error(request_id, -32602, f"Unknown tool: {name}")


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
    """HTTP слой: принимает LibreChat requests и отдает MCP/health responses."""

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
