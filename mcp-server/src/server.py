import base64
import importlib.util
import json
import os
import re
import shutil
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
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
LANGFUSE_ENABLED = os.getenv("LANGFUSE_ENABLED", "false").lower() == "true"
LANGFUSE_BASE_URL = os.getenv("LANGFUSE_BASE_URL", "").rstrip("/")
LANGFUSE_PUBLIC_KEY = os.getenv("LANGFUSE_PUBLIC_KEY", "")
LANGFUSE_SECRET_KEY = os.getenv("LANGFUSE_SECRET_KEY", "")
LANGFUSE_ENVIRONMENT = os.getenv("LANGFUSE_ENVIRONMENT", "local")

# Python-модули generated-коннекторов кэшируются, но при create/update конкретный модуль перечитывается.
GENERATED_CONNECTOR_CACHE = {}
TRANSLIT_CHARS = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e", "ж": "zh", "з": "z",
    "и": "i", "й": "i", "к": "k", "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r",
    "с": "s", "т": "t", "у": "u", "ф": "f", "х": "h", "ц": "ts", "ч": "ch", "ш": "sh",
    "щ": "sch", "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}
TRACE_CONTEXT = threading.local()


def utc_now_iso():
    """Возвращает ISO timestamp для Langfuse."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def json_bytes(value):
    """Сериализует Python-объект в JSON bytes."""
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def truncate_for_trace(value, limit=8000):
    """Ограничивает payload для Langfuse, чтобы trace не превращался в огромный дамп."""
    try:
        text = json.dumps(value, ensure_ascii=False)
    except TypeError:
        text = str(value)
    if len(text) <= limit:
        return value
    return {"truncated": True, "chars": len(text), "preview": text[:limit]}


def redacted_tool_args(tool_name, args):
    """Убирает большие source_code payloads из trace input."""
    clean = dict(args or {})
    if tool_name in {"create_generated_connector", "update_generated_connector"} and "source_code" in clean:
        source = str(clean.get("source_code") or "")
        clean["source_code"] = {"chars": len(source), "preview": source[:600]}
    return clean


def langfuse_auth_header():
    """Готовит Basic Auth для Langfuse ingestion API."""
    token = base64.b64encode(f"{LANGFUSE_PUBLIC_KEY}:{LANGFUSE_SECRET_KEY}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def send_langfuse_batch(events):
    """Отправляет batch событий в Langfuse. Ошибка трейсинга не ломает MCP."""
    if not (LANGFUSE_ENABLED and LANGFUSE_BASE_URL and LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY):
        return
    payload = {"batch": events, "metadata": {"source": "agentic-data-stack-mcp-server"}}
    try:
        request = Request(
            f"{LANGFUSE_BASE_URL}/api/public/ingestion",
            data=json_bytes(payload),
            headers={"Authorization": langfuse_auth_header(), "Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=3) as response:
            if response.status not in (200, 207):
                print(f"Langfuse MCP ingestion returned HTTP {response.status}")
    except Exception as error:
        print(f"Langfuse MCP ingestion skipped: {error}")


def send_langfuse_span(name, started_at, ended_at, input_value=None, output_value=None, metadata=None, level="DEFAULT", status_message=None):
    """Пишет span в текущий trace, если tool call уже создал trace context."""
    trace_id = getattr(TRACE_CONTEXT, "trace_id", None)
    parent_id = getattr(TRACE_CONTEXT, "span_id", None)
    if not trace_id:
        return
    body = {
        "id": str(uuid.uuid4()),
        "traceId": trace_id,
        "parentObservationId": parent_id,
        "name": name,
        "startTime": started_at,
        "endTime": ended_at,
        "input": truncate_for_trace(input_value),
        "output": truncate_for_trace(output_value),
        "metadata": metadata or {},
        "level": level,
        "statusMessage": status_message,
    }
    send_langfuse_batch(
        [
            {
                "id": str(uuid.uuid4()),
                "type": "span-create",
                "timestamp": utc_now_iso(),
                "body": body,
            }
        ]
    )


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


def normalize_analytics_sql(query):
    """Исправляет частые model shortcuts на реальные analytics-таблицы."""
    normalized = str(query or "").strip().rstrip(";")
    replacements = {
        r"\bcars\b": "v_car_inventory_summary",
        r"\bcar_inventory\b": "v_car_inventory_summary",
    }
    for pattern, replacement in replacements.items():
        normalized = re.sub(rf"(\bfrom\s+){pattern}\b", rf"\1{replacement}", normalized, flags=re.IGNORECASE)
        normalized = re.sub(rf"(\bjoin\s+){pattern}\b", rf"\1{replacement}", normalized, flags=re.IGNORECASE)
    if re.search(r"\bfrom\s+v_car_inventory_summary\b", normalized, flags=re.IGNORECASE):
        normalized = re.sub(
            r"\bcount\s*\(\s*\*\s*\)\s+as\s+([A-Za-z_][A-Za-z0-9_]*)",
            r"sum(cars) AS \1",
            normalized,
            flags=re.IGNORECASE,
        )
    return normalized


def run_query(query):
    """Выполняет только SELECT в ClickHouse и возвращает JSONEachRow как list[dict]."""
    started_at = utc_now_iso()
    normalized = normalize_analytics_sql(query)
    try:
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
        rows = [] if not body else [json.loads(line) for line in body.splitlines() if line.strip()]
        send_langfuse_span(
            "mcp.clickhouse.query",
            started_at,
            utc_now_iso(),
            input_value={"query": normalized},
            output_value={"row_count": len(rows), "sample": rows[:5]},
            metadata={"database": CLICKHOUSE_DATABASE},
        )
        return rows
    except Exception as error:
        send_langfuse_span(
            "mcp.clickhouse.query",
            started_at,
            utc_now_iso(),
            input_value={"query": normalized},
            output_value={"error": str(error)},
            metadata={"database": CLICKHOUSE_DATABASE},
            level="ERROR",
            status_message=str(error),
        )
        raise


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


def analytics_list_tables(include_views=True):
    """Возвращает список таблиц и VIEW из analytics для schema-discovery коннекторов."""
    engine_filter = "" if include_views else "AND engine NOT LIKE '%View%'"
    return run_query(
        f"""
        SELECT
          name,
          engine,
          ifNull(total_rows, 0) AS rows,
          formatReadableSize(ifNull(total_bytes, 0)) AS bytes
        FROM system.tables
        WHERE database = 'analytics'
          {engine_filter}
        ORDER BY name
        """
    )


def analytics_table_columns(table):
    """Возвращает имя таблицы и колонки из system.columns для внутренних проверок."""
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


def analytics_columns(table):
    """Возвращает список колонок таблицы из system.columns для generated-коннекторов."""
    _, rows = analytics_table_columns(table)
    return rows


def analytics_schema(include_views=True):
    """Возвращает таблицы/VIEW analytics вместе с колонками для первого schema-коннектора."""
    schema = []
    for table in analytics_list_tables(include_views=include_views):
        table_name = table.get("name")
        columns = analytics_columns(table_name)
        schema.append(
            {
                "name": table_name,
                "engine": table.get("engine"),
                "rows": table.get("rows"),
                "columns": [column.get("name") for column in columns],
                "column_types": {column.get("name"): column.get("type") for column in columns},
            }
        )
    return {"database": "analytics", "tables": schema}


def question_schema_terms(question):
    """Подбирает поисковые термины по вопросу пользователя для компактной schema discovery."""
    text = str(question or "").lower()
    terms = set(re.findall(r"[a-zа-яё0-9_]+", text))
    aliases = {
        "машин": ["car", "vehicle", "auto", "city", "make", "model"],
        "машина": ["car", "vehicle", "auto", "city", "make", "model"],
        "машины": ["car", "vehicle", "auto", "city", "make", "model"],
        "авто": ["car", "vehicle", "auto", "city", "make", "model"],
        "автомоб": ["car", "vehicle", "auto", "city", "make", "model"],
        "город": ["city"],
        "городе": ["city"],
        "городам": ["city"],
        "граф": ["dashboard", "grafana", "time", "city"],
        "grafana": ["dashboard", "grafana"],
    }
    for marker, values in aliases.items():
        if marker in text:
            terms.update(values)
    return terms


def analytics_schema_for_question(question, include_views=True):
    """Возвращает краткую схему и наиболее вероятные таблицы под вопрос пользователя."""
    schema = analytics_schema(include_views=include_views)
    terms = question_schema_terms(question)
    candidates = []
    all_tables = []
    for table in schema["tables"]:
        columns = table.get("columns") or []
        haystack = " ".join([str(table.get("name", "")), *[str(column) for column in columns]]).lower()
        score = sum(1 for term in terms if term and term in haystack)
        all_tables.append({"name": table.get("name"), "rows": table.get("rows")})
        if score > 0:
            candidates.append({**table, "score": score})
    candidates.sort(key=lambda row: (-row.get("score", 0), row.get("name") or ""))
    if not candidates:
        candidates = [table for table in schema["tables"] if int(table.get("rows") or 0) > 0]
    candidate_tables = candidates[:5]
    return {
        "database": "analytics",
        "question": question,
        "all_tables": all_tables,
        "candidate_tables": candidate_tables,
        "tables": candidate_tables,
    }


def analytics_column_exists(table, column):
    """Проверяет колонку перед тем, как generated-коннектор использует ее в SQL."""
    column_name = safe_sql_identifier(column)
    table_name, columns = analytics_table_columns(table)
    if not any(row.get("name") == column_name for row in columns):
        raise ValueError(f"Unknown column {column_name} in analytics.{table_name}")
    return table_name, column_name, columns


class HelperBag:
    """Объект helpers, который передается в generated Python-коннекторы."""

    analytics_list_tables = staticmethod(analytics_list_tables)
    analytics_schema = staticmethod(analytics_schema)
    analytics_schema_for_question = staticmethod(analytics_schema_for_question)
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


def normalize_generated_connector_name(value):
    """Приводит имя generated-коннектора к безопасному ASCII slug."""
    text = str(value or "").strip()
    text = "".join(TRANSLIT_CHARS.get(char, char) for char in text.lower())
    text = re.sub(r"[^a-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    if text and text[0].isdigit():
        text = f"clickhouse_{text}"
    return text[:80]


def safe_generated_connector_name(value):
    """Валидирует имя generated-коннектора и запрещает выход из папки хранения."""
    text = normalize_generated_connector_name(value)
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
    if source.count("\n") <= 1 and "\\n" in source:
        # Некоторые локальные модели возвращают source_code как одну строку с literal "\n".
        source = source.replace("\\n", "\n").replace("\\t", "    ")
    source = re.sub(r";\s*(def\s+handler\s*\()", r"\n\n\1", source)
    if not source:
        raise ValueError("Generated connector source_code is required.")
    if len(source) > 30000:
        raise ValueError("Generated connector source_code is too large.")
    if "CONNECTOR" not in source or "def handler" not in source:
        raise ValueError("Generated Python connector must define CONNECTOR and def handler(...).")
    if re.search(r"CONNECTOR\s*=\s*['\"]", source):
        raise ValueError(
            "Generated connector must define CONNECTOR as a dict, not a string. "
            "Use: CONNECTOR = {'name': '<connector_name>', 'description': '...', "
            "'input_schema': {'type': 'object', 'properties': {}}}."
        )
    name_match = re.search(r"['\"]name['\"]\s*:\s*['\"]([^'\"]+)['\"]", source)
    if not name_match or safe_generated_connector_name(name_match.group(1)) != safe_name:
        raise ValueError(
            f"Generated connector source must export name {safe_name} inside CONNECTOR dict. "
            f"Use: CONNECTOR = {{'name': '{safe_name}', 'description': '...', "
            "'input_schema': {'type': 'object', 'properties': {}}}}."
        )
    if (
        "run_query" in source
        and re.search(r"['\"][^'\"]*analytics\.", source)
        and "analytics_column_exists" not in source
        and "analytics_columns" not in source
    ):
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


def write_generated_connector_file(file_path, source):
    """Атомарно записывает connector.py, чтобы следующий tool call не увидел полузаписанный файл."""
    tmp_path = file_path.with_suffix(".py.tmp")
    tmp_path.write_text(source, encoding="utf-8")
    tmp_path.replace(file_path)


def wait_for_generated_connector_file(file_path, timeout_seconds=3):
    """Ждет появления connector.py на bind mount между create и run."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if file_path.is_file():
            return
        time.sleep(0.05)
    raise FileNotFoundError(file_path)


def delete_generated_connector(connector_name):
    """Удаляет временный generated-коннектор после запуска, чтобы он не влиял на следующие запросы."""
    safe_name = safe_generated_connector_name(connector_name)
    GENERATED_CONNECTOR_CACHE.pop(safe_name, None)
    connector_dir = generated_connector_path(safe_name).parent
    if connector_dir.exists() and connector_dir.parent == GENERATED_CONNECTORS_DIR:
        shutil.rmtree(connector_dir)


def cleanup_generated_connectors_dir():
    """Очищает старые generated-коннекторы при старте MCP server."""
    ensure_generated_connectors_dir()
    for connector_name in list_generated_connector_files():
        try:
            delete_generated_connector(connector_name)
        except Exception as error:
            print(f"Failed to delete generated connector {connector_name}: {error}")


def validate_generated_connector_module(module, expected_name=""):
    """Проверяет контракт импортированного Python-коннектора."""
    connector = getattr(module, "CONNECTOR", None)
    handler = getattr(module, "handler", None)
    if not isinstance(connector, dict):
        raise ValueError("Generated connector must define CONNECTOR dict.")
    if "input_schema" not in connector and isinstance(connector.get("inputSchema"), dict):
        connector["input_schema"] = connector["inputSchema"]
    if connector.get("input_schema") == {}:
        connector["input_schema"] = {"type": "object", "properties": {}}
    if isinstance(connector.get("input_schema"), dict) and "type" not in connector["input_schema"]:
        connector["input_schema"]["type"] = "object"
    if isinstance(connector.get("input_schema"), dict) and "properties" not in connector["input_schema"]:
        connector["input_schema"]["properties"] = {}
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
    wait_for_generated_connector_file(file_path)
    module_name = f"generated_{safe_name}_{int(time.time() * 1000)}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Cannot load generated connector: {public_generated_connector_path(safe_name)}")
    module = importlib.util.module_from_spec(spec)
    module.__dict__.update(
        {
            "quote_ident": quote_ident,
            "quote_string": quote_string,
            "sql_literal": sql_literal,
            "safe_sql_identifier": safe_sql_identifier,
            "bounded_limit": bounded_limit,
        }
    )
    spec.loader.exec_module(module)
    connector_name = validate_generated_connector_module(module, safe_name)
    connector = dict(module.CONNECTOR)
    connector["name"] = connector_name
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
    """Готовит metadata saved Python-коннекторов, если понадобится отладочный список.

    В обычный tools/list эти коннекторы больше не добавляются.
    Иначе модель видит старые task-specific tools и может выбрать их вместо создания свежего connector.
    """
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
    """Запускает generated-коннектор, возвращает результат и удаляет временный файл."""
    safe_name = safe_generated_connector_name(connector_name)
    connector = load_generated_connector(safe_name, force=True)
    try:
        result = connector["handler"](args or {}, run_query, HELPERS)
        return {
            "connector_name": connector["name"],
            "saved_path": connector["public_path"],
            "ephemeral": True,
            "deleted_after_run": True,
            "result": result,
        }
    finally:
        delete_generated_connector(safe_name)


def call_loaded_generated_connector(connector, args):
    """Запускает уже загруженный generated-коннектор и удаляет временный файл."""
    safe_name = safe_generated_connector_name(connector["name"])
    try:
        result = connector["handler"](args or {}, run_query, HELPERS)
        return {
            "connector_name": connector["name"],
            "saved_path": connector["public_path"],
            "ephemeral": True,
            "deleted_after_run": True,
            "result": result,
        }
    finally:
        delete_generated_connector(safe_name)


def grafana_target_format(value):
    """Переводит человекочитаемый format в формат Grafana ClickHouse datasource."""
    if isinstance(value, str):
        normalized = value.strip().lower().replace("-", "_")
        if normalized in {"time_series", "timeseries", "time"}:
            return 0
        if normalized in {"table", "logs"}:
            return 1
    return value


def clickhouse_grafana_target(raw_sql, ref_id="A", fmt=1, format=None):
    """Формирует target для Grafana ClickHouse datasource."""
    if format is not None:
        fmt = format
    return {
        "datasource": {"type": "grafana-clickhouse-datasource", "uid": "clickhouse-analytics"},
        "format": grafana_target_format(fmt),
        "rawSql": normalize_analytics_sql(raw_sql),
        "refId": ref_id,
    }


def sql_has_time_field(raw_sql):
    """Проверяет, похож ли SQL на time series запрос с колонкой time."""
    sql = str(raw_sql or "").lower()
    return bool(re.search(r"\bas\s+time\b|\btime\s*,|\btime\s+from\b", sql))


def normalize_grafana_dashboard(dashboard):
    """Исправляет частые ошибки generated dashboard перед отправкой в Grafana."""
    normalized = dict(dashboard or {})
    panels = []
    for panel in normalized.get("panels") or []:
        fixed_panel = dict(panel)
        targets = []
        has_time_field = False
        for target in fixed_panel.get("targets") or []:
            fixed_target = dict(target)
            fixed_target["format"] = grafana_target_format(fixed_target.get("format", 1))
            if sql_has_time_field(fixed_target.get("rawSql")):
                has_time_field = True
            targets.append(fixed_target)
        fixed_panel["targets"] = targets
        if fixed_panel.get("type") in {"timeseries", "time_series"} and not has_time_field:
            fixed_panel["type"] = "barchart"
            for target in fixed_panel["targets"]:
                target["format"] = 1
        panels.append(fixed_panel)
    normalized["panels"] = panels
    return normalized


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
    started_at = utc_now_iso()
    dashboard = normalize_grafana_dashboard(dashboard)
    payload = {"dashboard": dashboard, "folderId": 0, "overwrite": True, "message": "Created by Agentic Data Stack MCP"}
    try:
        result = http_json(
            "POST",
            f"{GRAFANA_API_URL}/api/dashboards/db",
            payload,
            {"Authorization": grafana_auth_header()},
        )
        send_langfuse_span(
            "mcp.grafana.create_dashboard",
            started_at,
            utc_now_iso(),
            input_value={"title": dashboard.get("title"), "uid": dashboard.get("uid"), "panels": len(dashboard.get("panels") or [])},
            output_value=result,
            metadata={"grafanaApiUrl": GRAFANA_API_URL},
        )
        return result
    except Exception as error:
        send_langfuse_span(
            "mcp.grafana.create_dashboard",
            started_at,
            utc_now_iso(),
            input_value={"title": dashboard.get("title"), "uid": dashboard.get("uid")},
            output_value={"error": str(error)},
            metadata={"grafanaApiUrl": GRAFANA_API_URL},
            level="ERROR",
            status_message=str(error),
        )
        raise


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


def generated_grafana_bar_chart_response(title, sql, rows=None, uid=None, panel_title=None, metadata=None):
    """Создает простой bar chart dashboard без ручной сборки Grafana JSON моделью."""
    if rows is None:
        rows = run_query(sql)
    dashboard_uid = normalize_generated_connector_name(uid or title or "clickhouse_dashboard")[:40]
    dashboard = {
        "uid": dashboard_uid,
        "title": safe_dashboard_title(title, "Analytics Dashboard"),
        "schemaVersion": 39,
        "version": 0,
        "panels": [
            {
                "id": 1,
                "type": "barchart",
                "title": safe_dashboard_title(panel_title or title, "Analytics Chart"),
                "gridPos": {"h": 9, "w": 18, "x": 0, "y": 0},
                "datasource": {"type": "grafana-clickhouse-datasource", "uid": "clickhouse-analytics"},
                "targets": [clickhouse_grafana_target(sql, ref_id="A", format="table")],
                "options": {"orientation": "auto", "xTickLabelRotation": 0, "xTickLabelSpacing": 0},
            }
        ],
    }
    return generated_grafana_dashboard_response(dashboard, rows=rows, metadata=metadata)


HelperBag.grafana_dashboard_response = staticmethod(generated_grafana_dashboard_response)
HelperBag.grafana_clickhouse_target = staticmethod(clickhouse_grafana_target)
HelperBag.grafana_bar_chart_response = staticmethod(generated_grafana_bar_chart_response)


def base_tools():
    """LibreChat видит только lifecycle tools, чтобы пользовательские запросы шли через generated-коннекторы."""
    connector_contract = (
        "Create a Python MCP connector file. source_code must be plain Python without imports. "
        "It must define CONNECTOR as a dict, not a string: "
        "CONNECTOR = {'name': '<same connector_name>', 'description': '...', 'input_schema': {'type': 'object', 'properties': {}}}. "
        "It must define handler(args, run_query, helpers). Use run_query(sql) and helpers only."
    )
    return [
        {
            "name": "create_and_run_generated_connector",
            "description": (
                connector_contract
                + " Create, validate, run, and delete the connector in one tool call. "
                "Use it first for the schema connector, and second for the Grafana dashboard connector only when the user asks for a graph."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "connector_name": {"type": "string", "description": "Generated connector name, starting with clickhouse_."},
                    "source_code": {"type": "string", "description": "Complete Python source code for the generated connector."},
                    "arguments": {"type": "object", "default": {}},
                },
                "required": ["connector_name", "source_code"],
            },
        },
        {
            "name": "create_generated_connector",
            "description": (
                connector_contract
                + " Connectors are ephemeral: run_generated_connector deletes the connector immediately after it runs."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "connector_name": {"type": "string", "description": "Generated connector name, starting with clickhouse_."},
                    "source_code": {"type": "string", "description": "Complete Python source code for the generated connector."},
                    "overwrite": {"type": "boolean", "default": False},
                },
                "required": ["connector_name", "source_code"],
            },
        },
        {
            "name": "update_generated_connector",
            "description": (
                connector_contract
                + " Use this only to replace a connector created in the current answer flow. "
                "Connectors are ephemeral and are deleted by run_generated_connector."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "connector_name": {"type": "string", "description": "Generated connector name, starting with clickhouse_."},
                    "source_code": {"type": "string", "description": "Complete Python source code for the generated connector."},
                },
                "required": ["connector_name", "source_code"],
            },
        },
        {
            "name": "run_generated_connector",
            "description": "Run a generated Python MCP connector by name. The connector is deleted immediately after this run.",
            "inputSchema": {"type": "object", "properties": {"connector_name": {"type": "string"}, "arguments": {"type": "object", "default": {}}}, "required": ["connector_name"]},
        },
    ]


def execute_tool_call(request_id, name, args):
    """Выполняет tools/call: lifecycle tools или запуск generated-коннектора."""
    args = args or {}

    if name in ["create_generated_connector", "update_generated_connector", "create_and_run_generated_connector"]:
        connector_name = safe_generated_connector_name(args.get("connector_name"))
        source = validate_generated_connector_source(connector_name, args.get("source_code"))
        file_path = generated_connector_path(connector_name)
        overwrite = True
        ensure_generated_connector_dir(connector_name)
        write_generated_connector_file(file_path, source)
        GENERATED_CONNECTOR_CACHE.pop(connector_name, None)
        connector = load_generated_connector(connector_name, force=True)
        if name == "create_and_run_generated_connector":
            result = call_loaded_generated_connector(connector, args.get("arguments") or {})
            result["status"] = "created_and_run"
            return json_rpc(request_id, json_text_result(result))
        return json_rpc(
            request_id,
            json_text_result(
                {
                    "connector_name": connector["name"],
                    "saved_path": connector["public_path"],
                    "ephemeral": True,
                    "status": "created" if name == "create_generated_connector" else "updated",
                    "instruction": "Now call run_generated_connector with connector_name. This connector will be deleted immediately after it runs.",
                }
            ),
        )

    if name == "run_generated_connector":
        return json_rpc(request_id, json_text_result(call_generated_connector(args.get("connector_name"), args.get("arguments") or {})))

    return json_rpc_error(request_id, -32602, f"Unknown tool: {name}")


def handle_tool_call(request_id, name, args):
    """Оборачивает MCP tool call в Langfuse trace и выполняет tool."""
    started_at = utc_now_iso()
    trace_id = str(uuid.uuid4())
    span_id = str(uuid.uuid4())
    previous_trace_id = getattr(TRACE_CONTEXT, "trace_id", None)
    previous_span_id = getattr(TRACE_CONTEXT, "span_id", None)
    TRACE_CONTEXT.trace_id = trace_id
    TRACE_CONTEXT.span_id = span_id
    args = args or {}
    status = "ok"
    response = None
    error_text = None
    try:
        response = execute_tool_call(request_id, name, args)
        return response
    except Exception as error:
        status = "error"
        error_text = str(error)
        raise
    finally:
        ended_at = utc_now_iso()
        connector_name = args.get("connector_name") if isinstance(args, dict) else None
        events = [
            {
                "id": str(uuid.uuid4()),
                "type": "trace-create",
                "timestamp": utc_now_iso(),
                "body": {
                    "id": trace_id,
                    "timestamp": started_at,
                    "name": f"mcp.tool.{name}",
                    "input": redacted_tool_args(name, args),
                    "output": truncate_for_trace(response if response is not None else {"error": error_text}),
                    "environment": LANGFUSE_ENVIRONMENT,
                    "tags": ["agentic-data-stack", "mcp-server", "clickhouse-analytics"],
                    "metadata": {"tool": name, "connector_name": connector_name, "status": status},
                },
            },
            {
                "id": str(uuid.uuid4()),
                "type": "span-create",
                "timestamp": utc_now_iso(),
                "body": {
                    "id": span_id,
                    "traceId": trace_id,
                    "name": f"mcp.tool.{name}",
                    "startTime": started_at,
                    "endTime": ended_at,
                    "input": redacted_tool_args(name, args),
                    "output": truncate_for_trace(response if response is not None else {"error": error_text}),
                    "metadata": {"tool": name, "connector_name": connector_name},
                    "level": "ERROR" if status == "error" else "DEFAULT",
                    "statusMessage": error_text,
                },
            },
        ]
        send_langfuse_batch(events)
        TRACE_CONTEXT.trace_id = previous_trace_id
        TRACE_CONTEXT.span_id = previous_span_id


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
        return json_rpc(request_id, {"tools": base_tools()})
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
        request_id = None
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            request_id = payload.get("id") if isinstance(payload, dict) else None
            response = handle_rpc(payload)
            self._send(200, response)
        except Exception as error:
            traceback.print_exc()
            self._send(200, json_rpc_error(request_id, -32000, str(error)))

    def log_message(self, fmt, *args):
        print(f"{self.address_string()} - {fmt % args}")


if __name__ == "__main__":
    cleanup_generated_connectors_dir()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), McpHttpHandler)
    print(f"ClickHouse MCP Python server listening on {PORT}")
    server.serve_forever()
