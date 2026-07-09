import base64
import json
import os
import time
import traceback
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


# elasticsearch-connector переносит документы из внешнего Elasticsearch в ClickHouse.
# Demo-ветка оставляет только простой batch-режим: /batch за заданный интервал.

PORT = int(os.getenv("PORT", "3366"))

ELASTICSEARCH_BASE_URL = os.getenv("ELASTICSEARCH_BASE_URL", "http://host.docker.internal:9200").rstrip("/")
ELASTICSEARCH_USER = os.getenv("ELASTICSEARCH_USER", "")
ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD", "")
ELASTICSEARCH_BEARER_TOKEN = os.getenv("ELASTICSEARCH_BEARER_TOKEN", "")
ELASTICSEARCH_INDEX_PATTERN = os.getenv("ELASTICSEARCH_INDEX_PATTERN", "logs-*")
ELASTICSEARCH_TIMESTAMP_FIELD = os.getenv("ELASTICSEARCH_TIMESTAMP_FIELD", "@timestamp")
ELASTICSEARCH_SOURCE_NAME = os.getenv("ELASTICSEARCH_SOURCE_NAME", "elasticsearch")
ELASTICSEARCH_BATCH_SIZE = int(os.getenv("ELASTICSEARCH_BATCH_SIZE", "1000"))
ELASTICSEARCH_REQUEST_TIMEOUT_SECONDS = int(os.getenv("ELASTICSEARCH_REQUEST_TIMEOUT_SECONDS", "120"))

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "http://clickhouse:8123").rstrip("/")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "analytics")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "analytics_password")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "analytics")
CLICKHOUSE_ELASTICSEARCH_TABLE = os.getenv("CLICKHOUSE_ELASTICSEARCH_TABLE", "es_raw_logs")


def utc_now():
    """Возвращает текущее UTC-время с timezone."""
    return datetime.now(timezone.utc)


def parse_time(value):
    """Преобразует ISO/time строку или epoch seconds в datetime UTC."""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    text = str(value)
    try:
        return datetime.fromtimestamp(float(text), tz=timezone.utc)
    except ValueError:
        pass
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def clickhouse_datetime64(value):
    """Форматирует datetime как DateTime64(3) для ClickHouse."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def clickhouse_string(value):
    """Экранирует строковый литерал для ClickHouse SQL."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def elasticsearch_time(value):
    """Форматирует datetime как ISO UTC для range query Elasticsearch."""
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def json_bytes(value):
    """Сериализует Python-объект в JSON bytes."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def send_json(handler, status, value):
    """Возвращает JSON-ответ пользователю, shell-скрипту или healthcheck."""
    payload = json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


def read_json_body(handler):
    """Читает JSON body входящего HTTP-запроса."""
    length = int(handler.headers.get("content-length") or 0)
    if not length:
        return {}
    return json.loads(handler.rfile.read(length).decode("utf-8"))


def elasticsearch_headers():
    """Готовит auth headers для Elasticsearch API."""
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    if ELASTICSEARCH_BEARER_TOKEN:
        headers["Authorization"] = f"Bearer {ELASTICSEARCH_BEARER_TOKEN}"
    elif ELASTICSEARCH_USER or ELASTICSEARCH_PASSWORD:
        token = base64.b64encode(f"{ELASTICSEARCH_USER}:{ELASTICSEARCH_PASSWORD}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    return headers


def elasticsearch_request(path, body=None, method="GET"):
    """Выполняет HTTP-запрос к Elasticsearch и возвращает распарсенный JSON."""
    request = Request(
        f"{ELASTICSEARCH_BASE_URL}{path}",
        data=json_bytes(body) if body is not None else None,
        headers=elasticsearch_headers(),
        method=method,
    )
    try:
        with urlopen(request, timeout=ELASTICSEARCH_REQUEST_TIMEOUT_SECONDS) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(detail or str(error)) from error
    return json.loads(raw or "{}")


def clickhouse_query(sql, database=CLICKHOUSE_DATABASE, timeout=120):
    """Выполняет SQL в ClickHouse через HTTP API и возвращает текстовый ответ."""
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


def clickhouse_insert_json_each_row(table, rows):
    """Пишет список dict rows в ClickHouse FORMAT JSONEachRow."""
    if not rows:
        return 0
    body = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
    query = f"INSERT INTO {table} FORMAT JSONEachRow"
    params = urlencode({"database": CLICKHOUSE_DATABASE, "query": query})
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
        with urlopen(request, timeout=180) as response:
            response.read()
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(detail or str(error)) from error
    return len(rows)


def ensure_clickhouse_tables():
    """Создает raw-таблицу Elasticsearch, если init SQL еще не выполнялся."""
    clickhouse_query(
        f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.{CLICKHOUSE_ELASTICSEARCH_TABLE}
(
  source_name LowCardinality(String),
  index_name String,
  document_id String,
  event_time DateTime64(3, 'UTC'),
  ingest_time DateTime64(3, 'UTC') DEFAULT now64(3),
  document_json String,
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_time)
ORDER BY (source_name, index_name, event_time, document_id)
"""
    )


def extract_event_time(hit):
    """Достает event_time из _source по ELASTICSEARCH_TIMESTAMP_FIELD."""
    source = hit.get("_source") or {}
    value = source.get(ELASTICSEARCH_TIMESTAMP_FIELD)
    if value is None:
        return utc_now()
    return parse_time(value)


def hit_to_row(hit, ingest_version):
    """Преобразует один Elasticsearch hit в строку raw-таблицы ClickHouse."""
    source = hit.get("_source") or {}
    event_time = extract_event_time(hit)
    return {
        "source_name": ELASTICSEARCH_SOURCE_NAME,
        "index_name": hit.get("_index") or "",
        "document_id": hit.get("_id") or "",
        "event_time": clickhouse_datetime64(event_time),
        "document_json": json.dumps(source, ensure_ascii=False, separators=(",", ":")),
        "version": ingest_version,
    }


def open_point_in_time(index_pattern):
    """Открывает PIT, чтобы batch пагинация читала согласованный снимок индексов."""
    payload = elasticsearch_request(f"/{index_pattern}/_pit?keep_alive=2m", method="POST")
    pit_id = payload.get("id")
    if not pit_id:
        raise RuntimeError(f"Elasticsearch did not return PIT id: {payload}")
    return pit_id


def close_point_in_time(pit_id):
    """Закрывает PIT; ошибка закрытия не должна ломать успешную загрузку."""
    try:
        elasticsearch_request("/_pit", body={"id": pit_id}, method="DELETE")
    except Exception as error:
        print(f"Failed to close Elasticsearch PIT: {error}")


def search_documents(index_pattern, start, end, batch_size):
    """Генератор страниц Elasticsearch hits за интервал [start, end)."""
    pit_id = open_point_in_time(index_pattern)
    search_after = None
    try:
        while True:
            body = {
                "size": batch_size,
                "track_total_hits": False,
                "pit": {"id": pit_id, "keep_alive": "2m"},
                "sort": [
                    {ELASTICSEARCH_TIMESTAMP_FIELD: {"order": "asc", "unmapped_type": "date"}},
                    {"_shard_doc": "asc"},
                ],
                "query": {
                    "bool": {
                        "filter": [
                            {"range": {ELASTICSEARCH_TIMESTAMP_FIELD: {"gte": elasticsearch_time(start), "lt": elasticsearch_time(end)}}}
                        ]
                    }
                },
            }
            if search_after:
                body["search_after"] = search_after
            payload = elasticsearch_request("/_search", body=body, method="POST")
            hits = payload.get("hits", {}).get("hits", [])
            if not hits:
                break
            yield hits
            search_after = hits[-1].get("sort")
            pit_id = payload.get("pit_id") or pit_id
    finally:
        close_point_in_time(pit_id)


def migrate_interval(start, end, index_pattern, batch_size):
    """Читает Elasticsearch за интервал и пишет документы в ClickHouse."""
    ensure_clickhouse_tables()
    inserted = 0
    batches = 0
    last_event_time = None
    last_document_id = ""
    ingest_version = int(time.time() * 1000)
    for hits in search_documents(index_pattern, start, end, batch_size):
        rows = [hit_to_row(hit, ingest_version) for hit in hits]
        inserted += clickhouse_insert_json_each_row(CLICKHOUSE_ELASTICSEARCH_TABLE, rows)
        batches += 1
        last = hits[-1]
        last_event_time = extract_event_time(last)
        last_document_id = last.get("_id") or ""
    return {"inserted": inserted, "batches": batches, "last_event_time": last_event_time, "last_document_id": last_document_id}


def format_result_times(result):
    """Готовит результат загрузки к JSON-ответу."""
    formatted = dict(result)
    if formatted.get("last_event_time"):
        formatted["last_event_time"] = elasticsearch_time(formatted["last_event_time"])
    return formatted


def handle_batch(handler):
    """HTTP endpoint /batch: разово мигрирует указанный временной интервал."""
    body = read_json_body(handler)
    start = parse_time(body.get("start") or os.getenv("ELASTICSEARCH_BATCH_START") or (utc_now() - timedelta(hours=1)).isoformat())
    end = parse_time(body.get("end") or os.getenv("ELASTICSEARCH_BATCH_END") or utc_now().isoformat())
    index_pattern = body.get("index_pattern") or ELASTICSEARCH_INDEX_PATTERN
    batch_size = int(body.get("batch_size") or ELASTICSEARCH_BATCH_SIZE)
    result = migrate_interval(start, end, index_pattern, batch_size)
    send_json(
        handler,
        200,
        {
            "ok": True,
            "mode": "batch",
            "source": ELASTICSEARCH_SOURCE_NAME,
            "index_pattern": index_pattern,
            "start": elasticsearch_time(start),
            "end": elasticsearch_time(end),
            **format_result_times(result),
        },
    )


class ElasticsearchConnectorHandler(BaseHTTPRequestHandler):
    """HTTP endpoints elasticsearch-connector."""

    def do_GET(self):
        if self.path == "/health":
            send_json(
                self,
                200,
                {
                    "ok": True,
                    "runtime": "python",
                    "elasticsearchBaseUrl": ELASTICSEARCH_BASE_URL,
                    "indexPattern": ELASTICSEARCH_INDEX_PATTERN,
                    "clickhouseDatabase": CLICKHOUSE_DATABASE,
                    "batchPath": "/batch",
                },
            )
            return
        send_json(self, 404, {"error": "Not found"})

    def do_POST(self):
        try:
            if self.path == "/batch":
                handle_batch(self)
                return
            send_json(self, 404, {"error": "Not found"})
        except Exception as error:
            traceback.print_exc()
            send_json(self, 500, {"error": str(error)})

    def log_message(self, fmt, *args):
        print(f"{self.address_string()} - {fmt % args}")


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), ElasticsearchConnectorHandler)
    print(f"elasticsearch-connector listening on 0.0.0.0:{PORT}")
    server.serve_forever()
