import base64
import hashlib
import json
import os
import struct
import time
import traceback
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


# Этот контейнер принимает Prometheus remote_write/backfill/debug_json
# и пишет samples в ClickHouse analytics.prometheus_samples.
# Реализация на Python не использует Node/npm и ходит в ClickHouse через HTTP API.

PORT = int(os.getenv("PORT", "3355"))
PROMETHEUS_BASE_URL = os.getenv("PROMETHEUS_BASE_URL", "http://prometheus:9090").rstrip("/")
PROMETHEUS_BEARER_TOKEN = os.getenv("PROMETHEUS_BEARER_TOKEN", "")
PROMETHEUS_BASIC_USER = os.getenv("PROMETHEUS_BASIC_USER", "")
PROMETHEUS_BASIC_PASSWORD = os.getenv("PROMETHEUS_BASIC_PASSWORD", "")
DEFAULT_BACKFILL_STEP = os.getenv("PROMETHEUS_BACKFILL_STEP", "60s")
DEFAULT_BACKFILL_QUERY = os.getenv("PROMETHEUS_BACKFILL_QUERY", "up")
SOURCE_NAME = os.getenv("PROMETHEUS_SOURCE_NAME", "prometheus")
DEBUG_JSON_ENABLED = os.getenv("PROMETHEUS_DEBUG_JSON_ENABLED", "true").lower() == "true"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "http://clickhouse:8123").rstrip("/")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "analytics")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "analytics_password")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "analytics")


def send_json(handler, status, value):
    """Отдает JSON-ответ вызывающему скрипту или healthcheck."""
    payload = json.dumps(value, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


def read_body(handler, limit_bytes=50 * 1024 * 1024):
    """Читает raw body и защищает контейнер от слишком большого запроса."""
    length = int(handler.headers.get("content-length") or 0)
    if length > limit_bytes:
        raise ValueError(f"Request body exceeds {limit_bytes} bytes")
    return handler.rfile.read(length) if length else b""


def read_json_body(handler):
    """Читает JSON body для /backfill и /debug/write-json."""
    raw = read_body(handler)
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def stable_labels_json(labels_object):
    """Сортирует labels, чтобы fingerprint был стабильным."""
    return json.dumps({key: labels_object[key] for key in sorted(labels_object)}, ensure_ascii=False, separators=(",", ":"))


def fingerprint(labels_json):
    """SHA256 fingerprint набора labels."""
    return hashlib.sha256(labels_json.encode("utf-8")).hexdigest()


def timestamp_to_datetime64(timestamp_ms):
    """Prometheus timestamp ms -> строка DateTime64 для ClickHouse."""
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def rows_from_timeseries(timeseries, ingest_mode):
    """Преобразует decoded Prometheus TimeSeries в JSONEachRow строки для ClickHouse."""
    rows = []
    for series in timeseries:
        labels_object = dict(series.get("labels") or {})
        metric_name = labels_object.get("__name__", "unknown_metric")
        labels_json = stable_labels_json(labels_object)
        labels_fingerprint = fingerprint(labels_json)
        for sample in series.get("samples") or []:
            rows.append(
                {
                    "metric_name": metric_name,
                    "labels_json": labels_json,
                    "fingerprint": labels_fingerprint,
                    "sample_time": timestamp_to_datetime64(float(sample["timestamp"])),
                    "value": float(sample["value"]),
                    "source": SOURCE_NAME,
                    "ingest_mode": ingest_mode,
                }
            )
    return rows


def insert_rows(rows):
    """Пишет rows в analytics.prometheus_samples через ClickHouse HTTP JSONEachRow."""
    if not rows:
        return {"inserted": 0}
    body = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
    query = "INSERT INTO prometheus_samples FORMAT JSONEachRow"
    url = f"{CLICKHOUSE_HOST}/?{urlencode({'database': CLICKHOUSE_DATABASE, 'query': query})}"
    request = Request(
        url,
        data=body.encode("utf-8"),
        headers={
            "X-ClickHouse-User": CLICKHOUSE_USER,
            "X-ClickHouse-Key": CLICKHOUSE_PASSWORD,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=120) as response:
            response.read()
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(detail or str(error)) from error
    return {"inserted": len(rows)}


def prometheus_headers():
    """Готовит auth headers для внешнего Prometheus API."""
    headers = {"Accept": "application/json"}
    if PROMETHEUS_BEARER_TOKEN:
        headers["Authorization"] = f"Bearer {PROMETHEUS_BEARER_TOKEN}"
    elif PROMETHEUS_BASIC_USER or PROMETHEUS_BASIC_PASSWORD:
        token = base64.b64encode(f"{PROMETHEUS_BASIC_USER}:{PROMETHEUS_BASIC_PASSWORD}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    return headers


def unix_seconds(value):
    """Преобразует ISO/time value в seconds для Prometheus query_range."""
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    try:
        float(text)
        return text
    except ValueError:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return str(dt.timestamp())


def query_range(query, start, end, step):
    """Читает range vector из Prometheus HTTP API."""
    params = urlencode({"query": query, "start": unix_seconds(start), "end": unix_seconds(end), "step": step or DEFAULT_BACKFILL_STEP})
    request = Request(f"{PROMETHEUS_BASE_URL}/api/v1/query_range?{params}", headers=prometheus_headers())
    with urlopen(request, timeout=120) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus query_range failed: {payload}")
    return payload.get("data", {}).get("result", [])


def rows_from_query_range_result(result, query):
    """Преобразует Prometheus query_range result в ClickHouse rows."""
    rows = []
    for series in result:
        labels_object = dict(series.get("metric") or {})
        metric_name = labels_object.get("__name__") or query
        labels_object.setdefault("__name__", metric_name)
        labels_json = stable_labels_json(labels_object)
        labels_fingerprint = fingerprint(labels_json)
        for timestamp_seconds, value in series.get("values") or []:
            rows.append(
                {
                    "metric_name": metric_name,
                    "labels_json": labels_json,
                    "fingerprint": labels_fingerprint,
                    "sample_time": timestamp_to_datetime64(float(timestamp_seconds) * 1000),
                    "value": float(value),
                    "source": SOURCE_NAME,
                    "ingest_mode": "backfill",
                }
            )
    return rows


def snappy_uncompress(data):
    """Минимальный Snappy block decoder для Prometheus remote_write."""
    index = 0

    def read_varint():
        nonlocal index
        shift = 0
        result = 0
        while True:
            byte = data[index]
            index += 1
            result |= (byte & 0x7F) << shift
            if byte < 0x80:
                return result
            shift += 7

    expected_length = read_varint()
    output = bytearray()
    while index < len(data):
        tag = data[index]
        index += 1
        tag_type = tag & 0x03
        if tag_type == 0:
            length = tag >> 2
            if length < 60:
                length += 1
            else:
                bytes_for_length = length - 59
                length = int.from_bytes(data[index:index + bytes_for_length], "little") + 1
                index += bytes_for_length
            output.extend(data[index:index + length])
            index += length
        elif tag_type == 1:
            length = ((tag >> 2) & 0x7) + 4
            offset = ((tag & 0xE0) << 3) | data[index]
            index += 1
            copy_from = len(output) - offset
            for _ in range(length):
                output.append(output[copy_from])
                copy_from += 1
        elif tag_type == 2:
            length = (tag >> 2) + 1
            offset = int.from_bytes(data[index:index + 2], "little")
            index += 2
            copy_from = len(output) - offset
            for _ in range(length):
                output.append(output[copy_from])
                copy_from += 1
        else:
            length = (tag >> 2) + 1
            offset = int.from_bytes(data[index:index + 4], "little")
            index += 4
            copy_from = len(output) - offset
            for _ in range(length):
                output.append(output[copy_from])
                copy_from += 1
    if len(output) != expected_length:
        raise ValueError(f"Snappy length mismatch: expected {expected_length}, got {len(output)}")
    return bytes(output)


def read_varint(buffer, index):
    """Читает protobuf varint."""
    shift = 0
    result = 0
    while True:
        byte = buffer[index]
        index += 1
        result |= (byte & 0x7F) << shift
        if byte < 0x80:
            return result, index
        shift += 7


def read_length_delimited(buffer, index):
    """Читает protobuf length-delimited field."""
    length, index = read_varint(buffer, index)
    return buffer[index:index + length], index + length


def skip_field(buffer, index, wire_type):
    """Пропускает неизвестное protobuf поле."""
    if wire_type == 0:
        _, index = read_varint(buffer, index)
        return index
    if wire_type == 1:
        return index + 8
    if wire_type == 2:
        _, index = read_length_delimited(buffer, index)
        return index
    if wire_type == 5:
        return index + 4
    raise ValueError(f"Unsupported protobuf wire type: {wire_type}")


def decode_label(buffer):
    """Декодирует prometheus.Label { name, value }."""
    index = 0
    name = ""
    value = ""
    while index < len(buffer):
        key, index = read_varint(buffer, index)
        field = key >> 3
        wire_type = key & 0x07
        if field == 1 and wire_type == 2:
            raw, index = read_length_delimited(buffer, index)
            name = raw.decode("utf-8")
        elif field == 2 and wire_type == 2:
            raw, index = read_length_delimited(buffer, index)
            value = raw.decode("utf-8")
        else:
            index = skip_field(buffer, index, wire_type)
    return name, value


def decode_sample(buffer):
    """Декодирует prometheus.Sample { value double, timestamp int64 }."""
    index = 0
    value = 0.0
    timestamp = 0
    while index < len(buffer):
        key, index = read_varint(buffer, index)
        field = key >> 3
        wire_type = key & 0x07
        if field == 1 and wire_type == 1:
            value = struct.unpack("<d", buffer[index:index + 8])[0]
            index += 8
        elif field == 2 and wire_type == 0:
            timestamp, index = read_varint(buffer, index)
        else:
            index = skip_field(buffer, index, wire_type)
    return {"value": value, "timestamp": timestamp}


def decode_timeseries(buffer):
    """Декодирует prometheus.TimeSeries."""
    index = 0
    labels = {}
    samples = []
    while index < len(buffer):
        key, index = read_varint(buffer, index)
        field = key >> 3
        wire_type = key & 0x07
        if field == 1 and wire_type == 2:
            raw, index = read_length_delimited(buffer, index)
            name, value = decode_label(raw)
            labels[name] = value
        elif field == 2 and wire_type == 2:
            raw, index = read_length_delimited(buffer, index)
            samples.append(decode_sample(raw))
        else:
            index = skip_field(buffer, index, wire_type)
    return {"labels": labels, "samples": samples}


def decode_write_request(buffer):
    """Декодирует prometheus.WriteRequest и возвращает список TimeSeries."""
    index = 0
    timeseries = []
    while index < len(buffer):
        key, index = read_varint(buffer, index)
        field = key >> 3
        wire_type = key & 0x07
        if field == 1 and wire_type == 2:
            raw, index = read_length_delimited(buffer, index)
            timeseries.append(decode_timeseries(raw))
        else:
            index = skip_field(buffer, index, wire_type)
    return timeseries


def handle_remote_write(handler):
    """Принимает Prometheus remote_write: Snappy -> Protobuf -> ClickHouse rows."""
    raw = read_body(handler)
    decoded = decode_write_request(snappy_uncompress(raw))
    rows = rows_from_timeseries(decoded, "remote_write")
    result = insert_rows(rows)
    handler.send_response(204)
    handler.send_header("X-Prometheus-Remote-Write-Version", "0.1.0")
    handler.end_headers()
    print(json.dumps({"event": "remote_write_ingested", **result}))


def handle_backfill(handler):
    """Пакетно забирает history из Prometheus query_range и пишет в ClickHouse."""
    body = read_json_body(handler)
    queries = body.get("queries") if isinstance(body.get("queries"), list) and body.get("queries") else [body.get("query") or DEFAULT_BACKFILL_QUERY]
    end = body.get("end") or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    start = body.get("start") or (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    step = body.get("step") or DEFAULT_BACKFILL_STEP
    inserted = 0
    details = []
    for query in queries:
        result = query_range(query, start, end, step)
        rows = rows_from_query_range_result(result, query)
        insert_result = insert_rows(rows)
        inserted += insert_result["inserted"]
        details.append({"query": query, "series": len(result), "inserted": insert_result["inserted"]})
    send_json(handler, 200, {"ok": True, "mode": "backfill", "start": start, "end": end, "step": step, "inserted": inserted, "details": details})


def handle_debug_json(handler):
    """Тестовый endpoint: принимает уже распарсенный JSON timeseries."""
    if not DEBUG_JSON_ENABLED:
        send_json(handler, 404, {"error": "Not found"})
        return
    body = read_json_body(handler)
    rows = rows_from_timeseries(body.get("timeseries") or [], body.get("ingest_mode") or "debug_json")
    send_json(handler, 200, {"ok": True, **insert_rows(rows)})


class PrometheusConnectorHandler(BaseHTTPRequestHandler):
    """HTTP endpoints prometheus-connector."""

    def do_GET(self):
        if self.path == "/health":
            send_json(
                self,
                200,
                {
                    "ok": True,
                    "runtime": "python",
                    "prometheusBaseUrl": PROMETHEUS_BASE_URL,
                    "clickhouseDatabase": CLICKHOUSE_DATABASE,
                    "remoteWritePath": "/api/v1/write",
                    "backfillPath": "/backfill",
                },
            )
            return
        send_json(self, 404, {"error": "Not found"})

    def do_POST(self):
        try:
            if self.path == "/api/v1/write":
                handle_remote_write(self)
                return
            if self.path == "/backfill":
                handle_backfill(self)
                return
            if self.path == "/debug/write-json":
                handle_debug_json(self)
                return
            send_json(self, 404, {"error": "Not found"})
        except Exception as error:
            traceback.print_exc()
            send_json(self, 500, {"error": str(error)})


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), PrometheusConnectorHandler)
    print(f"prometheus-connector listening on 0.0.0.0:{PORT}")
    print(f"prometheus base URL: {PROMETHEUS_BASE_URL}")
    server.serve_forever()
