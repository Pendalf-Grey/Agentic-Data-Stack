import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path


# Генератор synthetic Elasticsearch logs для локального batch/stream тестирования.
# Выходной файл имеет NDJSON bulk format: action line + document line.


SERVICES = [
    ("api-gateway", "edge-01", ["/api/search", "/api/orders", "/api/cars", "/api/login"]),
    ("checkout-service", "app-01", ["/checkout", "/payment/authorize", "/orders/confirm"]),
    ("inventory-service", "app-02", ["/inventory/reserve", "/inventory/release", "/inventory/search"]),
    ("payment-service", "app-03", ["/payment/charge", "/payment/refund", "/payment/status"]),
    ("notification-service", "app-04", ["/notifications/email", "/notifications/sms", "/notifications/push"]),
]

CITIES = ["Minsk", "Moscow", "Tokyo"]
METHODS = ["GET", "POST", "PUT"]
USER_AGENTS = ["desktop-web", "mobile-ios", "mobile-android", "partner-api"]


def parse_datetime(value):
    """Парсит ISO дату из env или возвращает текущее UTC время."""
    if not value:
        value = "2026-05-24T12:00:00Z"
    text = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso(value):
    """Форматирует timestamp для Elasticsearch date field."""
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def incident_for(timestamp, service):
    """Возвращает активный synthetic incident для timestamp/service."""
    hour = timestamp.hour
    minute = timestamp.minute
    if service == "payment-service" and hour in {10, 11}:
        return "payment_latency_spike"
    if service == "inventory-service" and hour == 15 and minute < 40:
        return "inventory_db_lock_contention"
    if service == "notification-service" and hour == 2 and minute < 30:
        return "notification_queue_backlog"
    return "none"


def document_for(seq, timestamp):
    """Создает один realistic log-документ."""
    service, host, paths = SERVICES[seq % len(SERVICES)]
    path = paths[(seq // len(SERVICES)) % len(paths)]
    city = CITIES[seq % len(CITIES)]
    incident = incident_for(timestamp, service)
    method = METHODS[seq % len(METHODS)]
    user_agent = USER_AGENTS[seq % len(USER_AGENTS)]

    status = 200
    level = "info"
    latency_ms = 40 + (seq * 17 % 180)
    error_code = ""
    message = "request completed"

    if incident == "payment_latency_spike":
        latency_ms += 900 + (seq % 7) * 120
        if seq % 5 == 0:
            status = 503
            level = "error"
            error_code = "PAYMENT_PROVIDER_TIMEOUT"
            message = "payment provider timeout"
        else:
            level = "warn"
            message = "payment latency above threshold"
    elif incident == "inventory_db_lock_contention":
        latency_ms += 650 + (seq % 11) * 60
        if seq % 6 == 0:
            status = 409
            level = "warn"
            error_code = "INVENTORY_LOCK_TIMEOUT"
            message = "inventory lock contention"
    elif incident == "notification_queue_backlog":
        latency_ms += 300 + (seq % 5) * 80
        if seq % 4 == 0:
            status = 429
            level = "error"
            error_code = "QUEUE_BACKLOG"
            message = "notification queue backlog"
        else:
            level = "warn"
            message = "notification worker delayed"
    elif seq % 97 == 0:
        status = 500
        level = "error"
        error_code = "UNHANDLED_EXCEPTION"
        latency_ms += 450
        message = "unexpected application error"

    return {
        "@timestamp": iso(timestamp),
        "environment": "synthetic",
        "service": service,
        "host": host,
        "level": level,
        "message": message,
        "http": {
            "method": method,
            "path": path,
            "status_code": status,
            "latency_ms": latency_ms,
            "user_agent": user_agent,
        },
        "geo": {"city": city},
        "trace_id": f"trc-{timestamp.strftime('%Y%m%d%H%M')}-{seq:06d}",
        "user_id": f"user-{1000 + (seq % 250)}",
        "incident": incident,
        "error_code": error_code,
        "labels": {
            "team": "platform" if service in {"api-gateway", "inventory-service"} else "product",
            "source": "elasticsearch-synthetic-lab",
        },
    }


def main():
    docs = int(os.getenv("ELASTICSEARCH_DEMO_DOCS", "720"))
    hours = int(os.getenv("ELASTICSEARCH_DEMO_HOURS", "72"))
    index_prefix = os.getenv("ELASTICSEARCH_DEMO_INDEX_PREFIX", "nginx-logs")
    output = Path(os.getenv("OUTPUT", "data/elasticsearch/synthetic-logs.bulk.ndjson"))
    end = parse_datetime(os.getenv("ELASTICSEARCH_DEMO_END"))
    start = end - timedelta(hours=hours)
    step = (end - start) / max(docs, 1)

    output.parent.mkdir(parents=True, exist_ok=True)
    counts = {}
    with output.open("w", encoding="utf-8") as stream:
        for seq in range(docs):
            timestamp = start + step * seq
            index_name = f"{index_prefix}-{timestamp.strftime('%Y.%m.%d')}"
            document_id = f"{index_name}-{seq:08d}"
            document = document_for(seq, timestamp)
            counts[index_name] = counts.get(index_name, 0) + 1
            stream.write(json.dumps({"index": {"_index": index_name, "_id": document_id}}, separators=(",", ":")) + "\n")
            stream.write(json.dumps(document, ensure_ascii=False, separators=(",", ":")) + "\n")

    print(
        json.dumps(
            {
                "output": str(output),
                "documents": docs,
                "indexPrefix": index_prefix,
                "indexPattern": f"{index_prefix}-*",
                "start": iso(start),
                "end": iso(end),
                "indices": counts,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
