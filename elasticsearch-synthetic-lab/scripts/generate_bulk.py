import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path


# Generates realistic Elasticsearch bulk NDJSON for local log-investigation tests.
# The documents imitate Nginx access/error logs for five internal web services.


SERVICES = [
    {
        "name": "aurora-gateway",
        "host": "edge-01",
        "upstream": "aurora-gateway:8080",
        "team": "platform",
        "paths": ["/api/v1/search", "/api/v1/login", "/api/v1/catalog", "/api/v1/orders"],
    },
    {
        "name": "orion-checkout",
        "host": "app-01",
        "upstream": "orion-checkout:8080",
        "team": "commerce",
        "paths": ["/checkout", "/checkout/confirm", "/cart/validate", "/orders/create"],
    },
    {
        "name": "vega-payments",
        "host": "app-02",
        "upstream": "vega-payments:8080",
        "team": "payments",
        "paths": ["/payments/authorize", "/payments/capture", "/payments/refund", "/payments/status"],
    },
    {
        "name": "nova-inventory",
        "host": "app-03",
        "upstream": "nova-inventory:8080",
        "team": "supply",
        "paths": ["/inventory/reserve", "/inventory/release", "/inventory/search", "/inventory/sync"],
    },
    {
        "name": "lumen-notifications",
        "host": "app-04",
        "upstream": "lumen-notifications:8080",
        "team": "crm",
        "paths": ["/notifications/email", "/notifications/sms", "/notifications/push", "/webhooks/provider"],
    },
]

METHODS = ["GET", "POST", "PUT", "DELETE"]
USER_AGENTS = ["desktop-web", "mobile-ios", "mobile-android", "partner-api", "backoffice"]
COUNTRIES = ["BY", "PL", "DE", "KZ", "TR", "AE", "JP"]
NGINX_WORKERS = ["nginx-edge-a", "nginx-edge-b", "nginx-edge-c"]


INCIDENTS = [
    {
        "name": "payment_provider_timeout",
        "service": "vega-payments",
        "start": "2024-09-18T06:20:00Z",
        "end": "2024-09-18T10:10:00Z",
        "error_rate": 5,
        "message": "upstream timed out while reading response header from upstream",
        "error_code": "PAYMENT_PROVIDER_TIMEOUT",
        "status": 504,
        "latency_add": 3900,
    },
    {
        "name": "checkout_connection_pool_exhausted",
        "service": "orion-checkout",
        "start": "2025-02-03T13:00:00Z",
        "end": "2025-02-03T16:45:00Z",
        "error_rate": 4,
        "message": "connect() failed (111: Connection refused) while connecting to upstream",
        "error_code": "UPSTREAM_CONNECTION_REFUSED",
        "status": 502,
        "latency_add": 1800,
    },
    {
        "name": "inventory_db_lock_contention",
        "service": "nova-inventory",
        "start": "2025-07-11T21:15:00Z",
        "end": "2025-07-12T01:30:00Z",
        "error_rate": 7,
        "message": "database lock wait timeout exceeded during inventory reservation",
        "error_code": "INVENTORY_LOCK_TIMEOUT",
        "status": 409,
        "latency_add": 2400,
    },
    {
        "name": "gateway_tls_handshake_spike",
        "service": "aurora-gateway",
        "start": "2025-12-22T03:00:00Z",
        "end": "2025-12-22T05:20:00Z",
        "error_rate": 6,
        "message": "SSL_do_handshake() failed while SSL handshaking to upstream",
        "error_code": "TLS_HANDSHAKE_FAILED",
        "status": 525,
        "latency_add": 1200,
    },
    {
        "name": "notification_queue_backlog",
        "service": "lumen-notifications",
        "start": "2026-04-28T08:40:00Z",
        "end": "2026-04-28T14:00:00Z",
        "error_rate": 3,
        "message": "message broker publish timeout, notification queue backlog detected",
        "error_code": "QUEUE_BACKLOG",
        "status": 429,
        "latency_add": 3100,
    },
]


def parse_datetime(value, default):
    text = value or default
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso(value):
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def stable_int(seed, modulo):
    return (seed * 1103515245 + 12345) % modulo


def active_incident(timestamp, service_name):
    for incident in INCIDENTS:
        if incident["service"] != service_name:
            continue
        start = parse_datetime(incident["start"], incident["start"])
        end = parse_datetime(incident["end"], incident["end"])
        if start <= timestamp < end:
            return incident
    return None


def release_for(timestamp, service_name):
    week = int(timestamp.strftime("%U"))
    return f"{service_name}-{timestamp.year}.{week:02d}.{stable_int(week + len(service_name), 17)}"


def document_for(seq, timestamp):
    service = SERVICES[seq % len(SERVICES)]
    path = service["paths"][(seq // len(SERVICES)) % len(service["paths"])]
    method = METHODS[stable_int(seq, len(METHODS))]
    agent = USER_AGENTS[stable_int(seq + 3, len(USER_AGENTS))]
    country = COUNTRIES[stable_int(seq + 7, len(COUNTRIES))]
    worker = NGINX_WORKERS[stable_int(seq + 11, len(NGINX_WORKERS))]
    incident = active_incident(timestamp, service["name"])

    base_latency = 28 + stable_int(seq + timestamp.minute, 220)
    upstream_latency = max(3, base_latency - stable_int(seq + 5, 18))
    status = 200
    level = "info"
    error_code = ""
    message = "request completed"
    incident_name = "none"

    if stable_int(seq + timestamp.day, 251) == 0:
        status = 500
        level = "error"
        error_code = "UNHANDLED_EXCEPTION"
        message = "unexpected application exception propagated through nginx upstream"
        upstream_latency += 700

    if incident:
        incident_name = incident["name"]
        upstream_latency += incident["latency_add"] + stable_int(seq, 900)
        base_latency = upstream_latency + stable_int(seq + 13, 80)
        if seq % incident["error_rate"] == 0:
            status = incident["status"]
            level = "error"
            error_code = incident["error_code"]
            message = incident["message"]
        else:
            status = 200 if incident["status"] not in {409, 429} else incident["status"]
            level = "warn"
            error_code = incident["error_code"] if status >= 400 else ""
            message = f"degraded upstream latency during {incident['name']}"

    request_id = f"req-{timestamp.strftime('%Y%m%d%H%M%S')}-{seq:09d}"
    trace_id = f"trc-{timestamp.strftime('%Y%m%d')}-{stable_int(seq, 10_000_000):07d}"
    user_id = f"user-{100_000 + stable_int(seq + 29, 900_000)}"
    remote_addr = f"10.{stable_int(seq, 240)}.{stable_int(seq + 1, 240)}.{1 + stable_int(seq + 2, 250)}"
    bytes_sent = 480 + stable_int(seq + 17, 180_000)

    return {
        "@timestamp": iso(timestamp),
        "event": {"dataset": "nginx.access", "kind": "event"},
        "environment": "synthetic-prod",
        "service": service["name"],
        "host": service["host"],
        "level": level,
        "message": message,
        "error_code": error_code,
        "incident": incident_name,
        "release": release_for(timestamp, service["name"]),
        "trace_id": trace_id,
        "span_id": f"spn-{stable_int(seq + 41, 1_000_000):06d}",
        "request_id": request_id,
        "user_id": user_id,
        "nginx": {
            "worker": worker,
            "remote_addr": remote_addr,
            "method": method,
            "path": path,
            "status": status,
            "request_time": round(base_latency / 1000, 3),
            "upstream_response_time": round(upstream_latency / 1000, 3),
            "upstream_addr": service["upstream"],
            "bytes_sent": bytes_sent,
            "referer": "https://app.example.local/dashboard",
            "request": f"{method} {path} HTTP/1.1",
        },
        "http": {
            "method": method,
            "path": path,
            "status_code": status,
            "latency_ms": base_latency,
            "upstream_latency_ms": upstream_latency,
            "user_agent": agent,
        },
        "geo": {"country_iso_code": country},
        "labels": {
            "team": service["team"],
            "source": "elasticsearch-synthetic-lab",
            "log_flavor": "nginx-plus-app-context",
        },
        "debug": {
            "retry_count": stable_int(seq + 53, 4) if level != "info" else 0,
            "circuit_breaker_open": incident_name != "none" and stable_int(seq + 59, 9) == 0,
            "payload_class": ["small", "medium", "large"][stable_int(seq + 61, 3)],
        },
    }


def main():
    docs = int(os.getenv("ELASTICSEARCH_DEMO_DOCS", "720"))
    target_mb = int(os.getenv("ELASTICSEARCH_DEMO_TARGET_MB", "0") or "0")
    index_prefix = os.getenv("ELASTICSEARCH_DEMO_INDEX_PREFIX", "nginx-logs")
    index_granularity = os.getenv("ELASTICSEARCH_DEMO_INDEX_GRANULARITY", "monthly").strip().lower()
    output = Path(os.getenv("OUTPUT", "data/elasticsearch/synthetic-logs.bulk.ndjson"))
    start = parse_datetime(os.getenv("ELASTICSEARCH_DEMO_START"), "2024-06-16T00:00:00Z")
    end = parse_datetime(os.getenv("ELASTICSEARCH_DEMO_END"), "2026-06-16T00:00:00Z")
    if end <= start:
        raise SystemExit("ELASTICSEARCH_DEMO_END must be after ELASTICSEARCH_DEMO_START")

    target_bytes = target_mb * 1024 * 1024
    output.parent.mkdir(parents=True, exist_ok=True)
    counts = {}
    bytes_written = 0
    seq = 0
    max_docs = docs if target_bytes <= 0 else 50_000_000

    with output.open("w", encoding="utf-8") as stream:
        while seq < max_docs and (target_bytes <= 0 or bytes_written < target_bytes):
            fraction = seq / max(max_docs, 1) if target_bytes <= 0 else (bytes_written / max(target_bytes, 1))
            timestamp = start + (end - start) * min(fraction, 0.999999)
            if index_granularity == "daily":
                index_suffix = timestamp.strftime("%Y.%m.%d")
            elif index_granularity == "yearly":
                index_suffix = timestamp.strftime("%Y")
            else:
                index_suffix = timestamp.strftime("%Y.%m")
            index_name = f"{index_prefix}-{index_suffix}"
            document_id = f"{index_name}-{seq:010d}"
            document = document_for(seq, timestamp)
            action_line = json.dumps({"index": {"_index": index_name, "_id": document_id}}, separators=(",", ":"))
            document_line = json.dumps(document, ensure_ascii=False, separators=(",", ":"))
            stream.write(action_line + "\n")
            stream.write(document_line + "\n")
            bytes_written += len(action_line.encode("utf-8")) + len(document_line.encode("utf-8")) + 2
            counts[index_name] = counts.get(index_name, 0) + 1
            seq += 1

    print(
        json.dumps(
            {
                "output": str(output),
                "documents": seq,
                "targetMB": target_mb,
                "bytes": bytes_written,
                "indexPrefix": index_prefix,
                "indexGranularity": index_granularity,
                "indexPattern": f"{index_prefix}-*",
                "start": iso(start),
                "end": iso(end),
                "services": [service["name"] for service in SERVICES],
                "incidents": [{"name": item["name"], "service": item["service"], "start": item["start"], "end": item["end"]} for item in INCIDENTS],
                "indices": counts,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
