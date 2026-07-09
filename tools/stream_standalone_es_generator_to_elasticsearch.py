#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


EXTRA_SERVICES = [
    {
        "name": "atlas-auth",
        "host": "app-05",
        "team": "identity",
        "upstream": "atlas-auth:8080",
        "paths": ["/auth/token", "/auth/session", "/auth/refresh", "/auth/mfa"],
        "warning_code": "AUTH_CACHE_RETRY",
        "warning_message": "auth cache lookup succeeded after a retry",
        "error_code": "AUTH_TOKEN_VALIDATION_FAILED",
        "error_message": "token validation failed during authentication",
    },
    {
        "name": "phoenix-search",
        "host": "app-06",
        "team": "discovery",
        "upstream": "phoenix-search:8080",
        "paths": ["/search", "/search/suggest", "/search/filter", "/search/reindex/status"],
        "warning_code": "SEARCH_REPLICA_LAG",
        "warning_message": "search replica lag exceeded the warning threshold",
        "error_code": "SEARCH_QUERY_TIMEOUT",
        "error_message": "search query timed out while waiting for shard response",
    },
    {
        "name": "solace-recommendations",
        "host": "app-07",
        "team": "personalization",
        "upstream": "solace-recommendations:8080",
        "paths": ["/recommendations/home", "/recommendations/cart", "/recommendations/item", "/features/score"],
        "warning_code": "MODEL_FEATURE_MISSING",
        "warning_message": "recommendation model used fallback features",
        "error_code": "MODEL_SCORING_TIMEOUT",
        "error_message": "recommendation scoring timed out before fallback response",
    },
    {
        "name": "titan-analytics",
        "host": "app-08",
        "team": "analytics",
        "upstream": "titan-analytics:8080",
        "paths": ["/events/ingest", "/events/batch", "/segments/update", "/reports/materialize"],
        "warning_code": "ANALYTICS_BATCH_DELAY",
        "warning_message": "analytics batch processing lag is above target",
        "error_code": "ANALYTICS_INGEST_REJECTED",
        "error_message": "analytics ingest rejected event batch after validation failure",
    },
    {
        "name": "ember-shipping",
        "host": "app-09",
        "team": "fulfillment",
        "upstream": "ember-shipping:8080",
        "paths": ["/shipping/rates", "/shipping/labels", "/shipping/tracking", "/shipping/warehouse-sync"],
        "warning_code": "CARRIER_RETRY",
        "warning_message": "carrier API request succeeded after a retry",
        "error_code": "CARRIER_API_UNAVAILABLE",
        "error_message": "carrier API unavailable while creating shipment",
    },
]


EXTRA_INCIDENTS = [
    {
        "name": "auth_cache_eviction_storm",
        "service": "atlas-auth",
        "start": "2022-09-14T06:20:00Z",
        "end": "2022-09-14T10:45:00Z",
        "status": 401,
        "error_rate": 5,
        "latency_add": 1800,
        "error_code": "AUTH_CACHE_EVICTION_STORM",
        "message": "auth cache eviction storm increased token validation latency",
    },
    {
        "name": "search_shard_relocation_timeout",
        "service": "phoenix-search",
        "start": "2023-06-02T11:00:00Z",
        "end": "2023-06-02T16:30:00Z",
        "status": 504,
        "error_rate": 4,
        "latency_add": 3200,
        "error_code": "SEARCH_SHARD_TIMEOUT",
        "message": "search shard relocation caused query timeout spike",
    },
    {
        "name": "recommendation_feature_store_lag",
        "service": "solace-recommendations",
        "start": "2024-04-19T08:15:00Z",
        "end": "2024-04-19T13:55:00Z",
        "status": 503,
        "error_rate": 5,
        "latency_add": 2600,
        "error_code": "FEATURE_STORE_LAG",
        "message": "feature store lag caused recommendation fallback storm",
    },
    {
        "name": "analytics_ingest_partition_hotspot",
        "service": "titan-analytics",
        "start": "2025-02-03T21:10:00Z",
        "end": "2025-02-04T02:25:00Z",
        "status": 429,
        "error_rate": 6,
        "latency_add": 2900,
        "error_code": "INGEST_PARTITION_HOTSPOT",
        "message": "analytics ingest partition hotspot caused throttling",
    },
    {
        "name": "shipping_carrier_api_degradation",
        "service": "ember-shipping",
        "start": "2025-05-28T07:40:00Z",
        "end": "2025-05-28T12:10:00Z",
        "status": 502,
        "error_rate": 4,
        "latency_add": 3100,
        "error_code": "CARRIER_API_DEGRADED",
        "message": "carrier API degradation caused shipment creation failures",
    },
]


def load_generator(path: Path):
    spec = importlib.util.spec_from_file_location("standalone_es_log_generator", path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"Cannot load generator from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def extend_generator(module) -> None:
    existing = {service["name"] for service in module.SERVICES}
    for service in EXTRA_SERVICES:
        if service["name"] not in existing:
            module.SERVICES.append(service)
            existing.add(service["name"])

    existing_incidents = {incident["name"] for incident in module.INCIDENTS}
    for incident in EXTRA_INCIDENTS:
        if incident["name"] not in existing_incidents:
            module.INCIDENTS.append(incident)
            existing_incidents.add(incident["name"])


def request_json(base_url: str, method: str, path: str, payload=None, timeout: int = 120):
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(base_url.rstrip("/") + path, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(detail or str(error)) from error
    return json.loads(raw or "{}")


def post_bulk(base_url: str, body: bytes, timeout: int):
    request = Request(
        base_url.rstrip("/") + "/_bulk",
        data=body,
        headers={"Content-Type": "application/x-ndjson", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8") or "{}")
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(detail or str(error)) from error
    if payload.get("errors"):
        examples = []
        for item in payload.get("items", []):
            action = item.get("index") or item.get("create") or {}
            if "error" in action:
                examples.append(action["error"])
            if len(examples) >= 5:
                break
        raise RuntimeError(json.dumps({"bulkErrors": examples}, ensure_ascii=False, indent=2))
    return payload


def create_index_template(base_url: str, index_prefix: str, shards: int, replicas: int) -> None:
    template = {
        "index_patterns": [f"{index_prefix}-*"],
        "template": {
            "settings": {
                "index": {
                    "number_of_shards": shards,
                    "number_of_replicas": replicas,
                    "codec": "best_compression",
                    "refresh_interval": "-1",
                    "mapping.total_fields.limit": 2000,
                }
            },
            "mappings": {
                "dynamic": False,
                "properties": {
                    "@timestamp": {"type": "date"},
                    "environment": {"type": "keyword"},
                    "service": {"type": "keyword"},
                    "host": {"type": "keyword"},
                    "level": {"type": "keyword"},
                    "message": {"type": "match_only_text"},
                    "error_code": {"type": "keyword"},
                    "incident": {"type": "keyword"},
                    "release": {"type": "keyword", "index": False},
                    "trace_id": {"type": "keyword", "index": False},
                    "span_id": {"type": "keyword", "index": False},
                    "request_id": {"type": "keyword", "index": False},
                    "user_id": {"type": "keyword", "index": False},
                    "event": {
                        "properties": {
                            "dataset": {"type": "keyword"},
                            "kind": {"type": "keyword"},
                        }
                    },
                    "nginx": {
                        "properties": {
                            "worker": {"type": "keyword"},
                            "remote_addr": {"type": "ip", "index": False},
                            "method": {"type": "keyword"},
                            "path": {"type": "keyword"},
                            "status": {"type": "integer"},
                            "request_time": {"type": "float"},
                            "upstream_response_time": {"type": "float"},
                            "upstream_addr": {"type": "keyword"},
                            "bytes_sent": {"type": "long"},
                            "referer": {"type": "keyword", "index": False},
                            "request": {"type": "keyword", "index": False},
                        }
                    },
                    "http": {
                        "properties": {
                            "method": {"type": "keyword"},
                            "path": {"type": "keyword"},
                            "status_code": {"type": "integer"},
                            "latency_ms": {"type": "integer"},
                            "upstream_latency_ms": {"type": "integer"},
                            "user_agent": {"type": "keyword"},
                        }
                    },
                    "geo": {"properties": {"country_iso_code": {"type": "keyword"}}},
                    "labels": {
                        "properties": {
                            "team": {"type": "keyword"},
                            "source": {"type": "keyword"},
                            "log_flavor": {"type": "keyword"},
                        }
                    },
                    "debug": {
                        "properties": {
                            "retry_count": {"type": "integer"},
                            "circuit_breaker_open": {"type": "boolean"},
                            "payload_class": {"type": "keyword"},
                        }
                    },
                },
            },
        },
    }
    path = "/_index_template/" + quote(f"{index_prefix}_synthetic_template")
    request_json(base_url, "PUT", path, template)


def set_refresh_interval(base_url: str, index_prefix: str, refresh_interval: str) -> None:
    request_json(
        base_url,
        "PUT",
        "/" + quote(f"{index_prefix}-*") + "/_settings",
        {"index": {"refresh_interval": refresh_interval}},
    )


def read_checkpoint(path: Path, resume: bool):
    if not resume or not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_checkpoint(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def disk_free_gib(path: Path) -> float:
    usage = shutil.disk_usage(path)
    return usage.free / 1024**3


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_args():
    parser = argparse.ArgumentParser(description="Stream standalone synthetic logs directly into Elasticsearch.")
    parser.add_argument("--generator", default="~/Downloads/standalone_es_log_generator.py")
    parser.add_argument("--elasticsearch-url", default="http://localhost:9200")
    parser.add_argument("--target-mb", type=int, default=102400)
    parser.add_argument("--bulk-mb", type=int, default=20)
    parser.add_argument("--index-prefix", default="nginx-logs")
    parser.add_argument("--index-granularity", choices=("daily", "monthly", "yearly"), default="monthly")
    parser.add_argument("--start", default="2021-01-01T00:00:00Z")
    parser.add_argument("--end", default="2026-01-01T00:00:00Z")
    parser.add_argument("--checkpoint", default="data/elasticsearch/standalone-stream-checkpoint.json")
    parser.add_argument("--meta-output", default="data/elasticsearch/standalone-stream-meta.json")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--replicas", type=int, default=0)
    parser.add_argument("--bulk-timeout-sec", type=int, default=240)
    parser.add_argument("--progress-every-gb", type=float, default=1.0)
    parser.add_argument("--min-free-gb", type=float, default=20.0)
    parser.add_argument("--disk-path", default=".")
    parser.add_argument("--skip-template", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    generator_path = Path(args.generator).expanduser()
    checkpoint_path = Path(args.checkpoint)
    meta_path = Path(args.meta_output)
    disk_path = Path(args.disk_path)

    module = load_generator(generator_path)
    extend_generator(module)

    start = module.parse_dt(args.start)
    end = module.parse_dt(args.end)
    if end <= start:
        raise SystemExit("--end must be later than --start")

    request_json(args.elasticsearch_url, "GET", "/")
    if not args.skip_template:
        create_index_template(args.elasticsearch_url, args.index_prefix, args.shards, args.replicas)

    target_bytes = args.target_mb * 1024 * 1024
    bulk_limit = args.bulk_mb * 1024 * 1024
    progress_step = int(args.progress_every_gb * 1024**3) if args.progress_every_gb > 0 else 0
    hours, cumulative, total_weight = module.weighted_hours(start, end)

    checkpoint = read_checkpoint(checkpoint_path, args.resume)
    if checkpoint:
        bytes_written = int(checkpoint["bulk_bytes"])
        document_bytes = int(checkpoint.get("document_bytes", 0))
        seq = int(checkpoint["documents"])
        counts = dict(checkpoint.get("indices", {}))
        print(json.dumps({"resume": True, "documents": seq, "bulk_bytes": bytes_written}, ensure_ascii=False), flush=True)
    else:
        bytes_written = 0
        document_bytes = 0
        seq = 0
        counts = {}

    next_progress = ((bytes_written // progress_step) + 1) * progress_step if progress_step else 0
    started = time.time()

    while bytes_written < target_bytes:
        free_gib = disk_free_gib(disk_path)
        if free_gib < args.min_free_gb:
            print(
                json.dumps(
                    {
                        "stopped": "low_disk",
                        "free_gib": round(free_gib, 2),
                        "min_free_gb": args.min_free_gb,
                        "documents": seq,
                        "bulk_gib": round(bytes_written / 1024**3, 2),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            break

        body_parts = []
        body_bytes = 0
        pending_counts = {}
        pending_docs = 0
        pending_doc_bytes = 0

        while body_bytes < bulk_limit and bytes_written + body_bytes < target_bytes:
            fraction = (bytes_written + body_bytes) / max(target_bytes, 1)
            timestamp = module.timestamp_for_fraction(fraction, seq, hours, cumulative, total_weight, end)
            index_name = f"{args.index_prefix}-{module.index_suffix(timestamp, args.index_granularity)}"
            doc_id = f"{index_name}-{seq:012d}"
            document_line = json.dumps(module.build_document(seq, timestamp), ensure_ascii=False, separators=(",", ":"))
            action_line = json.dumps({"index": {"_index": index_name, "_id": doc_id}}, separators=(",", ":"))
            chunk = (action_line + "\n" + document_line + "\n").encode("utf-8")
            body_parts.append(chunk)
            body_bytes += len(chunk)
            pending_doc_bytes += len(document_line.encode("utf-8")) + 1
            pending_counts[index_name] = pending_counts.get(index_name, 0) + 1
            pending_docs += 1
            seq += 1

        if not body_parts:
            break

        post_bulk(args.elasticsearch_url, b"".join(body_parts), args.bulk_timeout_sec)
        bytes_written += body_bytes
        document_bytes += pending_doc_bytes
        for index_name, count in pending_counts.items():
            counts[index_name] = counts.get(index_name, 0) + count

        checkpoint_payload = {
            "updated_at": utc_now_iso(),
            "generator": str(generator_path),
            "elasticsearch_url": args.elasticsearch_url,
            "index_prefix": args.index_prefix,
            "index_granularity": args.index_granularity,
            "start": module.iso(start),
            "end": module.iso(end),
            "target_mb": args.target_mb,
            "bulk_bytes": bytes_written,
            "document_bytes": document_bytes,
            "documents": seq,
            "services": [service["name"] for service in module.SERVICES],
            "incidents": [
                {"name": item["name"], "service": item["service"], "start": item["start"], "end": item["end"]}
                for item in module.INCIDENTS
            ],
            "indices": counts,
        }
        write_checkpoint(checkpoint_path, checkpoint_payload)

        if progress_step and bytes_written >= next_progress:
            print(
                json.dumps(
                    {
                        "progress": True,
                        "bulk_gib": round(bytes_written / 1024**3, 2),
                        "document_gib": round(document_bytes / 1024**3, 2),
                        "target_gib": round(target_bytes / 1024**3, 2),
                        "documents": seq,
                        "indices": len(counts),
                        "free_gib": round(disk_free_gib(disk_path), 2),
                        "elapsed_sec": round(time.time() - started, 1),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            while next_progress <= bytes_written:
                next_progress += progress_step

    set_refresh_interval(args.elasticsearch_url, args.index_prefix, "30s")
    request_json(args.elasticsearch_url, "POST", "/" + quote(f"{args.index_prefix}-*") + "/_refresh")

    final = dict(checkpoint_payload if "checkpoint_payload" in locals() else {})
    final.update(
        {
            "finished_at": utc_now_iso(),
            "completed": bytes_written >= target_bytes,
            "elapsed_sec": round(time.time() - started, 1),
            "bulk_gib": round(bytes_written / 1024**3, 3),
            "document_gib": round(document_bytes / 1024**3, 3),
            "free_gib": round(disk_free_gib(disk_path), 2),
        }
    )
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(final, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(final, ensure_ascii=False, indent=2))
    return 0 if final["completed"] else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (URLError, RuntimeError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2), file=sys.stderr)
        raise SystemExit(1)
