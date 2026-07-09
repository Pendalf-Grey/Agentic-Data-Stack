#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from log_compressor import TIMESTAMP_CANDIDATES, choose_context_value, iterate_records, parse_timestamp


def ch_time(value: str | None) -> str:
    if not value:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    text = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def post_insert(url: str, user: str, password: str, database: str, table: str, rows: list[dict]) -> None:
    if not rows:
        return
    body = "\n".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) for row in rows) + "\n"
    query = f"INSERT INTO {table} FORMAT JSONEachRow"
    request = Request(
        f"{url}/?{urlencode({'database': database, 'query': query})}",
        data=body.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    token = ("%s:%s" % (user, password)).encode("utf-8")
    import base64

    request.add_header("Authorization", "Basic " + base64.b64encode(token).decode("ascii"))
    with urlopen(request, timeout=300) as response:
        response.read()


def document_id(record: object, index: int) -> str:
    if isinstance(record, dict):
        for key in ("_id", "id", "document_id", "request_id", "trace_id"):
            value = record.get(key)
            if value:
                return str(value)
    digest = hashlib.blake2b(
        json.dumps(record, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8"),
        digest_size=16,
    ).hexdigest()
    return f"synthetic-{index}-{digest}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Stream JSON/JSONL/ES bulk logs into ClickHouse.")
    parser.add_argument("input", type=Path)
    parser.add_argument("--input-format", default="auto")
    parser.add_argument("--array-key", default="logs")
    parser.add_argument("--clickhouse-url", default="http://localhost:8123")
    parser.add_argument("--database", default="analytics")
    parser.add_argument("--table", default="analytics.es_raw_logs")
    parser.add_argument("--user", default="analytics")
    parser.add_argument("--password", default="analytics_password")
    parser.add_argument("--source-name", default="elasticsearch-demo")
    parser.add_argument("--index-name", default="nginx-logs-demo")
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--progress-every", type=int, default=100000)
    args = parser.parse_args()

    version = int(time.time() * 1000)
    batch: list[dict] = []
    inserted = 0
    for i, record in enumerate(iterate_records(args.input, args.input_format, args.array_key), start=1):
        _, timestamp_raw = choose_context_value(record, None, TIMESTAMP_CANDIDATES)
        timestamp, _ = parse_timestamp(timestamp_raw)
        row = {
            "source_name": args.source_name,
            "index_name": record.get("_index", args.index_name) if isinstance(record, dict) else args.index_name,
            "document_id": document_id(record, i),
            "event_time": ch_time(timestamp),
            "document_json": json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str),
            "version": version,
        }
        batch.append(row)
        if len(batch) >= args.batch_size:
            post_insert(args.clickhouse_url, args.user, args.password, args.database, args.table, batch)
            inserted += len(batch)
            if args.progress_every > 0 and inserted % args.progress_every == 0:
                print(json.dumps({"inserted": inserted}, ensure_ascii=False), flush=True)
            batch.clear()
    post_insert(args.clickhouse_url, args.user, args.password, args.database, args.table, batch)
    inserted += len(batch)
    print(json.dumps({"ok": True, "inserted": inserted}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
