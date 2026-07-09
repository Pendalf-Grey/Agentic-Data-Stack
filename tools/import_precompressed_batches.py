#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key, value)


def ch_time(value: str | None) -> str:
    if not value:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


class StreamingJson:
    def __init__(self, path: Path, chunk_size: int = 1024 * 1024) -> None:
        self.file = path.open("r", encoding="utf-8")
        self.chunk_size = chunk_size
        self.decoder = json.JSONDecoder()
        self.buffer = ""
        self.pos = 0
        self.eof = False

    def close(self) -> None:
        self.file.close()

    def fill(self) -> None:
        if self.eof:
            return
        chunk = self.file.read(self.chunk_size)
        if chunk:
            self.buffer += chunk
        else:
            self.eof = True

    def compact(self) -> None:
        if self.pos > self.chunk_size:
            self.buffer = self.buffer[self.pos :]
            self.pos = 0

    def skip_ws(self) -> None:
        while True:
            while self.pos < len(self.buffer) and self.buffer[self.pos].isspace():
                self.pos += 1
            if self.pos < len(self.buffer) or self.eof:
                self.compact()
                return
            self.fill()

    def peek(self) -> str:
        self.skip_ws()
        while self.pos >= len(self.buffer) and not self.eof:
            self.fill()
            self.skip_ws()
        if self.pos >= len(self.buffer):
            return ""
        return self.buffer[self.pos]

    def consume(self, expected: str) -> None:
        actual = self.peek()
        if actual != expected:
            raise ValueError(f"Expected {expected!r}, got {actual!r}")
        self.pos += 1
        self.compact()

    def value(self):
        while True:
            self.skip_ws()
            try:
                value, end = self.decoder.raw_decode(self.buffer, self.pos)
            except json.JSONDecodeError:
                if self.eof:
                    raise
                self.fill()
                continue
            self.pos = end
            self.compact()
            return value


def iter_compressed(path: Path):
    stream = StreamingJson(path)
    metadata = None
    templates = None
    try:
        stream.consume("{")
        first_field = True
        while True:
            if stream.peek() == "}":
                stream.consume("}")
                return
            if not first_field:
                stream.consume(",")
            first_field = False
            key = stream.value()
            stream.consume(":")
            if key == "metadata":
                metadata = stream.value()
            elif key == "templates":
                templates = stream.value()
            elif key == "rle_runs":
                if metadata is None or templates is None:
                    raise ValueError("metadata and templates must appear before rle_runs")
                stream.consume("[")
                first_run = True
                while True:
                    if stream.peek() == "]":
                        stream.consume("]")
                        break
                    if not first_run:
                        stream.consume(",")
                    first_run = False
                    yield metadata, templates, stream.value()
            else:
                stream.value()
    finally:
        stream.close()


def run_time_bounds(run: dict) -> tuple[str | None, str | None]:
    values = [run.get("start_time"), run.get("end_time")]
    values = [str(value) for value in values if value]
    if not values:
        return None, None
    return min(values), max(values)


def proportional_raw_chars(metadata: dict, rows_read: int) -> int:
    total_rows = int(metadata.get("input_records") or 0)
    total_size = int(metadata.get("source_file_size_bytes") or metadata.get("message_bytes") or 0)
    if total_rows <= 0 or total_size <= 0:
        return 0
    return max(1, round(total_size * rows_read / total_rows))


def top_values(counter: Counter) -> dict:
    total = sum(counter.values())
    return {
        "top": [{"value": value, "count": count} for value, count in counter.most_common(10)],
        "observed_total": total,
    }


def batch_templates(templates: list[dict], runs: list[dict]) -> list[dict]:
    originals = {template.get("id"): template for template in templates}
    stats: dict[str, dict] = {}
    for run in runs:
        template_id = run.get("template_id")
        if not template_id:
            continue
        count = int(run.get("count") or 0)
        item = stats.setdefault(
            template_id,
            {
                "count": 0,
                "first_seen": None,
                "last_seen": None,
                "services": Counter(),
                "hosts": Counter(),
                "levels": Counter(),
            },
        )
        item["count"] += count
        start, end = run_time_bounds(run)
        if start and (item["first_seen"] is None or start < item["first_seen"]):
            item["first_seen"] = start
        if end and (item["last_seen"] is None or end > item["last_seen"]):
            item["last_seen"] = end
        if run.get("service"):
            item["services"][str(run["service"])] += count
        if run.get("host"):
            item["hosts"][str(run["host"])] += count
        if run.get("level"):
            item["levels"][str(run["level"])] += count

    chunk_templates = []
    for template_id, item in stats.items():
        original = dict(originals.get(template_id, {"id": template_id}))
        original["count"] = item["count"]
        original["first_seen"] = item["first_seen"]
        original["last_seen"] = item["last_seen"]
        original["services"] = top_values(item["services"])
        original["hosts"] = top_values(item["hosts"])
        original["levels"] = top_values(item["levels"])
        chunk_templates.append(original)
    return sorted(chunk_templates, key=lambda template: template.get("id", ""))


def make_compressed_json(metadata: dict, templates: list[dict], runs: list[dict], batch_no: int) -> str:
    chunk_templates = batch_templates(templates, runs)
    rows_read = sum(int(run.get("count") or 0) for run in runs)
    first_index = min((int(run.get("first_record_index") or 0) for run in runs), default=0)
    last_index = max((int(run.get("last_record_index") or 0) for run in runs), default=0)
    doc = {
        "format_version": "1.0",
        "metadata": {
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source_file": metadata.get("source_file"),
            "source_file_size_bytes": metadata.get("source_file_size_bytes"),
            "input_records": rows_read,
            "skipped_records": 0,
            "unique_templates": len(chunk_templates),
            "rle_runs": len(runs),
            "batch_no": batch_no,
            "first_record_index": first_index,
            "last_record_index": last_index,
            "global_input_records": metadata.get("input_records"),
            "global_unique_templates": metadata.get("unique_templates"),
            "global_rle_runs": metadata.get("rle_runs"),
            "config": metadata.get("config", {}),
            "detected_fields": metadata.get("detected_fields", {}),
            "parameter_units": metadata.get("parameter_units", {}),
        },
        "templates": chunk_templates,
        "rle_runs": runs,
    }
    return json.dumps(doc, ensure_ascii=False, separators=(",", ":"))


def post_rows(args: argparse.Namespace, rows: list[dict]) -> None:
    if args.dry_run or not rows:
        return
    body = "\n".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) for row in rows) + "\n"
    query = f"INSERT INTO {args.table} FORMAT JSONEachRow"
    request = Request(
        f"{args.clickhouse_url}/?{urlencode({'database': args.database, 'query': query})}",
        data=body.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    token = base64.b64encode(f"{args.user}:{args.password}".encode("utf-8")).decode("ascii")
    request.add_header("Authorization", "Basic " + token)
    with urlopen(request, timeout=args.http_timeout) as response:
        response.read()


def flush_batch(args: argparse.Namespace, metadata: dict, templates: list[dict], runs: list[dict], batch_no: int) -> dict:
    compressed_json = make_compressed_json(metadata, templates, runs, batch_no)
    bounds = [run_time_bounds(run) for run in runs]
    starts = [start for start, _ in bounds if start]
    ends = [end for _, end in bounds if end]
    rows_read = sum(int(run.get("count") or 0) for run in runs)
    return {
        "batch_id": f"{args.source_name}:{args.index_name}:{batch_no:08d}",
        "source_name": args.source_name,
        "index_name": args.index_name,
        "batch_no": batch_no,
        "event_time_from": ch_time(min(starts) if starts else None),
        "event_time_to": ch_time(max(ends) if ends else None),
        "rows_read": rows_read,
        "raw_chars": proportional_raw_chars(metadata, rows_read),
        "compressed_chars": len(compressed_json),
        "compressed_json": compressed_json,
        "created_at": ch_time(None),
    }


def main() -> int:
    load_dotenv(Path(".env"))
    parser = argparse.ArgumentParser(description="Import a precompressed log_compressor.py JSON into ClickHouse compressed batches.")
    parser.add_argument("input", type=Path)
    parser.add_argument("--clickhouse-url", default=os.getenv("CLICKHOUSE_URL", "http://localhost:8123"))
    parser.add_argument("--database", default=os.getenv("CLICKHOUSE_DB", "analytics"))
    parser.add_argument("--table", default="analytics.es_log_compressed_batches")
    parser.add_argument("--user", default=os.getenv("CLICKHOUSE_USER", "analytics"))
    parser.add_argument("--password", default=os.getenv("CLICKHOUSE_PASSWORD", "analytics_password"))
    parser.add_argument("--source-name", default=os.getenv("LOGS_SOURCE_NAME", "elasticsearch-synthetic"))
    parser.add_argument("--index-name", default=os.getenv("LOGS_INDEX_NAME", "synthetic-logs"))
    parser.add_argument("--max-runs-per-batch", type=int, default=int(os.getenv("PRECOMPRESSED_MAX_RUNS_PER_BATCH", "1000")))
    parser.add_argument("--max-compressed-chars", type=int, default=int(os.getenv("PRECOMPRESSED_MAX_COMPRESSED_CHARS", "250000")))
    parser.add_argument("--insert-batches", type=int, default=25)
    parser.add_argument("--progress-every", type=int, default=100)
    parser.add_argument("--max-batches", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--http-timeout", type=int, default=600)
    args = parser.parse_args()

    pending_runs: list[dict] = []
    pending_chars = 0
    insert_rows: list[dict] = []
    batch_no = 0
    total_runs = 0
    total_rows = 0
    started = time.time()
    last_metadata = None

    for metadata, templates, run in iter_compressed(args.input):
        last_metadata = metadata
        run_chars = len(json.dumps(run, ensure_ascii=False, separators=(",", ":")))
        if pending_runs and (
            len(pending_runs) >= args.max_runs_per_batch
            or pending_chars + run_chars >= args.max_compressed_chars
        ):
            row = flush_batch(args, metadata, templates, pending_runs, batch_no)
            insert_rows.append(row)
            total_rows += int(row["rows_read"])
            total_runs += len(pending_runs)
            batch_no += 1
            pending_runs = []
            pending_chars = 0
            if len(insert_rows) >= args.insert_batches:
                post_rows(args, insert_rows)
                insert_rows.clear()
            if args.progress_every > 0 and batch_no % args.progress_every == 0:
                print(
                    json.dumps(
                        {
                            "batches": batch_no,
                            "rle_runs": total_runs,
                            "source_records": total_rows,
                            "seconds": round(time.time() - started, 1),
                            "dry_run": args.dry_run,
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
            if args.max_batches > 0 and batch_no >= args.max_batches:
                break
        pending_runs.append(run)
        pending_chars += run_chars

    if (args.max_batches <= 0 or batch_no < args.max_batches) and pending_runs and last_metadata is not None:
        row = flush_batch(args, last_metadata, templates, pending_runs, batch_no)
        insert_rows.append(row)
        total_rows += int(row["rows_read"])
        total_runs += len(pending_runs)
        batch_no += 1
    post_rows(args, insert_rows)

    print(
        json.dumps(
            {
                "ok": True,
                "dry_run": args.dry_run,
                "batches": batch_no,
                "rle_runs": total_runs,
                "source_records": total_rows,
                "seconds": round(time.time() - started, 1),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
