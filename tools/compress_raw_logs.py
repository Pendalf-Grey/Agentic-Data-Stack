#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import sys
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def http_query(url: str, user: str, password: str, database: str, query: str, timeout: int = 600) -> str:
    request = Request(
        f"{url}/?{urlencode({'database': database})}",
        data=query.encode("utf-8"),
        method="POST",
    )
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    request.add_header("Authorization", "Basic " + token)
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(body.strip() or str(exc)) from exc


def key_filter(last_time: str | None, last_id: str | None) -> str:
    if not last_time or last_id is None:
        return ""
    return (
        "AND (event_time, document_id) > "
        f"(toDateTime64({sql_string(last_time)}, 3, 'UTC'), {sql_string(last_id)})"
    )


def boundary_query(args: argparse.Namespace, last_time: str | None, last_id: str | None) -> str:
    return f"""
SELECT toString(event_time), document_id
FROM
(
  SELECT event_time, document_id
  FROM {args.raw_table}
  WHERE source_name = {sql_string(args.source_name)}
    AND index_name LIKE {sql_string(args.index_like)}
    {key_filter(last_time, last_id)}
  ORDER BY event_time, document_id
  LIMIT {args.batch_rows}
)
ORDER BY event_time DESC, document_id DESC
LIMIT 1
FORMAT TabSeparatedRaw
"""


def insert_query(args: argparse.Namespace, batch_no: int, last_time: str | None, last_id: str | None, boundary_time: str, boundary_id: str) -> str:
    upper = (
        "AND (event_time, document_id) <= "
        f"(toDateTime64({sql_string(boundary_time)}, 3, 'UTC'), {sql_string(boundary_id)})"
    )
    batch_select = f"""
    SELECT
      source_name,
      index_name,
      event_time,
      document_id,
      document_json
    FROM {args.raw_table}
    WHERE source_name = {sql_string(args.source_name)}
      AND index_name LIKE {sql_string(args.index_like)}
      {key_filter(last_time, last_id)}
      {upper}
    ORDER BY event_time, document_id
    """
    return f"""
INSERT INTO {args.compressed_table}
SELECT
  concat(batch.source_name, ':', batch.index_name, ':', toString({batch_no})) AS batch_id,
  batch.source_name,
  batch.index_name,
  {batch_no} AS batch_no,
  batch.event_time_from,
  batch.event_time_to,
  batch.document_id_to,
  batch.rows_read,
  batch.raw_chars,
  length(batch.compressed_json) AS compressed_chars,
  batch.compressed_json,
  now64(3) AS created_at
FROM
(
  SELECT
    source_name,
    index_name,
    min(event_time) AS event_time_from,
    max(event_time) AS event_time_to,
    argMax(document_id, tuple(event_time, document_id)) AS document_id_to,
    count() AS rows_read,
    sum(length(document_json)) AS raw_chars,
    log_compress_json(arrayStringConcat(groupArray(document_json), '\\n')) AS compressed_json
  FROM ({batch_select})
  GROUP BY source_name, index_name
) AS batch
"""


def existing_state(args: argparse.Namespace) -> tuple[int, str | None, str | None]:
    query = f"""
SELECT
  ifNull(max(batch_no), -1),
  argMax(toString(event_time_to), tuple(batch_no, event_time_to, document_id_to)),
  argMax(document_id_to, tuple(batch_no, event_time_to, document_id_to))
FROM {args.compressed_table}
WHERE source_name = {sql_string(args.source_name)}
  AND index_name LIKE {sql_string(args.index_like)}
FORMAT TabSeparatedRaw
"""
    out = http_query(args.clickhouse_url, args.user, args.password, args.database, query).strip()
    if not out:
        return 0, None, None
    max_batch_raw, time_to, document_id_to = out.split("\t")
    max_batch = int(max_batch_raw)
    if max_batch < 0:
        return 0, None, None
    if document_id_to:
        return max_batch + 1, time_to, document_id_to

    # Backward-compatible fallback for compressed rows written before document_id_to existed.
    document_query = f"""
SELECT document_id
FROM {args.raw_table}
WHERE source_name = {sql_string(args.source_name)}
  AND index_name LIKE {sql_string(args.index_like)}
  AND event_time = toDateTime64({sql_string(time_to)}, 3, 'UTC')
ORDER BY document_id DESC
LIMIT 1
FORMAT TabSeparatedRaw
"""
    last_id = http_query(args.clickhouse_url, args.user, args.password, args.database, document_query).strip()
    return max_batch + 1, time_to, last_id or None


def main() -> int:
    parser = argparse.ArgumentParser(description="Compress raw ClickHouse logs in keyset batches via Executable UDF.")
    parser.add_argument("--clickhouse-url", default="http://localhost:8123")
    parser.add_argument("--database", default="analytics")
    parser.add_argument("--user", default="analytics")
    parser.add_argument("--password", default="analytics_password")
    parser.add_argument("--raw-table", default="analytics.es_raw_logs")
    parser.add_argument("--compressed-table", default="analytics.es_log_compressed_batches")
    parser.add_argument("--source-name", default="elasticsearch-demo")
    parser.add_argument("--index-like", default="nginx-logs-%")
    parser.add_argument("--batch-rows", type=int, default=5000)
    parser.add_argument("--progress-every", type=int, default=100)
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--resume-unsafe", action="store_true")
    parser.add_argument("--max-batches", type=int, default=0)
    args = parser.parse_args()

    if args.truncate:
        http_query(args.clickhouse_url, args.user, args.password, args.database, f"TRUNCATE TABLE {args.compressed_table}")
        batch_no, last_time, last_id = 0, None, None
    else:
        if not args.resume_unsafe:
            existing = http_query(
                args.clickhouse_url,
                args.user,
                args.password,
                args.database,
                f"SELECT count() FROM {args.compressed_table} FORMAT TabSeparatedRaw",
            ).strip()
            if existing and int(existing) > 0:
                raise SystemExit(f"{args.compressed_table} is not empty; use --truncate to rebuild it.")
        batch_no, last_time, last_id = existing_state(args)

    started = time.time()
    while True:
        boundary = http_query(
            args.clickhouse_url,
            args.user,
            args.password,
            args.database,
            boundary_query(args, last_time, last_id),
        ).strip()
        if not boundary:
            break
        boundary_time, boundary_id = boundary.split("\t", 1)
        http_query(
            args.clickhouse_url,
            args.user,
            args.password,
            args.database,
            insert_query(args, batch_no, last_time, last_id, boundary_time, boundary_id),
        )
        batch_no += 1
        last_time, last_id = boundary_time, boundary_id
        if args.progress_every > 0 and batch_no % args.progress_every == 0:
            print(json.dumps({"compressed_batches": batch_no, "last_event_time": last_time}, ensure_ascii=False), flush=True)
        if args.max_batches > 0 and batch_no >= args.max_batches:
            break

    print(json.dumps({"ok": True, "compressed_batches": batch_no, "seconds": round(time.time() - started, 1)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
