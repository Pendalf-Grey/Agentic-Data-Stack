#!/usr/bin/env python3
import os
import subprocess
import sys
import tempfile


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
COMPRESSOR = os.getenv("LOG_COMPRESSOR_PATH", "/opt/ads-tools/log_compressor.py")
if not os.path.exists(COMPRESSOR):
    COMPRESSOR = os.path.join(SCRIPT_DIR, "log_compressor.py")
DEFAULT_FLAGS = [
    "--input-format",
    "jsonl",
    "--rle-key",
    "template",
    "--samples-per-template",
    "3",
    "--top-values",
    "10",
    "--progress-every",
    "0",
]


def tsv_unescape(value: str) -> str:
    out = []
    i = 0
    while i < len(value):
        ch = value[i]
        if ch != "\\" or i + 1 >= len(value):
            out.append(ch)
            i += 1
            continue
        nxt = value[i + 1]
        out.append({"n": "\n", "t": "\t", "r": "\r", "\\": "\\"}.get(nxt, nxt))
        i += 2
    return "".join(out)


def tsv_escape(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def compress_payload(payload: str) -> str:
    flags = os.getenv("LOG_COMPRESSOR_FLAGS", "").split() or DEFAULT_FLAGS
    with tempfile.TemporaryDirectory(prefix="ch_log_compress_") as tmp:
        input_path = os.path.join(tmp, "input.jsonl")
        output_path = os.path.join(tmp, "compressed.json")
        with open(input_path, "w", encoding="utf-8") as fp:
            fp.write(payload)
            if payload and not payload.endswith("\n"):
                fp.write("\n")
        subprocess.run(
            [sys.executable, COMPRESSOR, input_path, output_path, *flags],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        with open(output_path, "r", encoding="utf-8") as fp:
            return fp.read()


for raw_line in sys.stdin:
    raw_line = raw_line.rstrip("\n")
    payload = tsv_unescape(raw_line.split("\t", 1)[0])
    try:
        result = compress_payload(payload)
    except Exception as exc:
        result = '{"error":"' + str(exc).replace('"', '\\"') + '"}'
    sys.stdout.write(tsv_escape(result) + "\n")
    sys.stdout.flush()
