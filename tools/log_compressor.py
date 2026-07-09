#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
log_compressor.py

Потоковое "LLM-ориентированное" сжатие логов:
1) похожие сообщения превращаются в шаблоны;
2) подряд идущие одинаковые события сжимаются RLE;
3) по каждому шаблону считается статистика;
4) результат сохраняется в один JSON-файл.

Работает на Ubuntu 22.04 и macOS.
Зависимости: только стандартная библиотека Python 3.9+.

Поддерживаемый вход:
- JSON-массив: [ {...}, {...} ]
- JSONL/NDJSON: один JSON-объект на строку
- Elasticsearch bulk NDJSON: строки {"index": ...} / {"create": ...}
  чередуются с JSON-документами логов
- JSON-объект с массивом logs: {"logs": [...]} (загружается в память;
  для больших файлов лучше использовать JSON-массив или JSONL)

Примеры:
    python3 log_compressor.py input.json output.json
    python3 log_compressor.py logs.jsonl compressed_based.json --input-format jsonl
    python3 log_compressor.py es_bulk.json compressed_based.json --input-format elasticsearch-bulk
    python3 log_compressor.py input.json output.json --message-field event.message
    python3 log_compressor.py input.json output.json --pretty --progress-every 100000
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import heapq
import io
import json
import math
import os
import re
import sys
import tempfile
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


FORMAT_VERSION = "1.0"

MESSAGE_CANDIDATES = (
    "message",
    "msg",
    "log",
    "event.message",
    "event.original",
    "error.message",
    "error",
    "text",
    "description",
    "stacktrace",
    "stack_trace",
    "raw",
)

TIMESTAMP_CANDIDATES = (
    "@timestamp",
    "timestamp",
    "time",
    "datetime",
    "date",
    "ts",
    "event.created",
    "event.ingested",
)

SERVICE_CANDIDATES = (
    "service.name",
    "service",
    "app",
    "application",
    "component",
    "logger",
    "source",
)

HOST_CANDIDATES = (
    "host.name",
    "host",
    "hostname",
    "node",
    "pod",
    "kubernetes.pod.name",
    "container.name",
)

LEVEL_CANDIDATES = (
    "log.level",
    "level",
    "severity",
    "severity_text",
    "priority",
)

ELASTICSEARCH_BULK_ACTIONS = {"index", "create", "update", "delete"}

SENSITIVE_PARAMETER_TYPES = {
    "EMAIL",
    "UUID",
    "HASH",
    "TOKEN",
    "URL",
}

# Порядок важен: сначала более специфичные шаблоны.
PATTERNS: Sequence[Tuple[str, re.Pattern[str]]] = (
    (
        "TIMESTAMP",
        re.compile(
            r"\b\d{4}-\d{2}-\d{2}[T ][0-2]\d:[0-5]\d:[0-5]\d"
            r"(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b"
        ),
    ),
    (
        "UUID",
        re.compile(
            r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
            r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\b"
        ),
    ),
    (
        "EMAIL",
        re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    ),
    (
        "URL",
        re.compile(r"\b(?:https?|ftp)://[^\s\"'<>]+", re.IGNORECASE),
    ),
    (
        "IPV6",
        re.compile(
            r"(?<![0-9A-Fa-f:])(?:"
            r"(?:[0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}|"
            r"(?:[0-9A-Fa-f]{1,4}:){1,7}:|"
            r"(?:[0-9A-Fa-f]{1,4}:){1,6}:[0-9A-Fa-f]{1,4}|"
            r"(?:[0-9A-Fa-f]{1,4}:){1,5}(?::[0-9A-Fa-f]{1,4}){1,2}|"
            r"(?:[0-9A-Fa-f]{1,4}:){1,4}(?::[0-9A-Fa-f]{1,4}){1,3}|"
            r"(?:[0-9A-Fa-f]{1,4}:){1,3}(?::[0-9A-Fa-f]{1,4}){1,4}|"
            r"(?:[0-9A-Fa-f]{1,4}:){1,2}(?::[0-9A-Fa-f]{1,4}){1,5}|"
            r"[0-9A-Fa-f]{1,4}:(?:(?::[0-9A-Fa-f]{1,4}){1,6})|"
            r":(?:(?::[0-9A-Fa-f]{1,4}){1,7}|:)"
            r")(?![0-9A-Fa-f:])"
        ),
    ),
    (
        "IP",
        re.compile(
            r"(?<![\d.])(?:25[0-5]|2[0-4]\d|1?\d?\d)"
            r"(?:\.(?:25[0-5]|2[0-4]\d|1?\d?\d)){3}(?![\d.])"
        ),
    ),
    (
        "HTTP_STATUS",
        re.compile(
            r"(?:(?<=HTTP )|(?<=HTTP=)|(?<=HTTP:)|(?<=status )|"
            r"(?<=status=)|(?<=status:))[1-5]\d{2}\b",
            re.IGNORECASE,
        ),
    ),
    (
        "DURATION",
        re.compile(
            r"\b\d+(?:\.\d+)?\s*"
            r"(?:ns|µs|us|ms|milliseconds?|s|sec(?:onds?)?|m|min(?:utes?)?|h|hours?)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "SIZE",
        re.compile(
            r"\b\d+(?:\.\d+)?\s*(?:B|KB|KiB|MB|MiB|GB|GiB|TB|TiB)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "VERSION",
        re.compile(r"\bv?\d+\.\d+(?:\.\d+){0,3}(?:[-+][0-9A-Za-z.-]+)?\b"),
    ),
    (
        "HEX",
        re.compile(r"\b0x[0-9a-fA-F]+\b"),
    ),
    (
        "HASH",
        re.compile(r"\b[0-9a-fA-F]{24,128}\b"),
    ),
    (
        "PATH",
        re.compile(
            r"(?:(?<!\w)/(?:[^/\s]+/)*[^/\s]*|"
            r"\b[A-Za-z]:\\(?:[^\\\s]+\\)*[^\\\s]*)"
        ),
    ),
    (
        "NUMBER",
        re.compile(r"(?<![\w.])-?\d+(?:\.\d+)?(?![\w.])"),
    ),
)

WHITESPACE_RE = re.compile(r"[ \t]+")
BLANK_LINES_RE = re.compile(r"\n{3,}")


def open_text(path: Path, mode: str = "rt"):
    """Открывает обычный или gzip-файл."""
    if str(path).lower().endswith(".gz"):
        return gzip.open(path, mode, encoding="utf-8", errors="replace")
    return open(path, mode, encoding="utf-8", errors="replace")


def get_nested(obj: Any, dotted_path: str) -> Any:
    current = obj
    for part in dotted_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def first_present(obj: Dict[str, Any], candidates: Sequence[str]) -> Tuple[Optional[str], Any]:
    for key in candidates:
        value = get_nested(obj, key)
        if value is not None and value != "":
            return key, value
    return None, None


def stringify_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def choose_message(record: Any, forced_field: Optional[str]) -> Tuple[Optional[str], str]:
    if isinstance(record, str):
        return None, record

    if not isinstance(record, dict):
        return None, stringify_value(record)

    if forced_field:
        value = get_nested(record, forced_field)
        if value is None:
            return forced_field, json.dumps(
                record, ensure_ascii=False, separators=(",", ":"), default=str
            )
        return forced_field, stringify_value(value)

    field_name, value = first_present(record, MESSAGE_CANDIDATES)
    if value is not None:
        return field_name, stringify_value(value)

    # Если стандартного поля нет — берём самое длинное строковое поле.
    longest_key: Optional[str] = None
    longest_value = ""
    for key, value in record.items():
        if isinstance(value, str) and len(value) > len(longest_value):
            longest_key = key
            longest_value = value

    if longest_key is not None:
        return longest_key, longest_value

    return None, json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str)


def choose_context_value(
    record: Any,
    forced_field: Optional[str],
    candidates: Sequence[str],
) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(record, dict):
        return None, None

    if forced_field:
        value = get_nested(record, forced_field)
        return forced_field, None if value is None else stringify_value(value)

    field_name, value = first_present(record, candidates)
    return field_name, None if value is None else stringify_value(value)


def parse_timestamp(value: Any) -> Tuple[Optional[str], Optional[float]]:
    if value is None or value == "":
        return None, None

    if isinstance(value, (int, float)):
        number = float(value)
        # Автоопределение секунд / миллисекунд / микросекунд.
        if number > 1e15:
            number /= 1_000_000
        elif number > 1e12:
            number /= 1_000
        try:
            dt = datetime.fromtimestamp(number, tz=timezone.utc)
            return dt.isoformat().replace("+00:00", "Z"), number
        except (OverflowError, OSError, ValueError):
            return str(value), None

    text = str(value).strip()
    if not text:
        return None, None

    candidates = [text]
    if text.endswith("Z"):
        candidates.append(text[:-1] + "+00:00")

    for candidate in candidates:
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            epoch = dt.timestamp()
            return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"), epoch
        except ValueError:
            pass

    # Частые форматы.
    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d.%m.%Y %H:%M:%S",
        "%Y-%m-%d",
    )
    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return dt.isoformat().replace("+00:00", "Z"), dt.timestamp()
        except ValueError:
            continue

    return text, None


def normalize_duration_to_ms(raw: str) -> Optional[float]:
    match = re.fullmatch(
        r"\s*(\d+(?:\.\d+)?)\s*([A-Za-zµ]+)\s*",
        raw,
        re.IGNORECASE,
    )
    if not match:
        return None

    value = float(match.group(1))
    unit = match.group(2).lower()
    factors = {
        "ns": 1e-6,
        "µs": 1e-3,
        "us": 1e-3,
        "ms": 1.0,
        "millisecond": 1.0,
        "milliseconds": 1.0,
        "s": 1000.0,
        "sec": 1000.0,
        "second": 1000.0,
        "seconds": 1000.0,
        "m": 60_000.0,
        "min": 60_000.0,
        "minute": 60_000.0,
        "minutes": 60_000.0,
        "h": 3_600_000.0,
        "hour": 3_600_000.0,
        "hours": 3_600_000.0,
    }
    factor = factors.get(unit)
    return None if factor is None else value * factor


def normalize_size_to_bytes(raw: str) -> Optional[float]:
    match = re.fullmatch(
        r"\s*(\d+(?:\.\d+)?)\s*([KMGT]?i?B)\s*",
        raw,
        re.IGNORECASE,
    )
    if not match:
        return None

    value = float(match.group(1))
    unit = match.group(2).upper()
    decimal = {"B": 1, "KB": 1000, "MB": 1000**2, "GB": 1000**3, "TB": 1000**4}
    binary = {"KIB": 1024, "MIB": 1024**2, "GIB": 1024**3, "TIB": 1024**4}
    factor = decimal.get(unit, binary.get(unit))
    return None if factor is None else value * factor


def normalize_message(message: str) -> Tuple[str, Dict[str, List[Any]]]:
    """
    Превращает динамические значения в плейсхолдеры.
    Возвращает шаблон и извлечённые параметры.
    """
    text = message.replace("\r\n", "\n").replace("\r", "\n").strip()
    parameters: Dict[str, List[Any]] = {}

    for parameter_type, pattern in PATTERNS:
        values: List[Any] = []

        def replacer(match: re.Match[str]) -> str:
            raw = match.group(0)

            if parameter_type == "DURATION":
                normalized = normalize_duration_to_ms(raw)
                values.append(normalized if normalized is not None else raw)
            elif parameter_type == "SIZE":
                normalized = normalize_size_to_bytes(raw)
                values.append(normalized if normalized is not None else raw)
            elif parameter_type == "NUMBER":
                try:
                    values.append(float(raw) if "." in raw else int(raw))
                except ValueError:
                    values.append(raw)
            else:
                values.append(raw)

            return f"<{parameter_type}>"

        text = pattern.sub(replacer, text)
        if values:
            parameters.setdefault(parameter_type, []).extend(values)

    # Уменьшаем косметические различия, не меняя переносы строк.
    lines = [WHITESPACE_RE.sub(" ", line).strip() for line in text.split("\n")]
    text = "\n".join(lines)
    text = BLANK_LINES_RE.sub("\n\n", text).strip()

    return text, parameters


class BoundedCounter:
    """
    Ограниченный счётчик top-N.
    При переполнении удаляет наименее частый элемент.
    Это защищает память при миллионах уникальных значений.
    """

    def __init__(self, max_items: int = 50) -> None:
        self.max_items = max_items
        self.counts: Dict[str, int] = {}
        self.total = 0
        self.overflow = 0

    def add(self, value: Optional[str], count: int = 1) -> None:
        if value is None or value == "":
            return
        value = str(value)
        self.total += count

        if value in self.counts:
            self.counts[value] += count
            return

        if len(self.counts) < self.max_items:
            self.counts[value] = count
            return

        # Space-saving approximation.
        smallest_key = min(self.counts, key=self.counts.get)
        smallest_count = self.counts.pop(smallest_key)
        self.overflow += smallest_count
        self.counts[value] = smallest_count + count

    def to_json(self, limit: int = 10) -> Dict[str, Any]:
        top = sorted(self.counts.items(), key=lambda item: (-item[1], item[0]))[:limit]
        result: Dict[str, Any] = {
            "top": [{"value": value, "count": count} for value, count in top],
            "observed_total": self.total,
        }
        if self.overflow:
            result["approximate"] = True
        return result


class NumericReservoir:
    def __init__(self, capacity: int = 512) -> None:
        self.capacity = capacity
        self.values: List[float] = []
        self.count = 0
        self.total = 0.0
        self.minimum: Optional[float] = None
        self.maximum: Optional[float] = None

    def add(self, value: float) -> None:
        if not math.isfinite(value):
            return

        self.count += 1
        self.total += value
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)

        if len(self.values) < self.capacity:
            self.values.append(value)
            return

        # Детерминированный reservoir sampling без глобального random.
        digest = hashlib.blake2b(
            f"{self.count}:{value}".encode("utf-8"),
            digest_size=8,
        ).digest()
        position = int.from_bytes(digest, "big") % self.count
        if position < self.capacity:
            self.values[position] = value

    @staticmethod
    def percentile(sorted_values: List[float], p: float) -> Optional[float]:
        if not sorted_values:
            return None
        if len(sorted_values) == 1:
            return sorted_values[0]
        index = (len(sorted_values) - 1) * p
        lower = math.floor(index)
        upper = math.ceil(index)
        if lower == upper:
            return sorted_values[lower]
        fraction = index - lower
        return sorted_values[lower] * (1 - fraction) + sorted_values[upper] * fraction

    def to_json(self) -> Dict[str, Any]:
        values = sorted(self.values)
        return {
            "count": self.count,
            "min": self.minimum,
            "mean": None if self.count == 0 else self.total / self.count,
            "p50": self.percentile(values, 0.50),
            "p95": self.percentile(values, 0.95),
            "p99": self.percentile(values, 0.99),
            "max": self.maximum,
            "percentiles_approximate": self.count > len(self.values),
        }


@dataclass
class ParameterStats:
    parameter_type: str
    keep_values: bool
    numeric: NumericReservoir = field(default_factory=NumericReservoir)
    categorical: BoundedCounter = field(default_factory=lambda: BoundedCounter(100))
    distinct_hashes: set = field(default_factory=set)
    distinct_overflow: bool = False
    max_distinct_hashes: int = 10000

    def add(self, value: Any) -> None:
        raw = stringify_value(value)
        digest = hashlib.blake2b(raw.encode("utf-8", errors="replace"), digest_size=8).digest()
        if len(self.distinct_hashes) < self.max_distinct_hashes:
            self.distinct_hashes.add(digest)
        else:
            self.distinct_overflow = True

        # Числовые типы.
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            self.numeric.add(float(value))
            return


        if self.keep_values:
            self.categorical.add(raw)

    def to_json(self, top_limit: int) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "type": self.parameter_type,
        }

        if self.numeric.count:
            result["numeric"] = self.numeric.to_json()

        if self.keep_values and self.categorical.total:
            result["values"] = self.categorical.to_json(limit=top_limit)
        elif not self.keep_values:
            result["values_redacted"] = True

        result["distinct_values_observed"] = len(self.distinct_hashes)
        if self.distinct_overflow:
            result["distinct_values_lower_bound"] = True

        return result


@dataclass
class TemplateStats:
    template_id: str
    fingerprint: str
    template: str
    sample_limit: int
    keep_sensitive_values: bool
    count: int = 0
    first_seen: Optional[str] = None
    first_epoch: Optional[float] = None
    last_seen: Optional[str] = None
    last_epoch: Optional[float] = None
    services: BoundedCounter = field(default_factory=lambda: BoundedCounter(100))
    hosts: BoundedCounter = field(default_factory=lambda: BoundedCounter(100))
    levels: BoundedCounter = field(default_factory=lambda: BoundedCounter(30))
    parameters: Dict[str, ParameterStats] = field(default_factory=dict)
    # max-heap через отрицательный score, чтобы хранить самые маленькие хеши.
    samples_heap: List[Tuple[int, int, Dict[str, Any]]] = field(default_factory=list)

    def update(
        self,
        *,
        index: int,
        message: str,
        timestamp: Optional[str],
        epoch: Optional[float],
        service: Optional[str],
        host: Optional[str],
        level: Optional[str],
        parameters: Dict[str, List[Any]],
    ) -> None:
        self.count += 1

        if timestamp is not None:
            if self.first_seen is None or (
                epoch is not None and (self.first_epoch is None or epoch < self.first_epoch)
            ):
                self.first_seen = timestamp
                self.first_epoch = epoch

            if self.last_seen is None or (
                epoch is not None and (self.last_epoch is None or epoch > self.last_epoch)
            ):
                self.last_seen = timestamp
                self.last_epoch = epoch

        self.services.add(service)
        self.hosts.add(host)
        self.levels.add(level)

        for parameter_type, values in parameters.items():
            if parameter_type not in self.parameters:
                keep_values = (
                    self.keep_sensitive_values
                    or parameter_type not in SENSITIVE_PARAMETER_TYPES
                )
                self.parameters[parameter_type] = ParameterStats(
                    parameter_type=parameter_type,
                    keep_values=keep_values,
                )
            parameter_stats = self.parameters[parameter_type]
            for value in values:
                parameter_stats.add(value)

        if self.sample_limit > 0:
            score = int.from_bytes(
                hashlib.blake2b(
                    f"{index}:{message}".encode("utf-8", errors="replace"),
                    digest_size=8,
                ).digest(),
                "big",
            )
            sample = {
                "record_index": index,
                "timestamp": timestamp,
                "service": service,
                "host": host,
                "level": level,
                "message": message,
            }
            item = (-score, index, sample)
            if len(self.samples_heap) < self.sample_limit:
                heapq.heappush(self.samples_heap, item)
            elif item[0] > self.samples_heap[0][0]:
                heapq.heapreplace(self.samples_heap, item)

    def to_json(self, top_limit: int) -> Dict[str, Any]:
        samples = [item[2] for item in sorted(self.samples_heap, key=lambda x: -x[0])]

        result: Dict[str, Any] = {
            "id": self.template_id,
            "fingerprint": self.fingerprint,
            "template": self.template,
            "count": self.count,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "levels": self.levels.to_json(top_limit),
            "services": self.services.to_json(top_limit),
            "hosts": self.hosts.to_json(top_limit),
            "parameters": {
                parameter_type: stats.to_json(top_limit)
                for parameter_type, stats in sorted(self.parameters.items())
            },
            "samples": samples,
        }
        return result


@dataclass
class RunState:
    signature: Optional[Tuple[Any, ...]] = None
    template_id: Optional[str] = None
    count: int = 0
    first_record_index: Optional[int] = None
    last_record_index: Optional[int] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    service: Optional[str] = None
    host: Optional[str] = None
    level: Optional[str] = None

    def reset(
        self,
        *,
        signature: Tuple[Any, ...],
        template_id: str,
        record_index: int,
        timestamp: Optional[str],
        service: Optional[str],
        host: Optional[str],
        level: Optional[str],
    ) -> None:
        self.signature = signature
        self.template_id = template_id
        self.count = 1
        self.first_record_index = record_index
        self.last_record_index = record_index
        self.start_time = timestamp
        self.end_time = timestamp
        self.service = service
        self.host = host
        self.level = level

    def extend(self, record_index: int, timestamp: Optional[str]) -> None:
        self.count += 1
        self.last_record_index = record_index
        if timestamp is not None:
            self.end_time = timestamp

    def to_json(self) -> Dict[str, Any]:
        return {
            "template_id": self.template_id,
            "count": self.count,
            "first_record_index": self.first_record_index,
            "last_record_index": self.last_record_index,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "service": self.service,
            "host": self.host,
            "level": self.level,
        }


def stream_json_array(fp: io.TextIOBase, chunk_size: int = 1024 * 1024) -> Iterator[Any]:
    """
    Потоково читает JSON-массив верхнего уровня без сторонних библиотек.
    """
    decoder = json.JSONDecoder()
    buffer = ""
    position = 0
    eof = False
    started = False
    finished = False

    while not finished:
        if position >= len(buffer) and not eof:
            buffer = fp.read(chunk_size)
            position = 0
            if buffer == "":
                eof = True

        while True:
            while position < len(buffer) and buffer[position].isspace():
                position += 1

            if not started:
                if position >= len(buffer):
                    break
                if buffer[position] != "[":
                    raise ValueError("Ожидался JSON-массив верхнего уровня '['")
                started = True
                position += 1
                continue

            while position < len(buffer) and buffer[position].isspace():
                position += 1

            if position < len(buffer) and buffer[position] == "]":
                finished = True
                position += 1
                break

            if position < len(buffer) and buffer[position] == ",":
                position += 1
                continue

            if position >= len(buffer):
                break

            try:
                item, end = decoder.raw_decode(buffer, position)
            except json.JSONDecodeError:
                if eof:
                    raise
                # Сохраняем необработанный хвост и дочитываем.
                more = fp.read(chunk_size)
                buffer = buffer[position:] + more
                position = 0
                if more == "":
                    eof = True
                continue

            yield item
            position = end

            # Периодически обрезаем обработанную часть буфера.
            if position > chunk_size:
                buffer = buffer[position:]
                position = 0

        if eof and not finished:
            remaining = buffer[position:].strip()
            if remaining:
                raise ValueError("JSON-массив завершился некорректно")
            raise ValueError("Не найден закрывающий символ ']' JSON-массива")


def stream_jsonl(fp: io.TextIOBase) -> Iterator[Any]:
    for line_number, line in enumerate(fp, start=1):
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Некорректный JSONL в строке {line_number}: {exc}"
            ) from exc


def is_elasticsearch_bulk_action(record: Any) -> bool:
    if not isinstance(record, dict) or len(record) != 1:
        return False

    action, metadata = next(iter(record.items()))
    return action in ELASTICSEARCH_BULK_ACTIONS and isinstance(metadata, dict)


def stream_elasticsearch_bulk_jsonl(fp: io.TextIOBase) -> Iterator[Any]:
    """
    Потоково читает Elasticsearch bulk NDJSON.
    Служебные строки действий пропускаются, документы логов отдаются наружу.
    """
    pending_action: Optional[str] = None

    for line_number, line in enumerate(fp, start=1):
        line = line.strip()
        if not line:
            continue

        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Некорректный Elasticsearch bulk NDJSON в строке {line_number}: {exc}"
            ) from exc

        if is_elasticsearch_bulk_action(record):
            action = next(iter(record))
            pending_action = None if action == "delete" else action
            continue

        if pending_action == "update" and isinstance(record, dict):
            document = record.get("doc")
            if isinstance(document, dict):
                yield document
            else:
                yield record
        else:
            yield record

        pending_action = None


def read_initial_jsonl_records(path: Path, limit: int = 2) -> List[Any]:
    records: List[Any] = []

    with open_text(path, "rt") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                return records
            if len(records) >= limit:
                break

    return records


def detect_input_format(path: Path) -> str:
    if str(path).lower().endswith((".jsonl", ".ndjson", ".jsonl.gz", ".ndjson.gz")):
        initial_records = read_initial_jsonl_records(path, limit=1)
        if initial_records and is_elasticsearch_bulk_action(initial_records[0]):
            return "elasticsearch-bulk"
        return "jsonl"

    with open_text(path, "rt") as fp:
        while True:
            char = fp.read(1)
            if char == "":
                raise ValueError("Входной файл пуст")
            if char.isspace():
                continue
            if char == "[":
                return "array"
            if char == "{":
                initial_records = read_initial_jsonl_records(path, limit=2)
                if initial_records and is_elasticsearch_bulk_action(initial_records[0]):
                    return "elasticsearch-bulk"
                if len(initial_records) > 1:
                    return "jsonl"
                return "object"
            raise ValueError(f"Неизвестный формат JSON: первый символ {char!r}")


def iterate_records(
    path: Path,
    input_format: str,
    array_key: str,
) -> Iterator[Any]:
    if input_format == "auto":
        input_format = detect_input_format(path)

    if input_format == "array":
        with open_text(path, "rt") as fp:
            yield from stream_json_array(fp)
        return

    if input_format == "jsonl":
        with open_text(path, "rt") as fp:
            yield from stream_jsonl(fp)
        return

    if input_format in ("elasticsearch-bulk", "es-bulk"):
        with open_text(path, "rt") as fp:
            yield from stream_elasticsearch_bulk_jsonl(fp)
        return

    if input_format == "object":
        # Объект верхнего уровня загружается целиком.
        # Для действительно больших файлов используйте array или jsonl.
        with open_text(path, "rt") as fp:
            root = json.load(fp)

        if not isinstance(root, dict):
            raise ValueError("Для input-format=object ожидался JSON-объект")

        logs = get_nested(root, array_key)
        if not isinstance(logs, list):
            raise ValueError(
                f"В объекте не найден массив по ключу '{array_key}'. "
                "Укажите --array-key."
            )

        yield from logs
        return

    raise ValueError(f"Неподдерживаемый формат входа: {input_format}")


def template_fingerprint(template: str) -> str:
    return hashlib.sha256(template.encode("utf-8", errors="replace")).hexdigest()[:20]


def build_run_signature(
    rle_key: str,
    template_id: str,
    service: Optional[str],
    host: Optional[str],
    level: Optional[str],
) -> Tuple[Any, ...]:
    if rle_key == "template":
        return (template_id,)
    if rle_key == "template+service":
        return (template_id, service)
    if rle_key == "template+service+level":
        return (template_id, service, level)
    if rle_key == "full":
        return (template_id, service, host, level)
    raise ValueError(f"Неизвестный --rle-key: {rle_key}")


def write_json_output(
    output_path: Path,
    *,
    metadata: Dict[str, Any],
    templates: List[TemplateStats],
    top_limit: int,
    runs_temp_path: Path,
) -> None:
    """
    Записывает итоговый JSON потоково, не загружая RLE-массив в память.
    """
    with open(output_path, "w", encoding="utf-8") as out:
        out.write("{")

        out.write('"format_version":')
        json.dump(FORMAT_VERSION, out, ensure_ascii=False)

        out.write(',"metadata":')
        json.dump(metadata, out, ensure_ascii=False, separators=(",", ":"))

        out.write(',"templates":[')
        first = True
        for template_stats in templates:
            if not first:
                out.write(",")
            first = False
            json.dump(
                template_stats.to_json(top_limit),
                out,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        out.write("]")

        out.write(',"rle_runs":[')
        first = True
        with open(runs_temp_path, "r", encoding="utf-8") as runs_file:
            for line in runs_file:
                line = line.strip()
                if not line:
                    continue
                if not first:
                    out.write(",")
                first = False
                out.write(line)
        out.write("]")

        out.write("}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Преобразует большой JSON с логами в компактный JSON: "
            "шаблоны + RLE + статистика."
        )
    )
    parser.add_argument("input", type=Path, help="Входной JSON/JSONL файл")
    parser.add_argument("output", type=Path, help="Выходной JSON файл")

    parser.add_argument(
        "--input-format",
        choices=("auto", "array", "jsonl", "object", "elasticsearch-bulk", "es-bulk"),
        default="auto",
        help=(
            "Формат входа. auto определяет автоматически. "
            "Для больших файлов предпочтительны array, jsonl или elasticsearch-bulk."
        ),
    )
    parser.add_argument(
        "--array-key",
        default="logs",
        help="Путь к массиву логов при input-format=object, например data.logs",
    )

    parser.add_argument("--message-field", help="Поле сообщения, например event.message")
    parser.add_argument("--timestamp-field", help="Поле времени, например @timestamp")
    parser.add_argument("--service-field", help="Поле сервиса, например service.name")
    parser.add_argument("--host-field", help="Поле хоста, например host.name")
    parser.add_argument("--level-field", help="Поле уровня, например log.level")

    parser.add_argument(
        "--rle-key",
        choices=("template", "template+service", "template+service+level", "full"),
        default="template+service+level",
        help="Какие поля должны совпадать для объединения соседних событий в RLE.",
    )
    parser.add_argument(
        "--samples-per-template",
        type=int,
        default=5,
        help="Сколько исходных примеров сохранять для каждого шаблона.",
    )
    parser.add_argument(
        "--top-values",
        type=int,
        default=10,
        help="Сколько наиболее частых сервисов/хостов/значений сохранять.",
    )
    parser.add_argument(
        "--retain-sensitive-values",
        action="store_true",
        help=(
            "Сохранять в статистике реальные EMAIL/UUID/HASH/URL. "
            "По умолчанию они маскируются."
        ),
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100000,
        help="Показывать прогресс каждые N записей; 0 отключает.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help=(
            "После создания переформатировать JSON с отступами. "
            "Не рекомендуется для очень больших файлов."
        ),
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Остановиться при первой ошибочной записи вместо пропуска.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if not args.input.exists():
        parser.error(f"Входной файл не найден: {args.input}")

    if args.samples_per_template < 0:
        parser.error("--samples-per-template не может быть отрицательным")
    if args.top_values < 1:
        parser.error("--top-values должен быть не меньше 1")

    args.output.parent.mkdir(parents=True, exist_ok=True)

    source_size = args.input.stat().st_size
    message_bytes = 0
    input_records = 0
    skipped_records = 0
    run_count = 0

    templates_by_text: Dict[str, TemplateStats] = {}
    templates_in_order: List[TemplateStats] = []
    detected_fields: Dict[str, Counter[str]] = {
        "message": Counter(),
        "timestamp": Counter(),
        "service": Counter(),
        "host": Counter(),
        "level": Counter(),
    }

    run_state = RunState()

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        delete=False,
        prefix="log_compressor_runs_",
        suffix=".jsonl",
    ) as runs_temp:
        runs_temp_path = Path(runs_temp.name)

        try:
            for record_index, record in enumerate(
                iterate_records(args.input, args.input_format, args.array_key),
                start=1,
            ):
                try:
                    message_field, message = choose_message(record, args.message_field)
                    timestamp_field, timestamp_value = choose_context_value(
                        record,
                        args.timestamp_field,
                        TIMESTAMP_CANDIDATES,
                    )
                    service_field, service = choose_context_value(
                        record,
                        args.service_field,
                        SERVICE_CANDIDATES,
                    )
                    host_field, host = choose_context_value(
                        record,
                        args.host_field,
                        HOST_CANDIDATES,
                    )
                    level_field, level = choose_context_value(
                        record,
                        args.level_field,
                        LEVEL_CANDIDATES,
                    )

                    for kind, field_name in (
                        ("message", message_field),
                        ("timestamp", timestamp_field),
                        ("service", service_field),
                        ("host", host_field),
                        ("level", level_field),
                    ):
                        if field_name:
                            detected_fields[kind][field_name] += 1

                    timestamp, epoch = parse_timestamp(timestamp_value)
                    template_text, parameters = normalize_message(message)
                    fingerprint = template_fingerprint(template_text)

                    template_stats = templates_by_text.get(template_text)
                    if template_stats is None:
                        template_id = f"T{len(templates_in_order) + 1:06d}"
                        template_stats = TemplateStats(
                            template_id=template_id,
                            fingerprint=fingerprint,
                            template=template_text,
                            sample_limit=args.samples_per_template,
                            keep_sensitive_values=args.retain_sensitive_values,
                        )
                        templates_by_text[template_text] = template_stats
                        templates_in_order.append(template_stats)

                    template_stats.update(
                        index=record_index,
                        message=message,
                        timestamp=timestamp,
                        epoch=epoch,
                        service=service,
                        host=host,
                        level=level,
                        parameters=parameters,
                    )

                    signature = build_run_signature(
                        args.rle_key,
                        template_stats.template_id,
                        service,
                        host,
                        level,
                    )

                    if run_state.signature == signature:
                        run_state.extend(record_index, timestamp)
                    else:
                        if run_state.signature is not None:
                            runs_temp.write(
                                json.dumps(
                                    run_state.to_json(),
                                    ensure_ascii=False,
                                    separators=(",", ":"),
                                )
                                + "\n"
                            )
                            run_count += 1

                        run_state.reset(
                            signature=signature,
                            template_id=template_stats.template_id,
                            record_index=record_index,
                            timestamp=timestamp,
                            service=service,
                            host=host,
                            level=level,
                        )

                    input_records += 1
                    message_bytes += len(message.encode("utf-8", errors="replace"))

                    if (
                        args.progress_every > 0
                        and input_records % args.progress_every == 0
                    ):
                        print(
                            f"[progress] records={input_records:,} "
                            f"templates={len(templates_in_order):,} "
                            f"runs={run_count:,}",
                            file=sys.stderr,
                        )

                except Exception as exc:
                    skipped_records += 1
                    if args.strict:
                        raise
                    print(
                        f"[warning] запись {record_index} пропущена: {exc}",
                        file=sys.stderr,
                    )

            if run_state.signature is not None:
                runs_temp.write(
                    json.dumps(
                        run_state.to_json(),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                run_count += 1

        except Exception:
            try:
                runs_temp_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    try:
        metadata = {
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source_file": args.input.name,
            "source_file_size_bytes": source_size,
            "input_records": input_records,
            "skipped_records": skipped_records,
            "message_bytes": message_bytes,
            "unique_templates": len(templates_in_order),
            "rle_runs": run_count,
            "records_per_template": (
                None
                if not templates_in_order
                else input_records / len(templates_in_order)
            ),
            "records_per_rle_run": (
                None if run_count == 0 else input_records / run_count
            ),
            "config": {
                "input_format": args.input_format,
                "array_key": args.array_key,
                "rle_key": args.rle_key,
                "samples_per_template": args.samples_per_template,
                "top_values": args.top_values,
                "retain_sensitive_values": args.retain_sensitive_values,
            },
            "detected_fields": {
                kind: [
                    {"field": name, "count": count}
                    for name, count in counter.most_common()
                ]
                for kind, counter in detected_fields.items()
            },
            "parameter_units": {
                "DURATION": "milliseconds",
                "SIZE": "bytes",
            },
        }

        # Наиболее частые шаблоны в начале делают файл удобнее для LLM.
        templates_sorted = sorted(
            templates_in_order,
            key=lambda item: (-item.count, item.template_id),
        )

        write_json_output(
            args.output,
            metadata=metadata,
            templates=templates_sorted,
            top_limit=args.top_values,
            runs_temp_path=runs_temp_path,
        )

        if args.pretty:
            # Только для умеренных файлов: эта операция загружает итоговый JSON в память.
            with open(args.output, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            with open(args.output, "w", encoding="utf-8") as fp:
                json.dump(data, fp, ensure_ascii=False, indent=2)

    finally:
        try:
            runs_temp_path.unlink(missing_ok=True)
        except OSError:
            pass

    output_size = args.output.stat().st_size
    file_ratio = source_size / output_size if output_size else None
    message_ratio = message_bytes / output_size if output_size else None

    print("")
    print("Готово.")
    print(f"Входных записей:        {input_records:,}")
    print(f"Пропущено записей:      {skipped_records:,}")
    print(f"Уникальных шаблонов:    {len(templates_in_order):,}")
    print(f"RLE-блоков:             {run_count:,}")
    print(f"Размер входного файла:  {source_size:,} байт")
    print(f"Размер результата:      {output_size:,} байт")
    if file_ratio is not None:
        print(f"Сжатие по файлам:       {file_ratio:.2f}x")
    if message_ratio is not None:
        print(f"Сжатие текста логов:    {message_ratio:.2f}x")
    print(f"Результат:              {args.output}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"\nОшибка: {exc}", file=sys.stderr)
        raise SystemExit(1)
