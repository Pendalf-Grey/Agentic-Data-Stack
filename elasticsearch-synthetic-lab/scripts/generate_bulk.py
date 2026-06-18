import bisect
import copy
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path


# Генератор остаётся универсальным: предметные сценарии и коэффициенты живут в JSON-профиле.
LAB_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SCENARIO_FILE = LAB_DIR / "config" / "log_scenarios.json"


def load_scenario():
    configured = Path(os.getenv("ELASTICSEARCH_DEMO_SCENARIO_FILE", str(DEFAULT_SCENARIO_FILE))).expanduser()
    scenario_path = configured if configured.is_absolute() else (Path.cwd() / configured).resolve()
    try:
        scenario = json.loads(scenario_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"Не удалось прочитать профиль журналов {scenario_path}: {error}") from error

    required_sections = {
        "generation",
        "document",
        "latency",
        "traffic",
        "quality",
        "services",
        "incidents",
        "data_gaps",
    }
    missing = sorted(required_sections.difference(scenario))
    if missing:
        raise SystemExit(f"В профиле журналов отсутствуют разделы: {', '.join(missing)}")
    if not scenario["services"]:
        raise SystemExit("Профиль журналов должен содержать хотя бы одну службу")

    service_names = {service.get("name") for service in scenario["services"]}
    unknown_services = sorted({item.get("service") for item in scenario["incidents"]}.difference(service_names))
    if unknown_services:
        raise SystemExit(f"Инциденты ссылаются на неизвестные службы: {', '.join(unknown_services)}")
    return scenario_path, scenario


SCENARIO_FILE, SCENARIO = load_scenario()
GENERATION = SCENARIO["generation"]
DOCUMENT = SCENARIO["document"]
LATENCY = SCENARIO["latency"]
TRAFFIC = SCENARIO["traffic"]
QUALITY = SCENARIO["quality"]
SERVICES = SCENARIO["services"]
INCIDENTS = SCENARIO["incidents"]
DATA_GAPS = SCENARIO["data_gaps"]


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


_INCIDENT_INTERVALS = None
_DATA_GAP_INTERVALS = None


def incident_intervals():
    global _INCIDENT_INTERVALS
    if _INCIDENT_INTERVALS is None:
        _INCIDENT_INTERVALS = [
            {
                **incident,
                "start_at": parse_datetime(incident["start"], incident["start"]),
                "end_at": parse_datetime(incident["end"], incident["end"]),
            }
            for incident in INCIDENTS
        ]
    return _INCIDENT_INTERVALS


def data_gap_intervals():
    global _DATA_GAP_INTERVALS
    if _DATA_GAP_INTERVALS is None:
        _DATA_GAP_INTERVALS = [
            (parse_datetime(item["start"], item["start"]), parse_datetime(item["end"], item["end"]))
            for item in DATA_GAPS
        ]
    return _DATA_GAP_INTERVALS


def incident_context(timestamp, service_name):
    warning_delta = timedelta(hours=float(TRAFFIC["warning_hours_before"]))
    recovery_delta = timedelta(hours=float(TRAFFIC["recovery_hours_after"]))
    for incident in incident_intervals():
        if incident["service"] != service_name:
            continue
        if incident["start_at"] <= timestamp < incident["end_at"]:
            return incident, "active"
        if incident["start_at"] - warning_delta <= timestamp < incident["start_at"]:
            return incident, "warning"
        if incident["end_at"] <= timestamp < incident["end_at"] + recovery_delta:
            return incident, "recovery"
    return None, "normal"


def traffic_weight(timestamp):
    if any(start <= timestamp < end for start, end in data_gap_intervals()):
        return 0.0

    base_year = int(TRAFFIC["base_year"])
    year_growth = 1.0 + max(0, timestamp.year - base_year) * float(TRAFFIC["annual_growth"])
    weekday_weight = float(TRAFFIC["weekend_weight"]) if timestamp.weekday() >= 5 else 1.0
    hour_weight = float(TRAFFIC["default_hour_weight"])
    for interval in TRAFFIC["hour_weights"]:
        if int(interval["start"]) <= timestamp.hour < int(interval["end"]):
            hour_weight = float(interval["weight"])
            break
    seasonal_weight = float(
        TRAFFIC["month_weights"].get(str(timestamp.month), TRAFFIC["default_month_weight"])
    )

    incident_weight = 1.0
    warning_delta = timedelta(hours=float(TRAFFIC["warning_hours_before"]))
    recovery_delta = timedelta(hours=float(TRAFFIC["recovery_hours_after"]))
    for incident in incident_intervals():
        if incident["start_at"] <= timestamp < incident["end_at"]:
            incident_weight = float(TRAFFIC["incident_active_weight"])
            break
        if incident["start_at"] - warning_delta <= timestamp < incident["end_at"] + recovery_delta:
            incident_weight = max(incident_weight, float(TRAFFIC["incident_context_weight"]))
    return year_growth * weekday_weight * hour_weight * seasonal_weight * incident_weight


def weighted_hours(start, end):
    hours = []
    cumulative = []
    total = 0.0
    cursor = start.replace(minute=0, second=0, microsecond=0)
    while cursor < end:
        weight = traffic_weight(cursor)
        if weight > 0:
            hours.append(cursor)
            total += weight
            cumulative.append(total)
        cursor += timedelta(hours=1)
    if not hours:
        raise SystemExit("В заданном периоде не осталось часов для генерации журналов")
    return hours, cumulative, total


def timestamp_for_fraction(fraction, seq, hours, cumulative, total_weight, end):
    target = min(max(fraction, 0.0), 0.999999999) * total_weight
    hour_index = min(bisect.bisect_left(cumulative, target), len(hours) - 1)
    second = stable_int(seq + 97, 3600)
    return min(hours[hour_index] + timedelta(seconds=second), end - timedelta(milliseconds=1))


def release_for(timestamp, service_name):
    week = int(timestamp.strftime("%U"))
    return f"{service_name}-{timestamp.year}.{week:02d}.{stable_int(week + len(service_name), 17)}"


def format_message(template, incident):
    return str(template).format(incident=incident["name"])


def document_for(seq, timestamp):
    service = SERVICES[seq % len(SERVICES)]
    path = service["paths"][(seq // len(SERVICES)) % len(service["paths"])]
    methods = DOCUMENT["methods"]
    agents = DOCUMENT["user_agents"]
    countries = DOCUMENT["countries"]
    workers = DOCUMENT["workers"]
    payload_classes = DOCUMENT["payload_classes"]
    method = methods[stable_int(seq, len(methods))]
    agent = agents[stable_int(seq + 3, len(agents))]
    country = countries[stable_int(seq + 7, len(countries))]
    worker = workers[stable_int(seq + 11, len(workers))]
    incident, incident_phase = incident_context(timestamp, service["name"])

    base_latency = int(LATENCY["base_min_ms"]) + stable_int(seq + timestamp.minute, int(LATENCY["base_spread_ms"]))
    upstream_latency = max(3, base_latency - stable_int(seq + 5, int(LATENCY["upstream_delta_ms"])))
    status = 200
    level = "info"
    error_code = ""
    message = DOCUMENT["normal_message"]
    incident_name = "none"

    if stable_int(seq + timestamp.day, int(QUALITY["warning_modulo"])) == 0:
        level = "warn"
        error_code = service["warning_code"]
        message = service["warning_message"]
        upstream_latency += int(LATENCY["warning_add_min_ms"]) + stable_int(seq, int(LATENCY["warning_add_spread_ms"]))
        base_latency = upstream_latency + stable_int(seq + 13, int(LATENCY["response_overhead_ms"]))

    if stable_int(seq + timestamp.day, int(QUALITY["background_error_modulo"])) == 0:
        status = 500
        level = "error"
        error_code = service["background_error_code"]
        message = service["background_error_message"]
        upstream_latency += int(LATENCY["background_error_add_ms"])
        base_latency = upstream_latency + stable_int(seq + 13, int(LATENCY["response_overhead_ms"]))

    if incident and incident_phase == "warning":
        level = "warn"
        error_code = f"{incident['error_code']}_EARLY_WARNING"
        message = format_message(DOCUMENT["early_warning_message"], incident)
        upstream_latency += incident["latency_add"] // int(LATENCY["early_warning_divisor"])
        upstream_latency += stable_int(seq, int(LATENCY["early_warning_spread_ms"]))
        base_latency = upstream_latency + stable_int(seq + 13, int(LATENCY["response_overhead_ms"]))

    if incident and incident_phase == "active":
        incident_name = incident["name"]
        upstream_latency += incident["latency_add"] + stable_int(seq, int(LATENCY["incident_spread_ms"]))
        base_latency = upstream_latency + stable_int(seq + 13, int(LATENCY["response_overhead_ms"]))
        if seq % int(incident["error_rate"]) == 0:
            status = int(incident["status"])
            level = "error"
            error_code = incident["error_code"]
            message = incident["message"]
        else:
            exception_statuses = {int(value) for value in DOCUMENT["degraded_status_exceptions"]}
            status = 200 if int(incident["status"]) not in exception_statuses else int(incident["status"])
            level = "warn"
            error_code = incident["error_code"] if status >= 400 else ""
            message = format_message(DOCUMENT["degraded_message"], incident)

    if incident and incident_phase == "recovery":
        level = "warn"
        error_code = f"{incident['error_code']}_RECOVERY"
        message = format_message(DOCUMENT["recovery_message"], incident)
        upstream_latency += incident["latency_add"] // int(LATENCY["recovery_divisor"])
        upstream_latency += stable_int(seq, int(LATENCY["recovery_spread_ms"]))
        base_latency = upstream_latency + stable_int(seq + 13, int(LATENCY["response_overhead_ms"]))

    request_id = f"req-{timestamp.strftime('%Y%m%d%H%M%S')}-{seq:09d}"
    trace_id = f"trc-{timestamp.strftime('%Y%m%d')}-{stable_int(seq, 10_000_000):07d}"
    user_id = f"user-{100_000 + stable_int(seq + 29, 900_000)}"
    remote_addr = f"10.{stable_int(seq, 240)}.{stable_int(seq + 1, 240)}.{1 + stable_int(seq + 2, 250)}"
    bytes_sent = 480 + stable_int(seq + 17, 180_000)

    return {
        "@timestamp": iso(timestamp),
        "event": {"dataset": DOCUMENT["event_dataset"], "kind": DOCUMENT["event_kind"]},
        "environment": DOCUMENT["environment"],
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
            "referer": DOCUMENT["referer"],
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
            "source": DOCUMENT["source_label"],
            "log_flavor": DOCUMENT["log_flavor"],
        },
        "debug": {
            "retry_count": stable_int(seq + 53, 4) if level != "info" else 0,
            "circuit_breaker_open": incident_name != "none" and stable_int(seq + 59, 9) == 0,
            "payload_class": payload_classes[stable_int(seq + 61, len(payload_classes))],
        },
    }


def apply_quality_variations(document, seq):
    if stable_int(seq + 101, int(QUALITY["missing_user_modulo"])) == 0:
        document.pop("user_id", None)
    if stable_int(seq + 103, int(QUALITY["missing_latency_modulo"])) == 0:
        document["http"].pop("upstream_latency_ms", None)
        document["nginx"].pop("upstream_response_time", None)
    if stable_int(seq + 107, int(QUALITY["missing_geo_modulo"])) == 0:
        document.pop("geo", None)
    return document


def main():
    docs = int(os.getenv("ELASTICSEARCH_DEMO_DOCS", str(GENERATION["documents"])))
    target_mb = int(os.getenv("ELASTICSEARCH_DEMO_TARGET_MB", str(GENERATION["target_mb"])) or "0")
    index_prefix = os.getenv("ELASTICSEARCH_DEMO_INDEX_PREFIX", GENERATION["index_prefix"])
    index_granularity = os.getenv("ELASTICSEARCH_DEMO_INDEX_GRANULARITY", GENERATION["index_granularity"]).strip().lower()
    output = Path(os.getenv("OUTPUT", GENERATION["output"]))
    start = parse_datetime(os.getenv("ELASTICSEARCH_DEMO_START"), GENERATION["start"])
    end = parse_datetime(os.getenv("ELASTICSEARCH_DEMO_END"), GENERATION["end"])
    if end <= start:
        raise SystemExit("ELASTICSEARCH_DEMO_END должна быть позже ELASTICSEARCH_DEMO_START")

    target_bytes = target_mb * 1024 * 1024
    output.parent.mkdir(parents=True, exist_ok=True)
    counts = {}
    bytes_written = 0
    seq = 0
    max_docs = docs if target_bytes <= 0 else int(GENERATION["max_documents"])
    hours, cumulative_weights, total_weight = weighted_hours(start, end)
    previous_document = None

    with output.open("w", encoding="utf-8") as stream:
        while seq < max_docs and (target_bytes <= 0 or bytes_written < target_bytes):
            fraction = seq / max(max_docs, 1) if target_bytes <= 0 else bytes_written / max(target_bytes, 1)
            timestamp = timestamp_for_fraction(fraction, seq, hours, cumulative_weights, total_weight, end)
            if index_granularity == "daily":
                index_suffix = timestamp.strftime("%Y.%m.%d")
            elif index_granularity == "yearly":
                index_suffix = timestamp.strftime("%Y")
            else:
                index_suffix = timestamp.strftime("%Y.%m")
            index_name = f"{index_prefix}-{index_suffix}"
            document_id = f"{index_name}-{seq:010d}"
            document = apply_quality_variations(document_for(seq, timestamp), seq)
            if previous_document is not None and stable_int(seq + 109, int(QUALITY["duplicate_modulo"])) == 0:
                document = copy.deepcopy(previous_document)
            previous_document = document
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
                "scenarioFile": str(SCENARIO_FILE),
                "scenarioSchemaVersion": SCENARIO.get("schema_version"),
                "documents": seq,
                "targetMB": target_mb,
                "bytes": bytes_written,
                "indexPrefix": index_prefix,
                "indexGranularity": index_granularity,
                "indexPattern": f"{index_prefix}-*",
                "start": iso(start),
                "end": iso(end),
                "services": [service["name"] for service in SERVICES],
                "incidents": [
                    {"name": item["name"], "service": item["service"], "start": item["start"], "end": item["end"]}
                    for item in INCIDENTS
                ],
                "dataGaps": DATA_GAPS,
                "indices": counts,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
