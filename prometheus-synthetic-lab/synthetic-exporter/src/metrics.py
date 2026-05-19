import os

from scenario import (
    INCIDENTS,
    TARGETS,
    active_incident_for,
    db_state,
    service_error_ratio,
    service_latency_p95,
    service_rate,
)


# Этот файл превращает сценарий из scenario.py в OpenMetrics-текст для Prometheus.

ROUTES_BY_SERVICE = {
    "api-gateway": ["/api/v1/search", "/api/v1/profile", "/api/v1/checkout"],
    "auth-service": ["/login", "/refresh", "/oauth/callback"],
    "payment-service": ["/charge", "/refund", "/webhook/provider"],
    "orders-service": ["/orders", "/orders/{id}", "/cart/checkout"],
    "notification-service": ["/send", "/templates/render", "/delivery-status"],
}


def render_labels(values):
    """Экранирует Prometheus labels."""
    parts = []
    for key, value in values.items():
        if value is None or value == "":
            continue
        escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'{key}="{escaped}"')
    return ",".join(parts)


def sample(name, labels, value, timestamp=None):
    """Рендерит одну строку OpenMetrics sample."""
    suffix = "" if timestamp is None else f" {timestamp}"
    return f"{name}{{{render_labels(labels)}}} {float(value):.6f}{suffix}"


def counter_value(base_rate_per_minute, date, salt=0):
    """Синтетический counter, растущий от времени."""
    minutes = int(date.timestamp() // 60)
    return max(0, minutes * base_rate_per_minute + salt)


def render_metrics(date, include_timestamp=True):
    """Возвращает полный OpenMetrics payload для /metrics или history-файла."""
    ts = int(date.timestamp()) if include_timestamp else None
    lines = [
        "# HELP synthetic_service_up Whether the monitored target is up.",
        "# TYPE synthetic_service_up gauge",
        "# HELP synthetic_incident_active Whether a synthetic incident is active.",
        "# TYPE synthetic_incident_active gauge",
        "# HELP synthetic_log_events_total Synthetic log events by level and event type.",
        "# TYPE synthetic_log_events_total counter",
        "# HELP synthetic_http_requests_total Synthetic HTTP requests by service, route, and status class.",
        "# TYPE synthetic_http_requests_total counter",
        "# HELP synthetic_http_request_duration_seconds_p95 Synthetic HTTP request p95 latency in seconds.",
        "# TYPE synthetic_http_request_duration_seconds_p95 gauge",
        "# HELP synthetic_db_connections Active database connections.",
        "# TYPE synthetic_db_connections gauge",
        "# HELP synthetic_db_query_duration_seconds_p95 Synthetic database query p95 latency in seconds.",
        "# TYPE synthetic_db_query_duration_seconds_p95 gauge",
        "# HELP synthetic_db_replication_lag_seconds Synthetic database replication lag in seconds.",
        "# TYPE synthetic_db_replication_lag_seconds gauge",
        "# HELP synthetic_db_disk_usage_ratio Synthetic database disk usage ratio.",
        "# TYPE synthetic_db_disk_usage_ratio gauge",
        "# HELP synthetic_process_restarts_total Synthetic process restart counter.",
        "# TYPE synthetic_process_restarts_total counter",
    ]

    for target in TARGETS:
        common = {
            "job": target["job"],
            "instance": target["instance"],
            "service": target["service"],
            "team": target["team"],
            "tier": target["tier"],
            "environment": os.getenv("ENVIRONMENT", "synthetic-prod"),
        }
        incident = active_incident_for(target["service"], date)
        up = db_state(target, date)["up"] if target["kind"] == "db" else 1
        lines.append(sample("synthetic_service_up", {**common, "component_kind": target["kind"]}, up, ts))

        for item in INCIDENTS:
            if item["service"] == target["service"]:
                lines.append(sample("synthetic_incident_active", {**common, "incident": item["name"], "severity": item["severity"]}, 1 if incident and incident["name"] == item["name"] else 0, ts))

        if target["kind"] == "db":
            state = db_state(target, date)
            lines.append(sample("synthetic_db_connections", {**common, "db_engine": target["engine"], "state": "active"}, state["connections"], ts))
            lines.append(sample("synthetic_db_query_duration_seconds_p95", {**common, "db_engine": target["engine"]}, state["queryP95"], ts))
            lines.append(sample("synthetic_db_replication_lag_seconds", {**common, "db_engine": target["engine"]}, state["replicationLag"], ts))
            lines.append(sample("synthetic_db_disk_usage_ratio", {**common, "db_engine": target["engine"], "mount": "/var/lib/data"}, state["diskUsage"], ts))
            lines.append(sample("synthetic_process_restarts_total", {**common, "reason": "crash_loop"}, counter_value(state["restarts"] / 60, date, len(target["service"])), ts))
            error_rate = 20 if state["up"] == 0 else 3 if state["queryP95"] > 0.2 else 0.2
            lines.append(sample("synthetic_log_events_total", {**common, "level": "error", "event": "database_unavailable" if state["up"] == 0 else "slow_query"}, counter_value(error_rate, date, len(target["service"]) * 11), ts))
            lines.append(sample("synthetic_log_events_total", {**common, "level": "info", "event": "healthcheck_ok"}, counter_value(0 if state["up"] == 0 else 12, date, len(target["service"]) * 13), ts))
            continue

        routes = ROUTES_BY_SERVICE.get(target["service"], ["/"])
        rpm = service_rate(target["service"], date)
        err = service_error_ratio(target["service"], date)
        for route in routes:
            route_rpm = rpm / len(routes)
            lines.append(sample("synthetic_http_requests_total", {**common, "route": route, "status_class": "2xx"}, counter_value(route_rpm * (1 - err), date, len(route)), ts))
            lines.append(sample("synthetic_http_requests_total", {**common, "route": route, "status_class": "5xx"}, counter_value(route_rpm * err, date, len(route) * 7), ts))
            lines.append(sample("synthetic_http_request_duration_seconds_p95", {**common, "route": route}, service_latency_p95(target["service"], route, date), ts))
        lines.append(sample("synthetic_log_events_total", {**common, "level": "error", "event": "request_failed" if incident else "handled_exception"}, counter_value(rpm * err, date, len(target["service"]) * 19), ts))
        lines.append(sample("synthetic_log_events_total", {**common, "level": "warn", "event": "degraded_dependency" if incident else "retry"}, counter_value(rpm * 0.07 if incident else rpm * 0.003, date, len(target["service"]) * 23), ts))
        lines.append(sample("synthetic_log_events_total", {**common, "level": "info", "event": "request_completed"}, counter_value(rpm * 0.96, date, len(target["service"]) * 29), ts))
        lines.append(sample("synthetic_process_restarts_total", {**common, "reason": "oom_or_probe_failure" if incident else "rolling_deploy"}, counter_value(0.08 if incident else 0.0002, date, len(target["service"]) * 31), ts))

    return "\n".join(lines) + "\n"
