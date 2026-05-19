import math


# Этот файл описывает синтетическую инфраструктуру для Prometheus.
# metrics.py читает эти функции и генерирует правдоподобные метрики сервисов и БД.

TARGETS = [
    {"kind": "db", "engine": "postgresql", "service": "db-postgres-primary", "job": "postgresql", "instance": "postgresql-primary:5432", "team": "platform", "tier": "data"},
    {"kind": "db", "engine": "mysql", "service": "db-mysql-orders", "job": "mysql", "instance": "mysql-orders-1:3306", "team": "orders", "tier": "data"},
    {"kind": "db", "engine": "mysql", "service": "db-mysql-billing", "job": "mysql", "instance": "mysql-billing-1:3306", "team": "billing", "tier": "data"},
    {"kind": "db", "engine": "mongodb", "service": "db-mongodb-users", "job": "mongodb", "instance": "mongodb-users-1:27017", "team": "identity", "tier": "data"},
    {"kind": "db", "engine": "mongodb", "service": "db-mongodb-events", "job": "mongodb", "instance": "mongodb-events-1:27017", "team": "analytics", "tier": "data"},
    {"kind": "service", "service": "api-gateway", "job": "service", "instance": "api-gateway-1:8080", "team": "platform", "tier": "edge"},
    {"kind": "service", "service": "auth-service", "job": "service", "instance": "auth-service-1:8080", "team": "identity", "tier": "app"},
    {"kind": "service", "service": "payment-service", "job": "service", "instance": "payment-service-1:8080", "team": "billing", "tier": "app"},
    {"kind": "service", "service": "orders-service", "job": "service", "instance": "orders-service-1:8080", "team": "orders", "tier": "app"},
    {"kind": "service", "service": "notification-service", "job": "service", "instance": "notification-service-1:8080", "team": "messaging", "tier": "app"},
]

INCIDENTS = [
    {"name": "payment_gateway_degradation", "service": "payment-service", "severity": "critical", "startMinute": 9 * 60 + 30, "endMinute": 10 * 60 + 25},
    {"name": "mysql_billing_crash_loop", "service": "db-mysql-billing", "severity": "critical", "startMinute": 13 * 60 + 5, "endMinute": 13 * 60 + 28},
    {"name": "mongodb_events_disk_pressure", "service": "db-mongodb-events", "severity": "warning", "startMinute": 17 * 60, "endMinute": 19 * 60 + 15},
    {"name": "postgres_checkpoint_saturation", "service": "db-postgres-primary", "severity": "warning", "startMinute": 2 * 60 + 15, "endMinute": 3 * 60 + 10},
    {"name": "notification_queue_backlog", "service": "notification-service", "severity": "warning", "startMinute": 21 * 60, "endMinute": 22 * 60 + 20},
]


def minute_of_day(date):
    """Возвращает минуту UTC-дня для воспроизводимого расписания инцидентов."""
    return date.hour * 60 + date.minute


def daily_wave(date, period_minutes=1440, phase=0):
    """Плавная дневная волна нагрузки."""
    minute = minute_of_day(date)
    return (math.sin(((minute + phase) / period_minutes) * math.pi * 2) + 1) / 2


def is_incident_active(incident, date):
    """Проверяет, активен ли инцидент в текущую минуту."""
    minute = minute_of_day(date)
    return incident["startMinute"] <= minute <= incident["endMinute"]


def active_incident_for(service, date):
    """Находит активный инцидент для сервиса, если он есть."""
    return next((incident for incident in INCIDENTS if incident["service"] == service and is_incident_active(incident, date)), None)


def hash_number(text):
    """Стабильный hash для jitter без зависимости от random seed."""
    value = 0
    for char in text:
        value = ((value << 5) - value + ord(char)) & 0xFFFFFFFF
    return abs(value if value < 0x80000000 else value - 0x100000000)


def jitter(text, date, amplitude=1):
    """Небольшое детерминированное колебание, чтобы графики не были идеально плоскими."""
    bucket = int(date.timestamp() // 60)
    return math.sin((hash_number(text) + bucket * 17) * 0.013) * amplitude


def service_rate(service, date):
    """Синтетический requests-per-minute для application services."""
    base = {"api-gateway": 420, "auth-service": 95, "payment-service": 55, "orders-service": 120, "notification-service": 38}.get(service, 15)
    wave = 0.65 + daily_wave(date, 1440, -360) * 0.7
    incident = active_incident_for(service, date)
    multiplier = 1.45 if incident and incident["name"] == "payment_gateway_degradation" else 1
    return max(1, base * wave * multiplier + jitter(service, date, base * 0.04))


def service_latency_p95(service, route, date):
    """Синтетический p95 latency для HTTP route."""
    base = {"api-gateway": 0.18, "auth-service": 0.11, "payment-service": 0.28, "orders-service": 0.22, "notification-service": 0.35}.get(service, 0.2)
    route_multiplier = 1.7 if route == "/checkout" else 1.2 if route == "/login" else 1.6 if route == "/send" else 1
    incident = active_incident_for(service, date)
    incident_multiplier = 8 if incident and incident["name"] == "payment_gateway_degradation" else 3.2 if incident and incident["name"] == "notification_queue_backlog" else 1
    return max(0.01, base * route_multiplier * incident_multiplier + jitter(f"{service}:{route}:lat", date, 0.025))


def service_error_ratio(service, date):
    """Синтетическая доля ошибок HTTP."""
    base = {"api-gateway": 0.002, "auth-service": 0.0015, "payment-service": 0.004, "orders-service": 0.0025, "notification-service": 0.003}.get(service, 0.002)
    incident = active_incident_for(service, date)
    if incident and incident["name"] == "payment_gateway_degradation":
        return 0.18 + abs(jitter(service, date, 0.025))
    if incident and incident["name"] == "notification_queue_backlog":
        return 0.045 + abs(jitter(service, date, 0.012))
    return max(0, base + abs(jitter(f"{service}:err", date, base)))


def db_state(target, date):
    """Синтетическое состояние БД: up/down, latency, lag, disk, restarts."""
    incident = active_incident_for(target["service"], date)
    down = incident and incident["name"] == "mysql_billing_crash_loop"
    pressure = incident and incident["name"] == "mongodb_events_disk_pressure"
    checkpoint = incident and incident["name"] == "postgres_checkpoint_saturation"
    engine = target["engine"]
    return {
        "up": 0 if down else 1,
        "connections": 0 if down else round((82 if engine == "postgresql" else 64 if engine == "mysql" else 48) + daily_wave(date, 1440, -240) * 26 + jitter(f"{target['service']}:conn", date, 6)),
        "queryP95": 0 if down else max(0.005, (0.045 if engine == "postgresql" else 0.035 if engine == "mysql" else 0.055) * (6 if checkpoint else 3.8 if pressure else 1) + jitter(f"{target['service']}:query", date, 0.008)),
        "replicationLag": 180 if down else max(0, (45 if checkpoint else 18 if pressure else 1.5) + abs(jitter(f"{target['service']}:lag", date, 3))),
        "diskUsage": min(0.98, (0.62 if engine == "mongodb" else 0.54) + (0.28 if pressure else 0) + daily_wave(date, 1440, 180) * 0.05 + abs(jitter(f"{target['service']}:disk", date, 0.015))),
        "restarts": 1 if down else 0,
    }
