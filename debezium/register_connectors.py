import json
import os
import re
import socket
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


# Этот init job регистрирует Debezium source connector и ClickHouse sink connector.
# Поток данных после регистрации: Postgres -> Debezium/Kafka Connect -> Apache Kafka -> ClickHouse sink -> ClickHouse.

CONNECT_URL = os.getenv("CONNECT_URL", "http://debezium:8083").rstrip("/")
SOURCE_MODE = os.getenv("SOURCE_MODE", "external").strip().lower()
ACTIVE_SOURCE = os.getenv("ACTIVE_SOURCE_DB", "postgres").strip().lower()
CONNECTORS_DIR = Path("/connectors")
SOURCE_TEMPLATE = CONNECTORS_DIR / f"{ACTIVE_SOURCE}-source.json"
SINK_TEMPLATE = CONNECTORS_DIR / "clickhouse-sink.json"
ACTIVE_PREFIX = f"{ACTIVE_SOURCE.upper()}_SOURCE"
os.environ["ACTIVE_SOURCE_TOPIC"] = os.getenv(f"{ACTIVE_PREFIX}_TOPIC", "")


def required_env(name):
    """Возвращает env var для шаблона connector JSON или падает с понятной ошибкой."""
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def render_template(text):
    """Подставляет ${ENV_NAME} внутри connector JSON."""
    return re.sub(r"\$\{([A-Z0-9_]+)\}", lambda match: required_env(match.group(1)), text)


def read_connector(path):
    """Читает и рендерит connector template."""
    return json.loads(render_template(path.read_text(encoding="utf-8")))


def read_connector_name(path):
    """Читает имя connector из template без подстановок."""
    return json.loads(path.read_text(encoding="utf-8"))["name"]


def request(path, method="GET", body=None):
    """HTTP request к Kafka Connect с retry на rebalance."""
    payload = None if body is None else json.dumps(body).encode("utf-8")
    for attempt in range(1, 13):
        try:
            req = Request(f"{CONNECT_URL}{path}", data=payload, method=method, headers={"Content-Type": "application/json"})
            with urlopen(req, timeout=30) as response:
                text = response.read().decode("utf-8")
                return json.loads(text) if text else None
        except HTTPError as error:
            text = error.read().decode("utf-8", errors="replace")
            is_rebalance = "rebalance" in text.lower()
            if is_rebalance and attempt < 12:
                print(f"{method} {path} is waiting for Kafka Connect rebalance; retry {attempt}/12")
                time.sleep(5)
                continue
            raise RuntimeError(f"{method} {path} failed with {error.code}: {text}") from error


def wait_for_connect():
    """Ждет готовность Kafka Connect REST API."""
    print(f"Waiting for Kafka Connect at {CONNECT_URL}...")
    for _ in range(60):
        try:
            request("/connectors")
            return
        except Exception:
            time.sleep(5)
    raise RuntimeError(f"Kafka Connect did not become ready at {CONNECT_URL}")


def wait_for_port(host, port):
    """Проверяет TCP доступность source DB."""
    with socket.create_connection((host, int(port)), timeout=3):
        return True


def wait_for_source():
    """Ждет активную source DB, если для нее заданы HOST/PORT."""
    host = os.getenv(f"{ACTIVE_PREFIX}_HOST")
    port = os.getenv(f"{ACTIVE_PREFIX}_PORT")
    if not host or not port:
        return
    print(f"Waiting for active source {ACTIVE_SOURCE} at {host}:{port}...")
    for _ in range(60):
        try:
            wait_for_port(host, port)
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"Source {ACTIVE_SOURCE} did not become reachable at {host}:{port}")


def upsert_connector(connector):
    """Создает connector или обновляет config, если он уже существует."""
    try:
        request(f"/connectors/{connector['name']}")
        print(f"Updating connector {connector['name']}")
        request(f"/connectors/{connector['name']}/config", method="PUT", body=connector["config"])
    except RuntimeError as error:
        if "404" not in str(error):
            raise
        print(f"Registering connector {connector['name']}")
        request("/connectors", method="POST", body=connector)


def delete_connector(name):
    """Удаляет неактивный source connector, чтобы не работало две source DB сразу."""
    try:
        request(f"/connectors/{name}", method="DELETE")
        print(f"Deleted inactive source connector {name}")
    except RuntimeError as error:
        if "404" not in str(error):
            raise


def list_source_templates():
    """Находит все *-source.json templates."""
    return [path for path in CONNECTORS_DIR.iterdir() if path.name.endswith("-source.json")]


def main():
    """Главный сценарий init job."""
    if SOURCE_MODE not in ("external", "demo"):
        raise RuntimeError(f'SOURCE_MODE must be "external" or "demo", got "{SOURCE_MODE}"')
    print(f"Source mode is {SOURCE_MODE}; active source is {ACTIVE_SOURCE}")
    if not SOURCE_TEMPLATE.exists():
        raise RuntimeError(f"Missing source connector template: {SOURCE_TEMPLATE}")

    wait_for_source()
    wait_for_connect()

    active_connector = read_connector(SOURCE_TEMPLATE)
    sink_connector = read_connector(SINK_TEMPLATE)

    for path in list_source_templates():
        if path.name == f"{ACTIVE_SOURCE}-source.json":
            continue
        delete_connector(read_connector_name(path))

    upsert_connector(active_connector)
    upsert_connector(sink_connector)
    print(f"Debezium source is {ACTIVE_SOURCE}; connectors are ready")


if __name__ == "__main__":
    main()
