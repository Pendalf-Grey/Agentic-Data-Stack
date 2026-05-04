import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task


CONNECT_URL = os.getenv("CONNECT_URL", "http://debezium:8083").rstrip("/")
CONNECTORS_DIR = Path(os.getenv("CONNECTORS_DIR", "/opt/airflow/connectors"))
ACTIVE_SOURCE = os.getenv("ACTIVE_SOURCE_DB", "postgres").strip().lower()
SOURCE_MODE = os.getenv("SOURCE_MODE", "external").strip().lower()
ACTIVE_PREFIX = f"{ACTIVE_SOURCE.upper()}_SOURCE"
CRON = os.getenv("AIRFLOW_MIGRATION_CRON", "0 2 * * *")
DAG_PAUSED = os.getenv("AIRFLOW_DAG_PAUSED", "true").strip().lower() == "true"

os.environ["ACTIVE_SOURCE_TOPIC"] = os.getenv(f"{ACTIVE_PREFIX}_TOPIC", "")


def required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def render_template(path: Path) -> dict:
    text = path.read_text()

    def replace(match: re.Match) -> str:
        return required_env(match.group(1))

    return json.loads(re.sub(r"\$\{([A-Z0-9_]+)\}", replace, text))


def connector_name(path: Path) -> str:
    return json.loads(path.read_text())["name"]


def connect_request(path: str, method: str = "GET", payload: dict | None = None):
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{CONNECT_URL}{path}",
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            data = response.read().decode("utf-8")
            return json.loads(data) if data else None
    except urllib.error.HTTPError as error:
        data = error.read().decode("utf-8")
        raise RuntimeError(f"{method} {path} failed with {error.code}: {data}") from error


def wait_for_connect() -> None:
    for _ in range(60):
        try:
            connect_request("/connectors")
            return
        except Exception:
            time.sleep(5)

    raise RuntimeError(f"Kafka Connect did not become ready at {CONNECT_URL}")


def upsert_connector(connector: dict) -> None:
    name = connector["name"]

    try:
        connect_request(f"/connectors/{name}")
        connect_request(f"/connectors/{name}/config", method="PUT", payload=connector["config"])
        return
    except RuntimeError as error:
        if "404" not in str(error):
            raise

    connect_request("/connectors", method="POST", payload=connector)


def delete_connector(name: str) -> None:
    try:
        connect_request(f"/connectors/{name}", method="DELETE")
    except RuntimeError as error:
        if "404" not in str(error):
            raise


@dag(
    dag_id="scheduled_debezium_migration",
    description="Register or update active Debezium source and ClickHouse sink connectors on a schedule.",
    schedule=CRON,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=DAG_PAUSED,
    tags=["agentic-data-stack", "debezium", "clickhouse"],
)
def scheduled_debezium_migration():
    @task
    def apply_active_connectors() -> dict:
        if SOURCE_MODE not in {"external", "demo"}:
            raise RuntimeError(f'SOURCE_MODE must be "external" or "demo", got "{SOURCE_MODE}"')

        source_template = CONNECTORS_DIR / f"{ACTIVE_SOURCE}-source.json"
        sink_template = CONNECTORS_DIR / "clickhouse-sink.json"

        if not source_template.exists():
            raise RuntimeError(f"Unknown ACTIVE_SOURCE_DB={ACTIVE_SOURCE}; missing {source_template}")

        wait_for_connect()

        active_connector = render_template(source_template)
        sink_connector = render_template(sink_template)

        for file in CONNECTORS_DIR.glob("*-source.json"):
            if file.name == source_template.name:
                continue
            delete_connector(connector_name(file))

        upsert_connector(active_connector)
        upsert_connector(sink_connector)

        return {
            "source_mode": SOURCE_MODE,
            "active_source_db": ACTIVE_SOURCE,
            "source_connector": active_connector["name"],
            "sink_connector": sink_connector["name"],
            "schedule": CRON,
        }

    apply_active_connectors()


scheduled_debezium_migration()
