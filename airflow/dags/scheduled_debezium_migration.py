import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task


# URL Kafka Connect REST API внутри docker-compose сети.
# Через него DAG создает, обновляет и удаляет Debezium/Kafka Connect коннекторы.
CONNECT_URL = os.getenv("CONNECT_URL", "http://debezium:8083").rstrip("/")

# Каталог с JSON-шаблонами коннекторов.
# В контейнер Airflow он приходит из ./debezium/connectors как read-only volume.
CONNECTORS_DIR = Path(os.getenv("CONNECTORS_DIR", "/opt/airflow/connectors"))

# Какая исходная БД сейчас активна: например postgres.
# По этому имени DAG выбирает файл postgres-source.json.
ACTIVE_SOURCE = os.getenv("ACTIVE_SOURCE_DB", "postgres").strip().lower()

# Режим источника: demo или external.
# Это защита от случайного запуска с неизвестным режимом.
SOURCE_MODE = os.getenv("SOURCE_MODE", "external").strip().lower()

# Префикс env-переменных активного источника.
# Например для ACTIVE_SOURCE_DB=postgres используются POSTGRES_SOURCE_*.
ACTIVE_PREFIX = f"{ACTIVE_SOURCE.upper()}_SOURCE"

# Расписание DAG в cron-формате.
# Значение приходит из .env и управляет периодичностью обновления коннекторов.
CRON = os.getenv("AIRFLOW_MIGRATION_CRON", "0 2 * * *")

# Позволяет создать DAG сразу выключенным, чтобы пользователь сам включил его в UI.
DAG_PAUSED = os.getenv("AIRFLOW_DAG_PAUSED", "true").strip().lower() == "true"

# Этот env нужен JSON-шаблону ClickHouse sink connector.
# Он говорит sink-коннектору, из какого Kafka topic читать данные активного источника.
os.environ["ACTIVE_SOURCE_TOPIC"] = os.getenv(f"{ACTIVE_PREFIX}_TOPIC", "")


def required_env(name: str) -> str:
    """Возвращает обязательную env-переменную или явно падает с понятной ошибкой."""
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def render_template(path: Path) -> dict:
    """Подставляет env-переменные в JSON-шаблон коннектора и возвращает dict."""
    text = path.read_text()

    def replace(match: re.Match) -> str:
        # Каждая конструкция ${VAR_NAME} должна иметь значение в окружении Airflow.
        return required_env(match.group(1))

    return json.loads(re.sub(r"\$\{([A-Z0-9_]+)\}", replace, text))


def connector_name(path: Path) -> str:
    """Читает имя Kafka Connect connector из JSON-шаблона."""
    return json.loads(path.read_text())["name"]


def connect_request(path: str, method: str = "GET", payload: dict | None = None):
    """Выполняет HTTP-запрос к Kafka Connect REST API и возвращает JSON-ответ."""
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
        # Kafka Connect часто возвращает полезное тело ошибки, поэтому пробрасываем его в Airflow logs.
        data = error.read().decode("utf-8")
        raise RuntimeError(f"{method} {path} failed with {error.code}: {data}") from error


def wait_for_connect() -> None:
    """Ждет готовности Kafka Connect, чтобы не регистрировать коннекторы слишком рано."""
    for _ in range(60):
        try:
            connect_request("/connectors")
            return
        except Exception:
            time.sleep(5)

    raise RuntimeError(f"Kafka Connect did not become ready at {CONNECT_URL}")


def upsert_connector(connector: dict) -> None:
    """Создает connector, если его нет, или обновляет config уже существующего connector."""
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
    """Удаляет неактивный source connector; отсутствие connector не считается ошибкой."""
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
    # DAG содержит одну задачу: привести Kafka Connect к нужной конфигурации.
    @task
    def apply_active_connectors() -> dict:
        """Выбирает активный source connector и обновляет связанный ClickHouse sink connector."""
        if SOURCE_MODE not in {"external", "demo"}:
            raise RuntimeError(f'SOURCE_MODE must be "external" or "demo", got "{SOURCE_MODE}"')

        # Source connector зависит от ACTIVE_SOURCE_DB, sink connector общий для загрузки в ClickHouse.
        source_template = CONNECTORS_DIR / f"{ACTIVE_SOURCE}-source.json"
        sink_template = CONNECTORS_DIR / "clickhouse-sink.json"

        if not source_template.exists():
            raise RuntimeError(f"Unknown ACTIVE_SOURCE_DB={ACTIVE_SOURCE}; missing {source_template}")

        wait_for_connect()

        # На этом шаге JSON-шаблоны превращаются в реальные конфиги Kafka Connect.
        active_connector = render_template(source_template)
        sink_connector = render_template(sink_template)

        # Чтобы в Kafka Connect не висели старые источники, удаляем все source connectors кроме активного.
        for file in CONNECTORS_DIR.glob("*-source.json"):
            if file.name == source_template.name:
                continue
            delete_connector(connector_name(file))

        # Источник пишет изменения в Kafka topic, sink читает topic и пишет строки в ClickHouse.
        upsert_connector(active_connector)
        upsert_connector(sink_connector)

        # Этот dict попадет в Airflow XCom и будет виден в UI как результат задачи.
        return {
            "source_mode": SOURCE_MODE,
            "active_source_db": ACTIVE_SOURCE,
            "source_connector": active_connector["name"],
            "sink_connector": sink_connector["name"],
            "schedule": CRON,
        }

    apply_active_connectors()


# Регистрирует DAG в Airflow при импорте файла scheduler'ом/webserver'ом.
scheduled_debezium_migration()
