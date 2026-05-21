import json
import os
import re
import socket
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


# Этот файл запускается контейнером connectors-init.
# Контейнер одноразовый: стартует, регистрирует Kafka Connect connectors через REST API и завершается.
# Мы вынесли регистрацию в отдельный контейнер, потому что Kafka Connect - это runtime,
# а сами connectors нужно создать HTTP-запросами после того, как debezium/connect уже поднялся.
#
# Поток данных после регистрации:
# source DB -> Debezium source connector -> Apache Kafka topic -> ClickHouse sink connector -> ClickHouse table.

# Внутренний URL Kafka Connect REST API внутри docker-compose сети.
CONNECT_URL = os.getenv("CONNECT_URL", "http://debezium:8083").rstrip("/")

# SOURCE_MODE нужен, чтобы явно различать demo и external сценарии.
SOURCE_MODE = os.getenv("SOURCE_MODE", "external").strip().lower()

# ACTIVE_SOURCE_DB выбирает один source template: postgres-source.json, mysql-source.json или mongodb-source.json.
ACTIVE_SOURCE = os.getenv("ACTIVE_SOURCE_DB", "postgres").strip().lower()

# /connectors - read-only mount из ./debezium/connectors.
CONNECTORS_DIR = Path("/connectors")

# Source connector читает изменения из исходной БД.
SOURCE_TEMPLATE = CONNECTORS_DIR / f"{ACTIVE_SOURCE}-source.json"

# Sink connector читает Kafka topic и пишет данные в ClickHouse.
SINK_TEMPLATE = CONNECTORS_DIR / "clickhouse-sink.json"

# Префикс env-переменных активного источника, например POSTGRES_SOURCE_*.
ACTIVE_PREFIX = f"{ACTIVE_SOURCE.upper()}_SOURCE"


def set_default_env(name, value):
    """Заполняет производную env var только если пользователь не задал ее явно."""
    if value and not os.getenv(name):
        os.environ[name] = value


def prepare_connector_env():
    """Готовит явные списки таблиц/topics с обратной совместимостью для одной таблицы."""
    if ACTIVE_SOURCE == "postgres":
        schema = os.getenv("POSTGRES_SOURCE_SCHEMA", "")
        table = os.getenv("POSTGRES_SOURCE_TABLE", "")
        set_default_env("POSTGRES_SOURCE_TABLE_INCLUDE_LIST", f"{schema}.{table}" if schema and table else "")
    elif ACTIVE_SOURCE == "mysql":
        database = os.getenv("MYSQL_SOURCE_DB", "")
        table = os.getenv("MYSQL_SOURCE_TABLE", "")
        set_default_env("MYSQL_SOURCE_TABLE_INCLUDE_LIST", f"{database}.{table}" if database and table else "")
    elif ACTIVE_SOURCE == "mongodb":
        database = os.getenv("MONGODB_SOURCE_DB", "")
        collection = os.getenv("MONGODB_SOURCE_COLLECTION", "")
        set_default_env("MONGODB_SOURCE_COLLECTION_INCLUDE_LIST", f"{database}.{collection}" if database and collection else "")

    source_topic = os.getenv(f"{ACTIVE_PREFIX}_TOPIC", "")
    set_default_env("CLICKHOUSE_SINK_TOPICS", source_topic)
    if source_topic and os.getenv("CLICKHOUSE_SINK_TABLE"):
        set_default_env("CLICKHOUSE_TOPIC_TABLE_MAP", f"{source_topic}={os.getenv('CLICKHOUSE_SINK_TABLE')}")


prepare_connector_env()


def required_env(name):
    """Возвращает env var для шаблона connector JSON или падает с понятной ошибкой."""
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def render_template(text):
    """Подставляет ${ENV_NAME} внутри connector JSON перед отправкой в Kafka Connect."""
    return re.sub(r"\$\{([A-Z0-9_]+)\}", lambda match: required_env(match.group(1)), text)


def read_connector(path):
    """Читает JSON-шаблон, подставляет env и возвращает готовый connector config."""
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
                # Kafka Connect может временно отклонять REST-запросы во время rebalance.
                # Вместо падения ждем и повторяем запрос.
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
        # Если connector найден, обновляем только его config.
        request(f"/connectors/{connector['name']}")
        print(f"Updating connector {connector['name']}")
        request(f"/connectors/{connector['name']}/config", method="PUT", body=connector["config"])
    except RuntimeError as error:
        if "404" not in str(error):
            raise
        # Если connector не найден, регистрируем его впервые.
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

    # На этом этапе шаблоны становятся реальными JSON payload'ами для Kafka Connect REST API.
    active_connector = read_connector(SOURCE_TEMPLATE)
    sink_connector = read_connector(SINK_TEMPLATE)

    # Одновременно активным должен быть только один source connector.
    # Иначе разные источники могут писать в разные topics и путать demo-сценарий.
    for path in list_source_templates():
        if path.name == f"{ACTIVE_SOURCE}-source.json":
            continue
        delete_connector(read_connector_name(path))

    # Source connector начинает читать исходную БД.
    # Sink connector начинает читать Kafka topic и писать строки в ClickHouse.
    upsert_connector(active_connector)
    upsert_connector(sink_connector)
    print(f"Debezium source is {ACTIVE_SOURCE}; connectors are ready")


if __name__ == "__main__":
    main()
