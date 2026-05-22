import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Этот DAG запускает пакетную загрузку Elasticsearch -> ClickHouse по расписанию.
# Airflow не ходит в Elasticsearch сам: он вызывает elasticsearch-connector /batch.

# URL elasticsearch-connector внутри docker-compose сети.
ELASTICSEARCH_CONNECTOR_URL = os.getenv("ELASTICSEARCH_CONNECTOR_URL", "http://elasticsearch-connector:3366").rstrip("/")

# Расписание batch-загрузки Elasticsearch в cron-формате.
ELASTICSEARCH_BATCH_CRON = os.getenv("AIRFLOW_ELASTICSEARCH_BATCH_CRON", "15 * * * *")

# Позволяет создать DAG сразу выключенным, чтобы пользователь включил его вручную в Airflow UI.
DAG_PAUSED = os.getenv("AIRFLOW_DAG_PAUSED", "true").strip().lower() == "true"

# Index/index pattern, который будет передан в elasticsearch-connector /batch.
ELASTICSEARCH_INDEX_PATTERN = os.getenv("ELASTICSEARCH_INDEX_PATTERN", "logs-*")

# Размер страницы чтения из Elasticsearch.
ELASTICSEARCH_BATCH_SIZE = int(os.getenv("ELASTICSEARCH_BATCH_SIZE", "1000"))

# Если DAG запускается вручную без data interval, берём этот lookback.
ELASTICSEARCH_BATCH_LOOKBACK_HOURS = int(os.getenv("ELASTICSEARCH_BATCH_LOOKBACK_HOURS", "1"))


def iso_utc(value: datetime) -> str:
    """Форматирует datetime в ISO UTC для elasticsearch-connector /batch."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def json_request(path: str, payload: dict) -> dict:
    """Отправляет JSON-запрос в elasticsearch-connector и возвращает JSON-ответ."""
    request = urllib.request.Request(
        f"{ELASTICSEARCH_CONNECTOR_URL}{path}",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=900) as response:
            data = response.read().decode("utf-8")
            return json.loads(data) if data else {}
    except urllib.error.HTTPError as error:
        # Тело ошибки важно видеть в Airflow logs: там будут ошибки auth, index pattern или range query.
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"POST {path} failed with {error.code}: {detail}") from error


def interval_from_context() -> tuple[datetime, datetime]:
    """Берет интервал загрузки из Airflow data interval или строит fallback для ручного запуска."""
    context = get_current_context()
    start = context.get("data_interval_start")
    end = context.get("data_interval_end")
    if start and end and start != end:
        return start, end
    end = datetime.now(timezone.utc)
    return end - timedelta(hours=ELASTICSEARCH_BATCH_LOOKBACK_HOURS), end


@dag(
    dag_id="scheduled_elasticsearch_batch_to_clickhouse",
    description="Load Elasticsearch documents into ClickHouse by scheduled batch windows.",
    schedule=ELASTICSEARCH_BATCH_CRON,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=DAG_PAUSED,
    tags=["agentic-data-stack", "elasticsearch", "clickhouse", "batch"],
)
def scheduled_elasticsearch_batch_to_clickhouse():
    # DAG содержит одну задачу: вызвать /batch за текущий Airflow interval.
    @task
    def run_elasticsearch_batch() -> dict:
        """Запускает elasticsearch-connector /batch и возвращает результат в Airflow XCom."""
        start, end = interval_from_context()
        payload = {
            "index_pattern": ELASTICSEARCH_INDEX_PATTERN,
            "start": iso_utc(start),
            "end": iso_utc(end),
            "batch_size": ELASTICSEARCH_BATCH_SIZE,
        }
        result = json_request("/batch", payload)
        return {"request": payload, "result": result}

    run_elasticsearch_batch()


# Регистрирует DAG в Airflow при импорте файла scheduler'ом/webserver'ом.
scheduled_elasticsearch_batch_to_clickhouse()
