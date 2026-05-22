import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Этот DAG запускает пакетную загрузку Prometheus -> ClickHouse по расписанию.
# Airflow не читает Prometheus сам: он вызывает HTTP endpoint prometheus-connector /backfill.

# URL prometheus-connector внутри docker-compose сети.
PROMETHEUS_CONNECTOR_URL = os.getenv("PROMETHEUS_CONNECTOR_URL", "http://prometheus-connector:3355").rstrip("/")

# Расписание batch-загрузки Prometheus в cron-формате.
PROMETHEUS_BATCH_CRON = os.getenv("AIRFLOW_PROMETHEUS_BATCH_CRON", "0 * * * *")

# Позволяет создать DAG сразу выключенным, чтобы пользователь включил его вручную в Airflow UI.
DAG_PAUSED = os.getenv("AIRFLOW_DAG_PAUSED", "true").strip().lower() == "true"

# PromQL-запросы через запятую. Каждый запрос уйдет в Prometheus query_range.
PROMETHEUS_BATCH_QUERIES = [
    item.strip()
    for item in os.getenv("PROMETHEUS_BATCH_QUERIES", os.getenv("PROMETHEUS_BACKFILL_QUERY", "up")).split(",")
    if item.strip()
]

# Шаг query_range: одна точка в 60 секунд по умолчанию.
PROMETHEUS_BATCH_STEP = os.getenv("PROMETHEUS_BATCH_STEP", os.getenv("PROMETHEUS_BACKFILL_STEP", "60s"))

# Если DAG запускается вручную без data interval, берём этот lookback.
PROMETHEUS_BATCH_LOOKBACK_HOURS = int(os.getenv("PROMETHEUS_BATCH_LOOKBACK_HOURS", "1"))


def iso_utc(value: datetime) -> str:
    """Форматирует datetime в ISO UTC для prometheus-connector /backfill."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def json_request(path: str, payload: dict) -> dict:
    """Отправляет JSON-запрос в prometheus-connector и возвращает JSON-ответ."""
    request = urllib.request.Request(
        f"{PROMETHEUS_CONNECTOR_URL}{path}",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=600) as response:
            data = response.read().decode("utf-8")
            return json.loads(data) if data else {}
    except urllib.error.HTTPError as error:
        # Тело ошибки важно видеть в Airflow logs, иначе отладка Prometheus API будет слепой.
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
    return end - timedelta(hours=PROMETHEUS_BATCH_LOOKBACK_HOURS), end


@dag(
    dag_id="scheduled_prometheus_batch_to_clickhouse",
    description="Load Prometheus metrics into ClickHouse by scheduled query_range backfill.",
    schedule=PROMETHEUS_BATCH_CRON,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=DAG_PAUSED,
    tags=["agentic-data-stack", "prometheus", "clickhouse", "batch"],
)
def scheduled_prometheus_batch_to_clickhouse():
    # DAG содержит одну задачу: вызвать /backfill за текущий Airflow interval.
    @task
    def run_prometheus_backfill() -> dict:
        """Запускает prometheus-connector /backfill и возвращает результат в Airflow XCom."""
        start, end = interval_from_context()
        payload = {
            "queries": PROMETHEUS_BATCH_QUERIES,
            "start": iso_utc(start),
            "end": iso_utc(end),
            "step": PROMETHEUS_BATCH_STEP,
        }
        result = json_request("/backfill", payload)
        return {"request": payload, "result": result}

    run_prometheus_backfill()


# Регистрирует DAG в Airflow при импорте файла scheduler'ом/webserver'ом.
scheduled_prometheus_batch_to_clickhouse()
