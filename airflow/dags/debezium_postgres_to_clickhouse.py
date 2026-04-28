from __future__ import annotations

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

DEBEZIUM_URL = "http://debezium:8083"
CONNECTOR_NAME = "postgres-app-events-source"
CONNECTOR_CONFIG_PATH = "/opt/airflow/dags/../../connectors/postgres-source.json"


def register_or_restart_connector() -> None:
    with open(CONNECTOR_CONFIG_PATH, "r", encoding="utf-8") as file:
        payload = file.read()

    response = requests.get(f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}", timeout=20)

    if response.status_code == 404:
        create_response = requests.post(
            f"{DEBEZIUM_URL}/connectors",
            data=payload,
            headers={"Content-Type": "application/json"},
            timeout=20,
        )
        create_response.raise_for_status()
        return

    response.raise_for_status()

    restart_response = requests.post(
        f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}/restart?includeTasks=true&onlyFailed=false",
        timeout=20,
    )
    if restart_response.status_code not in (200, 202, 204):
        restart_response.raise_for_status()


def trigger_incremental_snapshot() -> None:
    payload = {
        "type": "execute-snapshot",
        "data": {
            "data-collections": ["public.app_events"],
            "type": "INCREMENTAL",
        },
    }
    response = requests.post(
        f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}/signal",
        json=payload,
        timeout=20,
    )
    if response.status_code not in (200, 202, 204):
        response.raise_for_status()


with DAG(
    dag_id="debezium_postgres_to_clickhouse",
    description="Register/restart Debezium and trigger scheduled snapshots for app logs.",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(hours=1),
    catchup=False,
    max_active_runs=1,
    tags=["debezium", "postgres", "clickhouse"],
) as dag:
    register_connector = PythonOperator(
        task_id="register_or_restart_connector",
        python_callable=register_or_restart_connector,
    )

    snapshot = PythonOperator(
        task_id="trigger_incremental_snapshot",
        python_callable=trigger_incremental_snapshot,
    )

    register_connector >> snapshot
