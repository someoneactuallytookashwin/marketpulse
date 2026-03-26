"""
Starter Airflow DAG (placeholder).

Create production DAGs once you confirm schedule/triggering and which ingestion tasks run.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

# Airflow operators will depend on how you ingest/load data (PythonOperator, BashOperator, etc.)
# For now, this file is only a structural placeholder.


default_args = {
    "owner": "marketpulse",
}


with DAG(
    dag_id="marketpulse_placeholder_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["marketpulse", "placeholder"],
) as dag:
    # TODO: define ingestion tasks and dbt execution tasks.
    pass

