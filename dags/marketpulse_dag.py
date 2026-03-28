from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ashwin',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='marketpulse_pipeline',
    default_args=default_args,
    description='Daily ingestion of stock prices and macro indicators',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['marketpulse']
) as dag:

    ingest_stocks = BashOperator(
        task_id='ingest_stock_prices',
        bash_command='python /opt/airflow/ingestion/yfinance_ingest.py'
    )

    ingest_macro = BashOperator(
        task_id='ingest_macro_indicators',
        bash_command='python /opt/airflow/ingestion/fred_ingest.py'
    )

    ingest_stocks >> ingest_macro