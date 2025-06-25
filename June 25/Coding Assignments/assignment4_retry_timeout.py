from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

def simulate_long_work():
    time.sleep(30)


with DAG("assignment4_retry_timeout", start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False) as dag:
    task = PythonOperator(
        task_id="simulate_work",
        python_callable=simulate_long_work,
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(seconds=60)
    )
