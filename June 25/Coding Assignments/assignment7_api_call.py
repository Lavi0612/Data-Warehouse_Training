from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime

with DAG("assignment7_api_call", start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False) as dag:
    call_api = SimpleHttpOperator(
        task_id="call_weather_api",
        http_conn_id="public_api",
        endpoint="data/2.5/weather?q=London&appid=YOUR_API_KEY",
        method="GET",
        response_check=lambda response: response.status_code == 200,
        log_response=True
    )
