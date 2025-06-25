from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

def get_api_key(conn_id):
    conn = BaseHook.get_connection(conn_id)
    return conn.extra_dejson.get("apikey")

with DAG(
    dag_id="assignment7_api_call",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    api_key = get_api_key("public_api") 

    call_api = SimpleHttpOperator(
        task_id="call_weather_api",
        http_conn_id="public_api",
        endpoint="data/2.5/weather?q=London&appid=07822ea58dfe23dd0007a355311462ce",
        method="GET",
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )
