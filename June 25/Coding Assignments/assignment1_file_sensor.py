from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import shutil
import os

base_path = os.path.dirname(__file__)
incoming_file = os.path.join(base_path, "C:\Users\Lavi\airflow\dags\Coding Assignments\data\incoming\report.csv")
archive_file_path = os.path.join(base_path, "data/archive/report.csv")

def process_file():
    with open(incoming_file, "r") as f:
        print(f.read())

def archive_file():
    shutil.move(incoming_file, archive_file_path)

with DAG("assignment1_file_sensor", start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False) as dag:
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=incoming_file,
        poke_interval=30,
        timeout=600,
        mode='poke',
        soft_fail=True
    )

    process = PythonOperator(task_id="process_file", python_callable=process_file)
    archive = PythonOperator(task_id="archive_file", python_callable=archive_file)

    wait_for_file >> process >> archive
