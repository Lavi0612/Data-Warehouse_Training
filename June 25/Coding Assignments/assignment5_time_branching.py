from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import datetime as dt

def choose_task():
    now = dt.datetime.now()
    if now.weekday() >= 5:
        return "skip_dag"
    elif now.hour < 12:
        return "task_a"
    else:
        return "task_b"

with DAG("assignment5_time_branching", start_date=datetime(2024, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    branch = BranchPythonOperator(task_id="check_time", python_callable=choose_task)

    task_a = EmptyOperator(task_id="task_a")
    task_b = EmptyOperator(task_id="task_b")
    skip_dag = EmptyOperator(task_id="skip_dag")
    cleanup = EmptyOperator(task_id="cleanup")

    branch >> [task_a, task_b, skip_dag]
    [task_a, task_b, skip_dag] >> cleanup
