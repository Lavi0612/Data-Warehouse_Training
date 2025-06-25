from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime

def task_func():
    print("Running task")

with DAG("assignment6_email_alerts", start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False) as dag:
    task1 = PythonOperator(task_id="task1", python_callable=task_func)
    task2 = PythonOperator(task_id="task2", python_callable=task_func)

    send_success_email = EmailOperator(
        task_id="send_success_email",
        to="{{ var.value.alert_email }}",
        subject="Airflow DAG Success",
        html_content="All tasks completed successfully!"
    )

    task1 >> task2 >> send_success_email
