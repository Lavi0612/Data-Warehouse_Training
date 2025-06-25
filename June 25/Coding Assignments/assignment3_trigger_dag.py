from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

# Parent DAG
def parent_task():
    print("Parent task executed.")

with DAG("assignment3_parent_dag", start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False) as parent_dag:
    task = PythonOperator(task_id="run_parent_task", python_callable=parent_task)

    trigger = TriggerDagRunOperator(
        task_id="trigger_child_dag",
        trigger_dag_id="assignment3_child_dag",
        conf={"run_date": str(datetime.now())}
    )

    task >> trigger

# Child DAG
def child_task(**context):
    conf = context['dag_run'].conf
    print("Received from parent:", conf)

with DAG("assignment3_child_dag", start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False) as child_dag:
    read = PythonOperator(task_id="read_metadata", python_callable=child_task, provide_context=True)
