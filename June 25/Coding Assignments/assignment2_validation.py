from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime
import pandas as pd
import os

base_path = os.path.dirname(__file__)
orders_file = os.path.join(base_path, "data/orders.csv")

def validate_data():
    df = pd.read_csv(orders_file)
    required_columns = ['order_id', 'customer_id', 'amount']
    for col in required_columns:
        if col not in df.columns:
            raise AirflowFailException(f"Missing required column: {col}")
        if df[col].isnull().any():
            raise AirflowFailException(f"Null values found in: {col}")

def summarize_data():
    print("Data is valid. Proceeding to summarization...")

with DAG("assignment2_validation", start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False) as dag:
    validate = PythonOperator(task_id="validate_data", python_callable=validate_data)
    summarize = PythonOperator(task_id="summarize_data", python_callable=summarize_data)

    validate >> summarize
