# dags/test_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    print("Hello! Airflow is working!")
    return "Success"

with DAG(
    dag_id='test_hello_dag',
    default_args=default_args,
    description='테스트용 DAG',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:
    
    task1 = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )