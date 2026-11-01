from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "deh-hof",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def extract():
    print("Extracting sample data...")
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    print(f"Extracted: {data}")
    return data

def transform():
    print("Transforming sample data...")
    transformed = [{"id": 1, "name": "ALICE"}, {"id": 2, "name": "BOB"}]
    print(f"Transformed: {transformed}")
    return transformed

def load():
    print("Loading sample data...")
    print("Data loaded successfully.")

with DAG(
    dag_id="sample_etl_dag",
    description="Sample ETL DAG for testing",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["sample", "etl", "test"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_task >> transform_task >> load_task
