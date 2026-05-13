from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.utils import (
    task_validate_source,
    task_ingest_raw,
    task_validate_raw,
    task_preprocess,
    task_store_clean,
    task_split_data,
    task_train_model,
    task_register_model,
    task_compare_and_promote,
    ensure_minio_bucket
)

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="diabetes_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["mlops", "diabetes"],
) as dag:

    validate_source = PythonOperator(
        task_id="validate_source",
        python_callable=task_validate_source,
    )

    ingest_raw = PythonOperator(
        task_id="ingest_raw",
        python_callable=task_ingest_raw,
    )

    validate_raw = PythonOperator(
        task_id="validate_raw",
        python_callable=task_validate_raw,
    )

    preprocess = PythonOperator(
        task_id="preprocess_data",
        python_callable=task_preprocess,
    )

    store_clean = PythonOperator(
        task_id="store_clean_data",
        python_callable=task_store_clean,
    )

    split_data = PythonOperator(
        task_id="split_data",
        python_callable=task_split_data,
    )

    ensure_bucket = PythonOperator(
        task_id="ensure_bucket",
        python_callable=ensure_minio_bucket,
    )

    train_model = PythonOperator(
        task_id="train_model",
        python_callable=task_train_model,
    )

    register_model = PythonOperator(
        task_id="register_model",
        python_callable=task_register_model,
    )

    promote_model = PythonOperator(
        task_id="compare_and_promote",
        python_callable=task_compare_and_promote,
    )

    (
        validate_source
        >> ingest_raw
        >> validate_raw
        >> preprocess
        >> store_clean
        >> split_data
        >> ensure_bucket
        >> train_model
        >> register_model
        >> promote_model
    )