from airflow import models
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator
)

PROJECT_ID = "project"
REGION = "us-central1"
CLUSTER_NAME = "cluster"
PYSPARK_URI = f"gs://bucket/scripts/transform_reviews.py"

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

with models.DAG(
    dag_id="transform",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False,
) as dag:
    
    pyspark_job = DataprocSubmitJobOperator(
        task_id="pyspark_job", job=PYSPARK_JOB, region=REGION, project_id=PROJECT_ID
    )
            
    pyspark_job