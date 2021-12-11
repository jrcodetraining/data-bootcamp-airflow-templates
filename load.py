
from airflow import models
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator
)

PROJECT_ID = "project"
BQ_LOCATION = "us"

GCS_BUCKET = "bucket"

DATASET_NAME = "dataset"
LOCATION_DATASET_NAME = f"{DATASET_NAME}_location"

TABLE_NAME = "user_behavior_metric"

SCHEMA_FIELDS = [
  {
    "mode": "NULLABLE",
    "name": "customerid",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "amount_spent",
    "type": "NUMERIC",
    "precision": "18",
    "scale": "5"
  },
  {
    "mode": "NULLABLE",
    "name": "review_score",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "review_count",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "insert_date",
    "type": "DATE"
  }
]

DATA_SAMPLE_GCS_URL = "gs://bucket/staging-layer/user_purchase.json"

INSERT_QUERY = "INSERT `project.dataset.user_behavior_metric` (customerid, amount_spent, review_score, review_count, insert_date) WITH reviews AS ( select cid as customer_id, positive_int as positive_review from `project.dataset.reviews` ), user_purchase AS ( select customer_id, quantity, unit_price, invoice_date, invoice_number,	stock_code,	detail,	country from `project.dataset.user_purchase` ) SELECT user_purchase.customer_id as customerid, CAST(SUM(user_purchase.quantity * user_purchase.unit_price) as NUMERIC) as amount_spent, SUM(reviews.positive_review) as review_score, COUNT(reviews.customer_id) as review_count, CURRENT_DATE() as insert_date FROM user_purchase JOIN reviews ON  user_purchase.customer_id = reviews.customer_id GROUP BY user_purchase.customer_id"

with models.DAG(
    dag_id="load",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False,
) as dag:
       
    create_bq_dataset = BigQueryCreateEmptyDatasetOperator (
        task_id = "create_bq_dataset", 
        dataset_id = DATASET_NAME,
        location = BQ_LOCATION
    )

    create_bq_table = BigQueryCreateEmptyTableOperator(
        task_id = "create_bq_table",
        dataset_id = DATASET_NAME,
        table_id = TABLE_NAME,
        project_id = PROJECT_ID,
        schema_fields = SCHEMA_FIELDS
    )
   
    load_user_behavior_metrics = BigQueryExecuteQueryOperator(
        task_id = "load_user_behavior_metrics",
        sql = INSERT_QUERY,
        use_legacy_sql = False
    )

    create_bq_dataset >> create_bq_table >> load_user_behavior_metrics