import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator


with DAG(
    dag_id="extract",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False,
) as dag:
   
    create_users_schema = PostgresOperator(
        postgres_conn_id="postgres_default",
        task_id="create_users_schema",
        sql="""
            CREATE SCHEMA IF NOT EXISTS users;
          """,
    )

    create_user_purchase_table = PostgresOperator(
        postgres_conn_id="postgres_default",
        task_id="create_user_purchase_table",
        sql="""
            CREATE TABLE IF NOT EXISTS users.user_purchase (
                invoice_number varchar(10),
                stock_code varchar(20),
                detail varchar(1000),
                quantity int,
                invoice_date timestamp,
                unit_price numeric(8,3),
                customer_id int,
                country varchar(20)          
            );
          """,
    )

    def file_path(relative_path):
        dir = os.path.dirname(os.path.abspath(__file__))
        split_path = relative_path.split("/")
        new_path = os.path.join(dir, *split_path)
        return new_path

    def csv_to_postgres():
        get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default',schema='dbusers').get_conn()
        cursor = get_postgres_conn.cursor()

        with open(file_path("user_purchase.csv"), "rb") as this_file:
            cursor.copy_expert("COPY users.user_purchase FROM STDIN WITH CSV HEADER", this_file)
            get_postgres_conn.commit()


    extract_user_purchase_data = PythonOperator (
        task_id = 'extract_user_purchase_data',
        provide_context = True,
        python_callable = csv_to_postgres,
    )

    extract_purchases_data_to_staging_layer = PostgresToGCSOperator (
        task_id = "extract_purchases_data_to_staging_layer",
        postgres_conn_id = "postgres_default",
        sql = "SELECT invoice_number, stock_code, detail, quantity, invoice_date, unit_price, customer_id, country FROM users.user_purchase;", 
        bucket = "bucket", 
        filename = "staging-layer/user_purchase.json", 
        gzip = False
    )
    
    create_users_schema >> create_user_purchase_table >> extract_user_purchase_data >> extract_purchases_data_to_staging_layer
