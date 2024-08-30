from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

uuid = '{{ macros.uuid() }}'
t = 'lect_2'

def check_table_not_empty():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    records = hook.get_records("SELECT * FROM lect_2 LIMIT 1;")
    if not records:
        raise ValueError("Table lect_2 is empty")


def print_max_create_date():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    result = hook.get_first("SELECT MAX(create_date) FROM lect_2;")
    if result:
        print(f"Max create_date: {result[0]}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'lab_07',
        default_args=default_args,
        schedule_interval='@monthly',
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 5, 1),
        catchup=True,
) as dag:
    t1 = PythonOperator(
        task_id='check_table_not_empty',
        python_callable=check_table_not_empty,
    )

    t2 = PostgresOperator(
        task_id='insert_surname',
        postgres_conn_id='postgres_default',
        sql=f"""
            INSERT INTO {t} (name, create_date) 
            VALUES ('kartashov', '{{{{ macros.ds_add(next_ds, -1) }}}}')
            """,
    )

    t3 = PythonOperator(
        task_id='print_max_create_date',
        python_callable=print_max_create_date,
    )

    t1 >> t2 >> t3
