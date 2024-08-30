from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='postgres_example',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=['lect2', 'postgres_example']
) as dag:
    create_table = PostgresOperator(
        task_id='postgres_example',
        postgres_conn_id='postgres_default',
        sql='''
        CREATE TABLE IF NOT EXISTS public.lect_2 (
            name VARCHAR,
            type VARCHAR,
            create_date VARCHAR
        );
        '''
    )

    create_table
