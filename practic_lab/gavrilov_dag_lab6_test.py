from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
import json

default_args = {
    "owner": "airflow"
}

with DAG(
        dag_id="gavrilov_dag_lab6_Test",
        start_date=datetime(2023, 1, 15),
        description="file_sensor",
        default_args=default_args,
        schedule_interval=None,
        tags=["gavrilov", "06_practice"]
) as dag:

    sql_sensor = SqlSensor(
        task_id="file_sensor",
        conn_id = "postgres_default",
        sql='''
            SELECT 1 FROM public.lect_2
            WHERE name = 'Gavrilov' AND create_date = '2024-08-29'
        ''',
        mode='poke',
        timeout=600,
        poke_interval=60,
        retries=2,
        retry_delay=timedelta(minutes=2)
    )

    insert_table = PostgresOperator(
        task_id='postgres_example',
        postgres_conn_id='postgres_default',
        sql='''
                INSERT INTO public.lect_2
                VALUES ('Gavrilov', 'student', '2024-08-29');
                ''',
        trigger_rule="one_failed",
    )


    sql_sensor >> insert_table