from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow"
}

with DAG(
        dag_id="gavrilov_dag_lab5_sensor",
        start_date=datetime(2023, 1, 15),
        description="file_sensor",
        default_args=default_args,
        schedule_interval=None,
        tags=["gavrilov", "05_practice"]
) as dag:
    
    insert_table = PostgresOperator(
        task_id='postgres_example',
        postgres_conn_id='postgres_default',
        sql='''
        INSERT INTO public.lect_2 VALUES
            ('Gavrilov', 'student', '9999-99-99');
        '''
    )

    sql_sensor = SqlSensor(
        task_id="file_sensor",
        conn_id = "postgres_default",
        sql='''
            SELECT 1 FROM public.lect_2
            WHERE name = 'Gavrilov'
        ''',
        mode='poke',
        timeout=600,
        poke_interval=60,
        retries=2,
        retry_delay=timedelta(minutes=2)
        )


    insert_table >> sql_sensor
