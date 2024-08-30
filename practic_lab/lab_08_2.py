from airflow import Dataset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def read_from_s3(surname, **kwargs):
    conn_id = 'yandexS3'
    s3_hook = S3Hook(aws_conn_id=conn_id)

    # Чтение данных из S3
    bucket_name = 'airf-bds'  # Название вашего бакета
    folder_path = f"work_dir/students/{surname}/"  # Путь к существующему каталогу в бакете
    file_name = f'random_number_{surname}.txt'  # Имя файла, который нужно создать
    s3_key = f'{folder_path}{file_name}'
    file_obj = s3_hook.get_key(key=s3_key, bucket_name=bucket_name)
    content = file_obj.get()['Body'].read().decode('utf-8')

    # Запись данных в XCom
    kwargs['ti'].xcom_push(key='bank_account_kartashov', value=content)


with DAG(
        'final_2',
        schedule=[Dataset("""s3://airf-bds/work_dir/students/"{{ var.json.students['0'] }}"/random_number_{{ var.json.students['0'] }}.txt""")],
        start_date=datetime(2024, 4, 1),
        catchup=True,
        tags=['final']
) as dag:
    postgres_sensor_students = SqlSensor(
        task_id='check_final_practice',
        conn_id='postgres_default',  # Идентификатор вашего соединения с PostgreSQL в Airflow
        sql="""
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'final_practice'
        )
        ;
        """,
        timeout=300,  # Время ожидания выполнения запроса
        poke_interval=10,  # Интервал проверки
    )

    push_data = PythonOperator(
        task_id="push_data",
        python_callable=read_from_s3,
        op_args=["{{ var.json.students['0'] }}"],
        inlets=[Dataset("""s3://airf-bds/work_dir/students/"{{ var.json.students['0'] }}"/random_number_{{ var.json.students['0'] }}.txt""")]
    )

    postgres_op = PostgresOperator(
        task_id="extract_data",
        sql="""
        INSERT INTO final_practice values (
            0,
            '{{ var.json.students.get("0") }}',
            '{{ ti.xcom_pull('push_data', key='bank_account_kartashov') }}',
            '{{ macros.ds_add(ds, -7) }}'
        )
        """
    )

    postgres_sensor_students >> push_data >> postgres_op

