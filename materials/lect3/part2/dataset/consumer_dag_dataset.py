from airflow import Dataset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

s3_dataset = Dataset("s3://airf-bds/test/test_file.txt")


# Функция для чтения данных из S3
def read_from_s3(**kwargs):
    conn_id = 'yandexS3'
    s3_hook = S3Hook(aws_conn_id=conn_id)

    # Чтение данных из S3
    bucket_name = 'airf-bds'
    folder_path = 'test/'
    file_name = 'test_file.txt'
    s3_key = f'{folder_path}{file_name}'
    file_obj = s3_hook.get_key(key=s3_key, bucket_name=bucket_name)
    content = file_obj.get()['Body'].read().decode('utf-8')

    # Запись данных в XCom
    kwargs['ti'].xcom_push(key='s3_content', value=content)


# Создание DAG
with DAG(
        'consumer_dag_example',
        schedule=[s3_dataset],  # DAG будет запускаться при обновлении s3_dataset
        start_date=datetime(2024, 6, 22),
        tags=['lect3', 'dataset']
) as dag:
    read_s3_file_task = PythonOperator(
        task_id='read_from_s3',
        python_callable=read_from_s3,
        inlets=[s3_dataset]  # Указываем, что эта задача потребляет s3_dataset
    )

    write_to_postgres_task = PostgresOperator(
        task_id='write_to_postgres',
        postgres_conn_id='postgres_default',  # Идентификатор вашего соединения с PostgreSQL в Airflow
        sql="""
            INSERT INTO lect_3_dataset (content, create_date)
            VALUES ('{{ ti.xcom_pull(task_ids='read_from_s3', key='s3_content') }}', '{{ ds }}');
        """
    )

    read_s3_file_task >> write_to_postgres_task
