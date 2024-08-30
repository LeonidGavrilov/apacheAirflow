import random
import string

from airflow import Dataset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

s3_dataset = Dataset("s3://airf-bds/test/test_file.txt")


def create_or_update_s3_file():
    # Параметры доступа к AWS S3 из Connection в Airflow
    conn_id = 'yandexS3'  # Идентификатор вашего соединения с S3 в Airflow
    s3_hook = S3Hook(aws_conn_id=conn_id)

    # Данные для создания файла
    bucket_name = 'airf-bds'  # Название вашего бакета
    folder_path = 'test/'  # Путь к существующему каталогу в бакете
    file_name = 'test_file.txt'  # Имя файла, который нужно создать
    random_string = ''.join(random.sample(string.ascii_letters + string.digits, k=16))
    content = f'some string - {random_string}'  # содержимое файла

    # Загрузка файла в S3
    s3_hook.load_string(content, key=f'{folder_path}{file_name}', bucket_name=bucket_name, replace=True)

    print(f"File '{file_name}' successfully created in '{folder_path}' folder of bucket '{bucket_name}'")


# Создание DAG
with DAG(
        'producer_dag_example',
        schedule_interval='@daily',
        start_date=datetime(2024, 8, 20),
        end_date=datetime(2024, 8, 25),
        # catchup=False, (как поведет себя даг в таком случае?)
        tags=['lect3', 'dataset']
) as dag:
    create_s3_file_task = PythonOperator(
        task_id='create_or_update_s3_file',
        python_callable=create_or_update_s3_file,
        outlets=[s3_dataset]  # Указываем, что эта задача производит s3_dataset
    )

    create_s3_file_task
