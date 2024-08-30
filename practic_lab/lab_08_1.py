from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime


def create_s3_file(surname, number):
    # Параметры доступа к AWS S3 из Connection в Airflow
    conn_id = 'yandexS3'  # Идентификатор вашего соединения с S3 в Airflow
    s3_hook = S3Hook(aws_conn_id=conn_id)

    # Данные для создания файла
    bucket_name = 'airf-bds'  # Название вашего бакета
    folder_path = f"work_dir/students/{surname}/"  # Путь к существующему каталогу в бакете
    file_name = f'random_number_{surname}.txt'  # Имя файла, который нужно создать
    content = str(number)  # Содержимое файла

    # Загрузка файла в S3
    s3_hook.load_string(content, key=f'{folder_path}{file_name}', bucket_name=bucket_name, replace=True)

    print(f"File '{file_name}' successfully created in '{folder_path}' folder of bucket '{bucket_name}'")


with DAG(
        'final_1',
        schedule_interval='45 13 5 * *',
        start_date=datetime(2024, 4, 1),
        catchup=True,
        max_active_runs=1,
        tags=['final']
) as dag:
    create_s3_file_task = PythonOperator(
        task_id='create_or_update_s3_file',
        python_callable=create_s3_file,
        op_args=["{{ var.json.students['0'] }}", "{{ macros.random() }}"],
        outlets=[Dataset("""s3://airf-bds/work_dir/students/"{{ var.json.students['0'] }}"/random_number_{{ var.json.students['0'] }}.txt""")]
    )
