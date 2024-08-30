from airflow import DAG
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow import settings


# Функция для создания соединения с S3
def create_s3_connection():
    conn_id = 'my_s3_conn'
    conn_type = 's3'
    extra = {
        "region_name": "US",
        "endpoint_url": "https://storage.yandexcloud.net",
        'aws_access_key_id': 'YCAJE5tP8F8uquPgaFWIDdEZv',
        'aws_secret_access_key': 'YCPzANBXNtINcYWygA1mxOaebgTBRdDQLBicDfYt'
    }

    # Создание объекта Connection
    new_conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        extra=extra
    )

    # Сохранение соединения в базе данных Airflow
    session = settings.Session()
    session.add(new_conn)
    session.commit()
    session.close()

    print(f"Connection {conn_id} created successfully.")


# Функция для удаления соединения с S3
def delete_s3_connection():
    conn_id = 'my_s3_conn'

    # Удаление соединения из базы данных Airflow
    session = settings.Session()
    conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if conn:
        session.delete(conn)
        session.commit()
        print(f"Connection {conn_id} deleted successfully.")
    else:
        print(f"Connection {conn_id} not found.")

    session.close()


# Функция для создания файла в S3
def create_s3_file():
    # Параметры доступа к AWS S3 из Connection в Airflow
    conn_id = 'my_s3_conn'  # Идентификатор вашего соединения с S3 в Airflow
    s3_hook = S3Hook(aws_conn_id=conn_id)

    # Данные для создания файла
    bucket_name = 'airf-bds'  # Название вашего бакета
    folder_path = 'test/'  # Путь к существующему каталогу в бакете
    file_name = 'test_file.txt'  # Имя файла, который нужно создать
    content = 'Hello, S3!'  # Содержимое файла

    # Загрузка файла в S3
    s3_hook.load_string(content, key=f'{folder_path}{file_name}', bucket_name=bucket_name, replace=True)

    print(f"File '{file_name}' successfully created in '{folder_path}' folder of bucket '{bucket_name}'")


# Определение параметров DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Инициализация DAG
with DAG(
        dag_id='s3_create_file_and_connection_dag',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["lect3", "s3"]
) as dag:
    # Создание соединения S3
    create_s3_connection_task = PythonOperator(
        task_id='create_s3_connection',
        python_callable=create_s3_connection
    )

    # Создание PythonOperator для выполнения функции создания файла
    create_file_task = PythonOperator(
        task_id='create_s3_file_task',
        python_callable=create_s3_file,
    )

    # # Оператор для удаления файла из S3
    # delete_s3_file_task = S3DeleteObjectsOperator(
    #     task_id='delete_s3_file',
    #     bucket='airf-bds',  # Название вашего бакета
    #     keys=['test/test_file.txt'],  # Путь к файлу в S3
    #     aws_conn_id='my_s3_conn'  # Ваш коннекшн
    # )

    # Удаление соединения S3
    delete_s3_connection_task = PythonOperator(
        task_id='delete_s3_connection',
        python_callable=delete_s3_connection
    )

    # Определение последовательности выполнения задач
    create_s3_connection_task >> create_file_task >> delete_s3_connection_task
