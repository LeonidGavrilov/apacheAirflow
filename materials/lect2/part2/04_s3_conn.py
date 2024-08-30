from airflow import DAG
from airflow.models import Connection
from airflow.operators.python import PythonOperator
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


# # Функция для удаления соединения с S3
# def delete_s3_connection():
#     conn_id = 'my_s3_conn'
#
#     # Удаление соединения из базы данных Airflow
#     session = settings.Session()
#     conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
#     if conn:
#         session.delete(conn)
#         session.commit()
#         print(f"Connection {conn_id} deleted successfully.")
#     else:
#         print(f"Connection {conn_id} not found.")
#
#     session.close()


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
        dag_id='s3_create_connection_dag',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["lect2", "connection"]
) as dag:
    # Создание соединения S3
    create_s3_connection_task = PythonOperator(
        task_id='create_s3_connection',
        python_callable=create_s3_connection
    )

    # Определение последовательности выполнения задач
    create_s3_connection_task
