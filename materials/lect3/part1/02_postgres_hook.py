from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


def handler_hook_postgres():
    # Создаем экземпляр PostgresHook, указывая id соединения
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')

    # Выполняем SQL запрос
    sql = "SELECT name FROM lect_2;"
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()

    # Закрываем соединение
    cursor.close()
    connection.close()

    # Преобразуем значения в верхний регистр и выводим их
    for row in result:
        uppercase_name = row[0].upper()
        print(uppercase_name)


def psql_hook():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    records = postgres_hook.get_records("SELECT name FROM lect_2;")

    for row in records:
        uppercase_name = row[0].upper()
        print(uppercase_name)


with DAG(
        "postgres_hook_example",
        start_date=datetime(2024, 6, 21),
        schedule_interval=None,
        tags=['lect3', 'postgres_hook']
) as dag:
    handler_hook_postgres_op = PythonOperator(
        task_id='handler_hook_postgres',
        python_callable=handler_hook_postgres
    )

    psql_hook_op = PythonOperator(
        task_id="psql_hook",
        python_callable=psql_hook
    )

    handler_hook_postgres_op >> psql_hook_op
