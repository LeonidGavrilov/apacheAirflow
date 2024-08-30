from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from pprint import pprint


# Определение функции для PythonOperator
def print_context_f(**kwargs):
    pprint(kwargs)


# Параметры по умолчанию для DAG
default_args = {
    'start_date': datetime(2023, 1, 15),
    'description': "print_context"
}

# Инициализация DAG
with DAG(
        "print_context",
        default_args=default_args,
        schedule_interval="@once",
        tags=["lect1", "context"]
) as dag:
    # Задачи DAG
    start = EmptyOperator(task_id="start")

    print_context = PythonOperator(
        task_id="print_context",
        python_callable=print_context_f
    )

    stop = EmptyOperator(task_id="stop")

    # Определение последовательности выполнения задач
    start >> print_context >> stop
