from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime


# Функция для задания PythonOperator
def print_name(name):
    print(f"My name is {name}")


# Определение параметров DAG
default_args = {
    'start_date': datetime(2023, 1, 15),
    'description': "Привет, мир",
}

# Инициализация DAG
with DAG(
        dag_id="first_dag",
        default_args=default_args,
        schedule_interval="@once",
        tags=["lect1", "first_dag"]
) as dag:
    # Задачи DAG
    start = EmptyOperator(task_id="start")

    with_bash = BashOperator(
        task_id="bash_task",
        bash_command="echo Hello, world",
    )

    pwd_cmd = BashOperator(
        task_id="pwd_cmd",
        bash_command='pwd'
    )

    with_python = PythonOperator(
        task_id="python_task",
        python_callable=print_name,
        op_args=["Andrei"]
    )

    stop = EmptyOperator(task_id="stop")

    # Определение последовательности выполнения задач
    start >> [with_bash, with_python] >> stop
