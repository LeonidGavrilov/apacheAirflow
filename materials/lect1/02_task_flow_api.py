from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import subprocess


# Определение DAG с использованием декоратора
@dag(
    dag_id="task_flow_api_dag",
    start_date=datetime(2023, 1, 15),
    description="Привет, мир и task flow api",
    schedule_interval="@once",
    tags=["lect1", "task_flow_api"]
)
def first_task_flow_api_dag():
    @task
    def print_hello_world():
        print("Hello, world!")

    @task
    def bash_command():
        subprocess.run(['echo', 'Hello, world!'])

    @task
    def print_something_else(text):
        print(f"and something else {text}")

    # Инициализация задач с использованием Task Flow API
    op1 = print_hello_world()

    # Использование PythonOperator для вызова функции bash_command
    # op2 = PythonOperator(
    #     task_id='python_bash_command',
    #     python_callable=bash_command
    # )

    op2 = bash_command()

    # Инициализация задачи с использованием Task Flow API
    op3 = print_something_else("something else")

    # Инициализация BashOperator
    op4 = BashOperator(
        task_id='bash_operator',
        bash_command='echo "Hello, world!"'
    )

    # Определение зависимостей задач
    op1 >> [op2, op3, op4]


# Инициализация DAG
dag = first_task_flow_api_dag()
