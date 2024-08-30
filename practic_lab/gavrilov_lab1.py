from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime


# Функция для задания PythonOperator
def sum_number(number):
    print(f"Sum = {number + 10}")


# Определение параметров DAG
default_args = {
    'start_date': datetime(2024, 8, 24),
    'description': "Привет, мир",
}

# Инициализация DAG
with DAG(
        dag_id="gavrilov_dag_lab1",
        default_args=default_args,
        schedule_interval="@once",
        tags=["gavrilov", "01_practice"]
) as dag:
    # Задачи DAG
    start = EmptyOperator(task_id="start")

    with_bash = BashOperator(
        task_id="bash_task",
        bash_command="echo Leonid",
    )

    with_python = PythonOperator(
        task_id="python_task",
        python_callable=sum_number,
        op_args=[10],
        # op_kwargs={"number":10}
    )

    stop = EmptyOperator(task_id="stop")

    # Определение последовательности выполнения задач
    start >> [with_bash, with_python] >> stop
