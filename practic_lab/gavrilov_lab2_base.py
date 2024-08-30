from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def sum_numbers(numbers: list) -> int:
    total = sum(numbers)
    print(f"Сумма чисел: {total}")
    return total

# Инициализация DAG
with DAG(
        dag_id="gavrilov_dag_lab2_base",
        start_date=datetime(2024, 8, 24),
        schedule_interval=None,
        tags=["gavrilov", "02_practice", "task_flow_api"]
) as dag:
    
    # Задачи DAG
    start = EmptyOperator(task_id="start")

    with_python = PythonOperator(
        task_id="python_task",
        python_callable=sum_numbers,
        op_args=([1, 2, 3, 4, 5],),
    )

    stop = EmptyOperator(task_id="stop")

    # Определяем последовательность задач
    start >> with_python >> stop
