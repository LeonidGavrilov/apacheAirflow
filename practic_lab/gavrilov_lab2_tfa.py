from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# Определение DAG с использованием декоратора
@dag(
    dag_id="gavrilov_dag_lab2_tfa",
    start_date=datetime(2024, 8, 24),
    schedule_interval=None,
    tags=["gavrilov", "02_practice", "task_flow_api"]
)
def my_dag():
    start = EmptyOperator(task_id='start')

    @task
    def sum_numbers(numbers: list) -> int:
        total = sum(numbers)
        print(f"Сумма чисел: {total}")
        return total

    end = EmptyOperator(task_id='end')

    # Определяем последовательность задач
    start >> sum_numbers([1, 2, 3, 4, 5]) >> end

# Создаем экземпляр DAG
dag = my_dag()