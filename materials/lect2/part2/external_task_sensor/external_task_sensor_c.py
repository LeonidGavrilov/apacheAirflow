from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2024, 6, 21, 21, 0),
    "end_date": datetime(2024, 6, 23, 22, 0),
    "schedule_interval": "@daily"
}

with DAG(
    dag_id="DAG_C",
    default_args=default_args,
    description="Пример DAG с BashOperator, выводящим значение {{ ds }}",
    tags=["external_task_sensor", "lect2"]
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command="echo {{ ds }}"
    )

    bash_task
