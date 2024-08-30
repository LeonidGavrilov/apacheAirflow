from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "start_date": datetime(2024, 6, 21, 22, 0),
    "end_date": datetime(2024, 6, 23, 22, 0),
    "schedule_interval": "@daily"
}

with DAG(
    dag_id="DAG_B",
    default_args=default_args,
    description="DAG B с использованием ExternalTaskSensor для ожидания задач из DAG A и DAG C",
    tags=["external_task_sensor", "lect2"]
) as dag:
    sensor_a = ExternalTaskSensor(
        task_id="sensor_a",
        external_dag_id="DAG_A",
        external_task_id="bash_task",
        poke_interval=10
    )

    sensor_c = ExternalTaskSensor(
        task_id="sensor_c",
        external_dag_id="DAG_C",
        external_task_id="bash_task",
        execution_delta=timedelta(hours=1),  # Ожидать выполнения задачи с задержкой в 1 час
        poke_interval=10
    )

    empty = EmptyOperator(task_id="empty")

    # Определение зависимостей между задачами
    [sensor_a, sensor_c] >> empty
