from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3)
}

# Определение DAG
with DAG(
        dag_id="trigger_rules_example",
        start_date=datetime(2023, 1, 15),
        description="Demonstration of trigger rules",
        default_args=default_args,
        schedule_interval=None,
        tags=["lect2"]
) as dag:
    # FileSensor для проверки файла
    file_sensor = FileSensor(
        task_id="file_sensor",
        filepath="/path/to/your/file.txt",  # Укажи реальный путь к файлу здесь
        poke_interval=10,
        timeout=300
    )

    # BashOperator, который всегда выполнится
    always_execute = BashOperator(
        task_id="always_execute",
        bash_command="echo Always Execute",
        trigger_rule="all_done"
    )

    # BashOperator, который выполнится только в случае успешного завершения file_sensor
    success_only = BashOperator(
        task_id="success_only",
        bash_command="echo Success Only",
        trigger_rule="all_success"
    )

    # BashOperator, который выполнится только в случае неудачного завершения file_sensor
    failure_only = BashOperator(
        task_id="failure_only",
        bash_command="echo Failure Only",
        trigger_rule="all_failed"
    )

    # BashOperator, который выполнится, если любой из предыдущих операторов завершится с ошибкой
    one_failed = BashOperator(
        task_id="one_failed",
        bash_command="echo One Failed",
        trigger_rule="one_failed"
    )

    # BashOperator, который выполнится, если все предыдущие операторы завершатся успешно или будут пропущены
    none_failed = BashOperator(
        task_id="none_failed",
        bash_command="echo None Failed",
        trigger_rule="none_failed"
    )

    # Определение зависимостей задач
    file_sensor >> [always_execute, success_only, failure_only] >> one_failed >> none_failed