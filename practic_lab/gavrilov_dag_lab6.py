from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator


import os

# Аргументы DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Создание DAG
with DAG(
    dag_id='gavrilov_dag_lab6',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["gavrilov", "06_practice"]
) as dag:

    # Переменные окружения Airflow
    poke_interval = int(os.getenv('POKE_INTERVAL', 60))  # 60 секунд по умолчанию
    timeout = int(os.getenv('TIMEOUT', 600))  # 10 минут по умолчанию

    # Дата для вставки в таблицу
    date_to_insert = Variable.get("date_to_06_practice")
    current_name = "Gavrilov"

    start = EmptyOperator(task_id="start")
    # Шаг 1: Проверка наличия строки с вашим именем и сегодняшней датой
    check_row_sensor = SqlSensor(
        task_id='check_row_exists',
        conn_id='postgres_default',
        sql=f"SELECT 1 FROM public.lect_2 WHERE name = '{current_name}' AND create_date = '{{{{ ds }}}}'",
        poke_interval=poke_interval,
        timeout=timeout,
        mode='poke'
    )

    # Шаг 2: Добавление новой строки, если строки с вашим именем нет
    insert_row = SQLExecuteQueryOperator(
        task_id='insert_new_row',
        conn_id='postgres_default',
        sql=f"INSERT INTO public.lect_2 VALUES ('{current_name}', 'student', '{{{{ var.value.date_to_06_practice }}}}')",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # Шаг 3: Проверка, что строка была добавлена после вставки
    check_inserted_row_sensor = SqlSensor(
        task_id='check_inserted_row_exists',
        conn_id='postgres_default',
        sql=f"SELECT 1 FROM public.lect_2 WHERE name = '{current_name}' AND create_date = '{{{{ var.value.date_to_06_practice }}}}'",
        poke_interval=poke_interval,
        timeout=timeout
    )

    stop = EmptyOperator(task_id="stop")

    # Определение порядка выполнения задач
    start >> check_row_sensor >> insert_row >> check_inserted_row_sensor >> stop
