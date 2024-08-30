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
    'start_date': datetime(2024, 6, 15),
    'end_date': datetime(2024, 6, 20),
    'description': "print_context_backfill"
}

# Инициализация DAG
with DAG(
        "catchup",
        default_args=default_args,
        schedule_interval="@daily",
        max_active_runs=1,
        tags=["lect2", "backfill"]
) as dag:
    dag.doc_md = """
            #### Проект:
            voice-antifraud
            #### Ссылка на задачу:
            https://btask.beeline.ru/browse/VAFDE-28
            #### Описание DAG:
            Запись статистики event_io в vafs_dds.all_events_agg
            #### Входные данные:
            события по event-io в S3
            #### Выходные данные:
            vafs_dds.all_events_agg
        """
    # Задачи DAG
    start = EmptyOperator(task_id="start")

    print_context = PythonOperator(
        task_id="print_context_backfill",
        python_callable=print_context_f
    )

    stop = EmptyOperator(task_id="stop")

    # Определение последовательности выполнения задач
    start >> print_context >> stop
