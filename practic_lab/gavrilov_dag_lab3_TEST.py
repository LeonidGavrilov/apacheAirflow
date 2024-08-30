import json
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# Параметры по умолчанию для DAG
args_gavrilov = json.loads(Variable.get('args_gavrilov', default_var='default_args_gavrilov'))
args_gavrilov['retry_delay'] = timedelta(minutes=int(args_gavrilov['retry_delay']))

# Инициализация DAG
with DAG(
        catchup=True,
        dag_id="gavrilov_dag_lab3",
        default_args=args_gavrilov,
        schedule_interval="@daily",
        tags=["gavrilov", "03_practice"]
) as dag:
    
    # Задачи DAG
    start = EmptyOperator(task_id="start")

    stop = EmptyOperator(task_id="stop")

    # Определяем последовательность задач
    start >> stop
