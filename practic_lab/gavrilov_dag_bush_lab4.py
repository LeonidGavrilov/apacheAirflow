import json
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


# Определение DAG
with DAG(
        dag_id='gavrilov_dag_bush_lab4',
        start_date=datetime(2024, 8, 24),
        schedule_interval=None,
        catchup=False,
        tags=["gavrilov", "04_practice"]
) as dag:

    print_average_task = BashOperator(
        task_id='print_average_task',
        bash_command='echo "Среднее значение: $(airflow variables get average_leonid)"'
    )
