import json
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta

def calculate_average():
    # Получение значений из переменной Airflow
    numbers = json.loads(Variable.get('numbers_leonid', default_var='[1, 2, 3, 4, 5]'))
    
    # Вычисление среднего значения
    average = sum(numbers) / len(numbers)
    
    # Сохранение среднего значения в новую переменную Airflow
    Variable.set('average_leonid', average)
    print(f"Среднее значение: {average}")

# Определение DAG
with DAG(
        dag_id='gavrilov_dag_cv_lab4',
        start_date=datetime(2024, 8, 24),
        schedule_interval=None,
        catchup=False,
        tags=["gavrilov", "04_practice"]
) as dag:

    calculate_average_task = PythonOperator(
        task_id='calculate_average_task',
        python_callable=calculate_average
    )

