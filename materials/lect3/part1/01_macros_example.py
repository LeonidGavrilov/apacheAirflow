from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
import random
import string


# Определение пользовательского макроса
def generate_random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


# Функция PythonOperator
def my_func(ds, **kwargs):
    print(f'Execution date: {ds}')
    print(f"Yesterday's date: {kwargs['yesterday_ds']}")
    print(f"Random string: {kwargs['templates_dict']['random_string']}")


# Регистрация пользовательских макросов
custom_macros = {
    'generate_random_string': generate_random_string
}

with DAG(
        "macros_example",
        start_date=datetime(2024, 6, 21),
        end_date=datetime(2024, 6, 23),
        schedule_interval='0 9 * * *',
        default_args={"depends_on_past": True},
        user_defined_macros=custom_macros,
        tags=['lect3', 'macros_example']
) as dag:
    # BashOperator, использующий макрос ds
    op1 = BashOperator(
        task_id="ds",
        bash_command='echo {{ ds }}'
    )

    # PythonOperator, использующий макросы
    python_task = PythonOperator(
        task_id='my_task',
        python_callable=my_func,
        op_kwargs={'ds': '{{ ds }}'},
        provide_context=True,
        templates_dict={
            'random_string': '{{ generate_random_string() }}'
        },
    )

    # BashOperator, использующий встроенный макрос random
    op2 = BashOperator(
        task_id="random",
        bash_command="echo random = '{{ macros.random() }}'"
    )

    # BashOperator, использующий макрос ds_add и next_ds
    op3 = BashOperator(
        task_id="ds_example",
        bash_command="echo ds = {{ ds }}, ds-1 = {{ macros.ds_add(ds, -1) }}, next_ds = {{ next_ds }}"
    )

    # BashOperator, демонстрирующий работу с пользовательским макросом
    op4 = BashOperator(
        task_id="custom_macro",
        bash_command="echo random_string = '{{ generate_random_string(12) }}'"
    )

    # BashOperator, использующий макросы для работы с датами и временем
    op5 = BashOperator(
        task_id="date_manipulation",
        bash_command="echo start_date = {{ ds }}, end_date = {{ next_ds }}, execution_time = {{ execution_date }}, "
                     "start_of_week = {{ macros.ds_format(ds, '%Y-%m-%d', '%A') }}"
    )

    [op1, op3] >> python_task >> op2 >> op4 >> op5
