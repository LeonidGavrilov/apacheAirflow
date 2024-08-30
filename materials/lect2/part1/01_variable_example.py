from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

# Параметры по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2)
}

# Инициализация DAG
with DAG(
    'variable_example',
    default_args=default_args,
    schedule_interval=None,
    tags=['lect2', 'variable']
) as dag:

    # Установка значения переменной
    Variable.set('my_variable2', 'qwerty2')

    # Инициализация переменных внутри контекста DAG
    my_var = Variable.get('my_variable', default_var='default_value1')
    my_var2 = Variable.get('my_variable2', default_var='default_value2')

    # Определение задач DAG
    print_var = BashOperator(
        task_id='print_var',
        bash_command=f'echo {my_var}, {my_var2}'
    )

    print_var
