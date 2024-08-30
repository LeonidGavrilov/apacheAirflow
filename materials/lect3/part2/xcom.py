import string
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from random import randint
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# Функция для создания или обновления файла в S3
def create_or_update_s3_file():
    conn_id = 'yandexS3'
    s3_hook = S3Hook(aws_conn_id=conn_id)

    bucket_name = 'airf-bds'
    folder_path = 'test/'
    file_name = 'test_xcom.txt'
    content = 'xcom example\ncreate_date: {{ ds }}'  # содержимое файла

    s3_hook.load_string(content, key=f'{folder_path}{file_name}', bucket_name=bucket_name, replace=True)
    print(f"File '{file_name}' successfully created in '{folder_path}' folder of bucket '{bucket_name}'")


# Функция для чтения содержимого файла из S3 и передачи его в XCom
def read_from_s3(**kwargs):
    conn_id = 'yandexS3'
    s3_hook = S3Hook(aws_conn_id=conn_id)

    bucket_name = 'airf-bds'
    folder_path = 'test/'
    file_name = 'test_xcom.txt'
    s3_key = f'{folder_path}{file_name}'
    file_obj = s3_hook.get_key(key=s3_key, bucket_name=bucket_name)
    content = file_obj.get()['Body'].read().decode('utf-8')

    kwargs['ti'].xcom_push(key='s3_content', value=content)


# Функция для генерации случайных чисел и отправки их в XCom
def push_nums(ti):
    push_nums_list = [randint(0, 100), randint(0, 100)]
    ti.xcom_push(key='push_nums', value=push_nums_list)


# Функция для извлечения чисел из XCom и выполнения операций над ними
def pull_nums(ti):
    pull_nums_list = ti.xcom_pull(key='push_nums', task_ids=['test_task_1', 'test_task_2', 'test_task_3'])
    total_sum = sum(sum(sublist) for sublist in pull_nums_list)
    print(f"Total sum of numbers pushed: {total_sum}")


# Создание DAG
with DAG(
        'xcom_example',
        start_date=days_ago(1),
        schedule_interval=None,  # Запуск вручную
        tags=['xcom', 'lect3']  # Теги для идентификации DAG
) as dag:
    dag.doc_md = """
    # Пример использования XCom в Apache Airflow

    Этот DAG демонстрирует использование XCom для передачи данных между задачами,
    а также динамическую генерацию задач (Dynamic Task).
    """

    # Задача для создания или обновления файла в S3
    create_file = PythonOperator(
        task_id='create_file',
        python_callable=create_or_update_s3_file
    )

    # Задача для чтения файла и отправки содержимого в XCom
    read_file = PythonOperator(
        task_id='read_file',
        python_callable=read_from_s3
    )

    # Динамическая генерация задач для отправки случайных чисел в XCom
    push_nums_tasks = [
        PythonOperator(
            task_id=f'test_task_{task}',
            python_callable=push_nums,
        ) for task in ['1', '2', '3']
    ]

    # Задача для извлечения чисел из XCom и выполнения операций над ними
    pull_nums_op = PythonOperator(
        task_id='pull_nums',
        python_callable=pull_nums
    )

    # Пример BashOperator для выполнения bash-команды с использованием Jinja-шаблонов
    bash_push = BashOperator(
        task_id='bash_push',
        bash_command='echo "{{ ds }}"',  # Выводит текущую дату выполнения
    )

    # Пример BashOperator для демонстрации XCom push из bash-команды
    bash_push_with_key = BashOperator(
        task_id='bash_push_with_key',
        bash_command='echo "{{ ti.xcom_push(key=\'my_key\', value=\'my_val\') }}"',
        # do_xcom_push=False  # Если False, то не произойдет автоматического пуша
    )

# Определение порядка выполнения задач
create_file >> read_file >> push_nums_tasks >> pull_nums_op >> bash_push >> bash_push_with_key
