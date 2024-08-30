from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def check_table(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Проверяем количество записей в таблице
    count = postgres_hook.get_first("SELECT COUNT(*) FROM lect_2")[0]
    
    # Если таблица не пуста, возвращаем True, иначе False
    return count != 0

def print_max(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    result = postgres_hook.get_first("SELECT MAX(create_date) FROM lect_2")
    
    if result and result[0]:
        max_date = result[0]
        print(f"Самая большая create_date: {max_date}")
    else:
        print("Таблица lect_2 пуста.")



dag = DAG(
    dag_id="gavrilov_dag_lab7",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 5, 1),
    schedule_interval='0 0 1 * *',
    catchup=True,
    default_args={"depends_on_past": True},
    tags=["gavrilov", "07_practice"],
)

# Задача для проверки пустоты таблицы
check_table_task = PythonOperator(
    task_id='check_table',
    python_callable=check_table,
    provide_context=True,
    dag=dag,
)

# Задача для вставки записи
insert_record_task = PostgresOperator(
    task_id='insert_record',
    postgres_conn_id='postgres_default',
    sql="""
        INSERT INTO lect_2 (name, create_date) 
        VALUES ('Gavrilov', '{{ macros.ds_add(macros.ds_format(ds, "%Y-%m-01", "%Y-%m-%d"), 31)|replace(" ", "T") }}');
    """,
    trigger_rule='all_success',
    dag=dag,
)

# Задача для вывода максимальной даты
print_max_task = PythonOperator(
    task_id='print_max',
    python_callable=print_max,
    provide_context=True,
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# Логика выполнения
check_table_task >> insert_record_task >> print_max_task
