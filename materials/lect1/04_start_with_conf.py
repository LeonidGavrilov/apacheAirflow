from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def my_task(**kwargs):
    print("Starting my_task...")
    dag_run = kwargs.get('dag_run')
    if dag_run:
        conf = dag_run.conf
        if conf:
            print(f"Received config: {conf}")
        else:
            print("Empty config received.")
    else:
        print("Start dag without conf")
    print("Finished my_task...")


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
        dag_id='start_with_conf',
        default_args=default_args,
        schedule_interval=None,
        tags=["lect1", "start_with_conf"]
) as dag:
    task = PythonOperator(
        task_id='my_task',
        python_callable=my_task,
    )

    task
