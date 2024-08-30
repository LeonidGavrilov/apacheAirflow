from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
        dag_id="s3_file_sensor",
        start_date=datetime(2023, 1, 15),
        description="s3_file_sensor",
        default_args=default_args,
        schedule_interval=None,
        tags=["lect2", "file_sensor"]
) as dag:
    # оператор FileSensor по дефолту использует conn = fs_default

    s3_file_sensor = S3KeySensor(
        task_id="file_sensor",
        bucket_name="airf-bds",
        bucket_key="dags/materials/lect1/01_first_dag.py",
        aws_conn_id="yandexS3",
        verify=False,
        poke_interval=60,
        timeout=300,
        )

    s3_file_sensor_2 = S3KeySensor(
        task_id="file_sensor_2",
        bucket_name="airf-bds",
        bucket_key="dags/materials/lect1/test.py",
        aws_conn_id="yandexS3",
        soft_fail=True,
        verify=False,
        poke_interval=5,
        timeout=20,
    )

    s3_file_sensor >> s3_file_sensor_2
