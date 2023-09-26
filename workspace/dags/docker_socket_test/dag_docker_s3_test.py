import logging
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python import PythonOperator

def upload_to_s3():
    s3_hook = S3Hook(aws_conn_id='S3_Conn')
    bucket = s3_hook.get_bucket('landing')
    for obj in bucket.objects.all():
        logging.info(obj.key)

with DAG('s3_upload_dag', start_date=datetime(2023, 9, 25)) as dag:
    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )
    
    spark_command = [
        'spark-submit',
        '--master', 'spark://datalake-spark-master:7077',
        '--deploy-mode', 'client',
        's3a://artifacts/'
    ]

    dop = DockerOperator(
        api_version='auto',
        docker_url='TCP://docker-socket-proxy:2375',
        command='echo Hello World',
        image='ubuntu',
        network_mode='bridge',
        task_id='docker_op_tester',
        dag=dag,
    )
