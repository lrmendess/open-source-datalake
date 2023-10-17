''' Execute birthday data pipeline. '''
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

def deploy_artifacts():
    ''' Upload spark artifacts to S3 '''
    filename = 'pyspark_raw_tb_ipca_hist.py'
    filepath = os.path.join(AIRFLOW_HOME, 'dags', 'birthdays', filename)
    s3_hook = S3Hook()
    s3_hook.load_file(filepath, filename, 'artifacts', replace=True)

with DAG('dag.birthdays', start_date=datetime(2023, 9, 26), catchup=False) as dag:
    deploy_artifacts_task = PythonOperator(
        task_id='deploy_artifacts',
        python_callable=deploy_artifacts
    )

    spark_submit_command = [
        'spark-submit',
        's3a://artifacts/pyspark_raw_tb_ipca_hist.py'
    ]

    spark_submit_task = DockerOperator(
        api_version='auto',
        docker_url='TCP://docker-socket-proxy:2375',
        command=spark_submit_command,
        image='datalake-spark-image',
        network_mode='datalake-network',
        task_id='spark_submit_task',
        dag=dag
    )

    deploy_artifacts_task >> spark_submit_task
    