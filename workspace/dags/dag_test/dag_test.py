''' Test DAG. '''
import logging
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger("airflow.task")


def fn_test():
    s3_hook = S3Hook()
    obj = s3_hook.get_key(key='ipca/ipca2023.tsv', bucket_name='datalake-landing')
    logger.info(obj.get()['Body'].read().decode())


with DAG('dag.test', start_date=datetime(2024, 2, 10), catchup=False) as dag:
    start_task = EmptyOperator(
        task_id='start_task',
        dag=dag
    )

    deploy_artifacts_task = PythonOperator(
        task_id='fn_test',
        python_callable=fn_test,
        dag=dag
    )

    end_task = EmptyOperator(
        task_id='end_task',
        dag=dag
    )

    start_task >> deploy_artifacts_task >> end_task
