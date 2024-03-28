import logging
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset

from commons.workspace_utils import get_jobs_dir
from commons.docker_operator_utils import docker_operator_spark_kwargs
from commons.task_group_utils import task_group_upload_spark_artifacts

JOB_DIR = get_jobs_dir('preco_cesta_basica')

logger = logging.getLogger()


@dag(
    dag_id='dag.preco_cesta_basica',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)
def preco_cesta_basica():
    artifacts_task, src, pyfiles = task_group_upload_spark_artifacts(
        group_id='upload_artifacts',
        job_dir=JOB_DIR
    )

    raw_task = DockerOperator(
        **docker_operator_spark_kwargs,
        task_id='raw_tb_preco_cesta_basica',
        command=[
            'spark-submit',
            '--name', 'raw_tb_preco_cesta_basica',
            f'{src}/pyspark_raw_preco_cesta_basica.py'
        ]
    )

    trusted_task = DockerOperator(
        **docker_operator_spark_kwargs,
        task_id='trusted_tb_preco_cesta_basica',
        command=[
            'spark-submit',
            '--name', 'trusted_tb_preco_cesta_basica',
            '--py-files', pyfiles,
            f'{src}/pyspark_trusted_preco_cesta_basica.py'
        ]
    )

    end_task = EmptyOperator(
        task_id='end_task',
        outlets=[Dataset('dag.preco_cesta_basica')]
    )

    artifacts_task >> raw_task >> trusted_task >> end_task


preco_cesta_basica()
