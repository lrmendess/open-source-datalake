import logging
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.datasets import Dataset

from commons.workspace_utils import get_jobs_dir
from commons.docker_operator_utils import docker_operator_spark_kwargs
from commons.task_group_utils import task_group_upload_spark_artifacts

JOB_DIR = get_jobs_dir('poder_compra')

logger = logging.getLogger()


@dag(
    dag_id='dag.poder_compra',
    start_date=datetime(2024, 1, 1),
    schedule=[Dataset('dag.salario_minimo'), Dataset('dag.preco_cesta_basica')],
    catchup=False
)
def poder_compra():
    artifacts_task, src, pyfiles = task_group_upload_spark_artifacts(
        group_id='upload_artifacts',
        job_dir=JOB_DIR
    )

    refined_task = DockerOperator(
        **docker_operator_spark_kwargs,
        task_id='refined_tb_poder_compra',
        command=[
            'spark-submit',
            '--name', 'refined_tb_poder_compra',
            f'{src}/pyspark_refined_poder_compra.py'
        ]
    )

    artifacts_task >> refined_task


poder_compra()
