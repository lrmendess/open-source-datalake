import os
import logging
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator

from commons.docker_operator_utils import docker_operator_spark_kwargs
from commons.task_group_utils import task_group_upload_spark_artifacts

SRC_DIR = Path(os.getenv('AIRFLOW_HOME'), 'src', 'salario_minimo')

logger = logging.getLogger()


@dag(
    dag_id='dag.salario_minimo',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)
def salario_minimo():
    artifacts_task, src, pyfiles = task_group_upload_spark_artifacts(
        group_id='upload_artifacts',
        src_path=SRC_DIR
    )

    raw_task = DockerOperator(
        **docker_operator_spark_kwargs,
        task_id='raw_tb_salario_minimo',
        command=[
            'spark-submit',
            '--name', 'raw_tb_salario_minimo',
            f'{src}/pyspark_raw_salario_minimo.py'
        ]
    )

    trusted_task = DockerOperator(
        **docker_operator_spark_kwargs,
        task_id='trusted_tb_salario_minimo',
        command=[
            'spark-submit',
            '--name', 'trusted_tb_salario_minimo',
            '--py-files', pyfiles,
            f'{src}/pyspark_trusted_salario_minimo.py'
        ]
    )

    artifacts_task >> raw_task >> trusted_task


salario_minimo()
