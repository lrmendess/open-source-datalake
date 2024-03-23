import os
import logging
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from commons.docker_operator import docker_operator_kwargs
from operators.upload_artifacts_operator import (UploadArtifactsOperator,
                                                 UploadSingleArtifactOperator)
from operators.zip_pyfiles_operator import ZipPyfilesOperator

SRC_DIR = Path(os.getenv('AIRFLOW_HOME'), 'src', 'preco_cesta_basica')

logger = logging.getLogger()


@dag(
    dag_id='dag.preco_cesta_basica',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)
def preco_cesta_basica():
    with TaskGroup('upload_artifacts') as upload_artifacts:
        pyfiles_zip_task = ZipPyfilesOperator(
            task_id='pyfiles_zip',
            pip_requirements=f'{SRC_DIR}/requirements.txt'
        )

        upload_pyfiles_task = UploadSingleArtifactOperator(
            task_id='upload_pyfiles',
            path="{{ti.xcom_pull(task_ids='upload_artifacts.pyfiles_zip')}}",
        )

        upload_src_task = UploadArtifactsOperator(
            task_id='upload_src',
            root_dir=SRC_DIR
        )

        pyfiles_zip_task >> upload_pyfiles_task >> upload_src_task

    src_path = "{{ti.xcom_pull(task_ids='upload_artifacts.upload_src')}}"

    raw_tb_preco_cesta_basica = DockerOperator(
        task_id='raw_tb_preco_cesta_basica',
        image='datalake-spark-image',
        **docker_operator_kwargs,
        command=[
            'spark-submit',
            '--name', 'raw_tb_preco_cesta_basica',
            f'{src_path}/pyspark_raw_preco_cesta_basica.py'
        ]
    )

    trusted_tb_preco_cesta_basica = DockerOperator(
        task_id='trusted_tb_preco_cesta_basica',
        image='datalake-spark-image',
        **docker_operator_kwargs,
        command=[
            'spark-submit',
            '--name', 'trusted_tb_preco_cesta_basica',
            '--py-files', "{{ti.xcom_pull(task_ids='upload_artifacts.upload_pyfiles')}}",
            f'{src_path}/pyspark_trusted_preco_cesta_basica.py'
        ]
    )

    upload_artifacts >> raw_tb_preco_cesta_basica >> trusted_tb_preco_cesta_basica


preco_cesta_basica()
