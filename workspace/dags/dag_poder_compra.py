import os
import logging
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from operators import (PySparkDockerOperator, UploadArtifactsOperator,
                       UploadSingleArtifactOperator, ZipPyfilesOperator)

PROJECT = Path(os.getenv('AIRFLOW_HOME'), 'project')

logger = logging.getLogger()


@dag(
    dag_id='dag.poder_compra',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)
def poder_compra():
    with TaskGroup('upload_artifacts') as upload_artifacts_task:
        pyfiles_zip_task = ZipPyfilesOperator(
            task_id='pyfiles_zip',
            pip_requirements=PROJECT.joinpath('requirements.txt').as_posix()
        )

        upload_pyfiles_task = UploadSingleArtifactOperator(
            task_id='upload_pyfiles',
            path="{{ti.xcom_pull(task_ids='upload_artifacts.pyfiles_zip')}}",
        )

        upload_src_task = UploadArtifactsOperator(
            task_id='upload_src',
            root_dir=PROJECT.joinpath('src').as_posix(),
            paths=['salario_minimo/*', 'preco_cesta_basica/*', 'poder_compra/*']
        )

        pyfiles_zip_task >> upload_pyfiles_task >> upload_src_task

    s3_src_prefix = "{{ti.xcom_pull(task_ids='upload_artifacts.upload_src')}}"
    s3_pyfiles_01 = "{{ti.xcom_pull(task_ids='upload_artifacts.upload_pyfiles')}}"

    with TaskGroup('salario_minimo') as salario_minimo_task:
        raw_task_salario_minimo = PySparkDockerOperator(
            name='raw_tb_salario_minimo',
            application=f'{s3_src_prefix}/salario_minimo/pyspark_raw_salario_minimo.py',
            environment={
                'BUCKET_DATALAKE_LANDING': Variable.get('bucket_datalake_landing')
            }
        )

        trusted_task_salario_minimo = PySparkDockerOperator(
            name='trusted_tb_salario_minimo',
            application=f'{s3_src_prefix}/salario_minimo/pyspark_trusted_salario_minimo.py',
            options=[('--py-files', s3_pyfiles_01)]
        )

        raw_task_salario_minimo >> trusted_task_salario_minimo

    with TaskGroup('preco_cesta_basica') as preco_cesta_basica_task:
        raw_task_preco_cesta_basica = PySparkDockerOperator(
            name='raw_tb_preco_cesta_basica',
            application=f'{s3_src_prefix}/preco_cesta_basica/pyspark_raw_preco_cesta_basica.py',
            environment={
                'BUCKET_DATALAKE_LANDING': Variable.get('bucket_datalake_landing')
            }
        )

        trusted_task_preco_cesta_basica = PySparkDockerOperator(
            name='trusted_tb_preco_cesta_basica',
            application=f'{s3_src_prefix}/preco_cesta_basica/pyspark_trusted_preco_cesta_basica.py',
            options=[('--py-files', s3_pyfiles_01)]
        )

        raw_task_preco_cesta_basica >> trusted_task_preco_cesta_basica

    task_poder_compra = PySparkDockerOperator(
        name='refined_tb_poder_compra',
        application=f'{s3_src_prefix}/poder_compra/pyspark_refined_poder_compra.py'
    )

    upload_artifacts_task >> [salario_minimo_task, preco_cesta_basica_task] >> task_poder_compra


poder_compra()
