import os
import logging
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from operators.upload_artifacts_operator import UploadArtifactsOperator
from commons.docker_operator import docker_operator_kwargs

SRC_DIR = Path(os.getenv('AIRFLOW_HOME'), 'src', 'preco_cesta_basica')

logger = logging.getLogger()


@dag(
    dag_id='dag.preco_cesta_basica',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)
def preco_cesta_basica():
    with TaskGroup('build_requirements') as build_requirements:
        pip_install = BashOperator(
            task_id='pip_install',
            bash_command=f"""
                tempdir=$(mktemp -d) && \\
                mkdir -p $tempdir/pyfiles && \\
                pip install -r {SRC_DIR}/requirements.txt -t $tempdir/pyfiles && \\
                cd $tempdir/pyfiles && \\
                zip -r pyfiles.zip * && \\
                echo $tempdir/pyfiles
            """,
            do_xcom_push=True
        )

        pyfiles_root_dir = "{{ti.xcom_pull(task_ids='build_requirements.pip_install')}}"

        upload_zip = UploadArtifactsOperator(
            task_id='upload_zip',
            paths=['pyfiles.zip'],
            root_dir=pyfiles_root_dir
        )

        pip_install >> upload_zip

    upload_artifacts_task = UploadArtifactsOperator(
        task_id='upload_artifacts',
        root_dir=SRC_DIR
    )

    artifacts_path = "{{ti.xcom_pull(task_ids='upload_artifacts')}}"

    raw_tb_preco_cesta_basica = DockerOperator(
        task_id='raw_tb_preco_cesta_basica',
        image='datalake-spark-image',
        **docker_operator_kwargs,
        command=[
            'spark-submit',
            '--name', 'raw_tb_preco_cesta_basica',
            f'{artifacts_path}/pyspark_raw_preco_cesta_basica.py'
        ]
    )

    trusted_tb_preco_cesta_basica = DockerOperator(
        task_id='trusted_tb_preco_cesta_basica',
        image='datalake-spark-image',
        **docker_operator_kwargs,
        command=[
            'spark-submit',
            '--name', 'trusted_tb_preco_cesta_basica',
            '--py-files', f'{artifacts_path}/pyfiles.zip',
            f'{artifacts_path}/pyspark_trusted_preco_cesta_basica.py'
        ]
    )

    build_requirements >> upload_artifacts_task >> raw_tb_preco_cesta_basica
    raw_tb_preco_cesta_basica >> trusted_tb_preco_cesta_basica


preco_cesta_basica()
