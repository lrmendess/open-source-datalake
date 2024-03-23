import os
import subprocess
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
    zip_dependencies = BashOperator(
        task_id='pip_install',
        bash_command=f"""
            tempdir=$(mktemp -d) && \\
            mkdir -p $tempdir/pyfiles && \\
            mkdir -p {SRC_DIR}/pyfiles && \\
            pip install -r {SRC_DIR}/requirements.txt -t $tempdir/pyfiles && \\
            cd $tempdir/pyfiles && \\
            zip -r pyfiles.zip * && \\
            cp pyfiles.zip {SRC_DIR}
        """
    )

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
            f'{artifacts_path}/pyspark_trusted_preco_cesta_basica.py'
        ]
    )

    zip_dependencies >> upload_artifacts_task >> raw_tb_preco_cesta_basica >> trusted_tb_preco_cesta_basica


preco_cesta_basica()
