import os
import logging
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator

from operators.upload_artifacts_operator import UploadArtifactsOperator
from commons.docker_operator import docker_operator_kwargs

SRC_DIR = Path(os.getenv('AIRFLOW_HOME'), 'src', 'ipca_hist')

logger = logging.getLogger()


@dag(
    dag_id='dag.ipca_hist',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)
def ipca_hist():
    upload_artifacts_task = UploadArtifactsOperator(
        task_id='upload_artifacts',
        root_dir=SRC_DIR
    )

    artifacts_path = "{{ti.xcom_pull(task_ids='upload_artifacts')}}"

    raw_tb_ipca_hist_task = DockerOperator(
        task_id='raw_tb_ipca_hist',
        image='datalake-spark-image',
        **docker_operator_kwargs,
        command=[
            'spark-submit',
            '--name', 'raw_tb_ipca_hist',
            f'{artifacts_path}/pyspark_raw_ipca_hist.py'
        ]
    )

    trusted_tb_ipca_hist_task = DockerOperator(
        task_id='trusted_tb_ipca_hist',
        image='datalake-spark-image',
        **docker_operator_kwargs,
        command=[
            'spark-submit',
            '--name', 'trusted_tb_ipca_hist',
            f'{artifacts_path}/pyspark_trusted_ipca_hist.py'
        ]
    )

    upload_artifacts_task >> raw_tb_ipca_hist_task >> trusted_tb_ipca_hist_task


ipca_hist()
