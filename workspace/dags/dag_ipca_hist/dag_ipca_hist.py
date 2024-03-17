import logging
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable

from operators.upload_disposable_artifacts_operator import UploadDisposableArtifactsOperator

DAG_DIR = Path(__file__).parent.absolute().as_posix()

logger = logging.getLogger()

docker_operator_kwargs = {
    'api_version': 'auto',
    'docker_url': 'TCP://docker-socket-proxy:2375',
    'network_mode': 'datalake-network',
    'environment': {
        'BUCKET_ARTIFACTS': Variable.get('bucket_artifacts'),
        'BUCKET_LANDING': Variable.get('bucket_landing'),
        'BUCKET_RAW': Variable.get('bucket_raw'),
        'BUCKET_TRUSTED': Variable.get('bucket_trusted'),
        'BUCKET_REFINED': Variable.get('bucket_refined'),
    }
}


@dag(
    dag_id='dag.ipca_hist',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)
def ipca_hist():
    upload_artifacts_task = UploadDisposableArtifactsOperator(
        task_id='upload_artifacts',
        root_dir=DAG_DIR,
        paths=[
            'pyspark_raw_ipca_hist.py'
        ]
    )

    s3_path: str = "{{ ti.xcom_pull(task_ids='upload_artifacts') }}"

    spark_submit_command = [
        'spark-submit',
        '--name', 'spark.raw.tb_ipca_hist',
        '--conf', 'spark.cores.max=1',
        '--conf', 'spark.executor.cores=1',
        '--conf', 'spark.executor.memory=1g',
        f'{s3_path}/pyspark_raw_ipca_hist.py'
    ]

    spark_submit_task = DockerOperator(
        task_id='spark_submit',
        image='datalake-spark-image',
        command=spark_submit_command,
        **docker_operator_kwargs
    )

    upload_artifacts_task >> spark_submit_task


ipca_hist()
