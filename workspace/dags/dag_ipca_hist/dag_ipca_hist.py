''' Execute birthday data pipeline. '''
import logging
from datetime import datetime
from pathlib import Path
from typing import List

from airflow.decorators import dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models.baseoperator import BaseOperator

logger = logging.getLogger()

DAG_DIR = Path(__file__).parent.absolute().as_posix()

BUCKET_ARTIFACTS = 'datalake-artifacts'
BUCKET_LANDING = 'datalake-landing'
BUCKET_RAW = 'datalake-raw'
BUCKET_TRUSTED = 'datalake-trusted'
BUCKET_REFINED = 'datalake-refined'

docker_operator_kwargs = {
    'api_version': 'auto',
    'docker_url': 'TCP://docker-socket-proxy:2375',
    'network_mode': 'datalake-network',
    'environment': {
        'BUCKET_LANDING': BUCKET_LANDING,
        'BUCKET_RAW': BUCKET_RAW,
        'BUCKET_TRUSTED': BUCKET_TRUSTED,
        'BUCKET_REFINED': BUCKET_REFINED
    }
}


class UploadDisposableArtifactsOperator(BaseOperator):
    def __init__(self, root: str, paths: List[str] = None, **kwargs) -> None:
        """ Upload artifact files to $BUCKET_ARTIFACTS.

        Args:
            root (str, optional): Root directory. Defaults to None.
            paths (List[str]): List of files or paths (using glob strategy).

        Returns:
            str: Path to the root of files on S3.
        """
        super().__init__(**kwargs)
        self.root = root
        self.paths = paths or ["**/*"]

    def execute(self, context):
        s3_hook = S3Hook()
        files: List[Path] = []
        prefix = f"airflow/dag-run/{context['run_id']}"

        for path in self.paths:
            nodes = Path(self.root).glob(path)
            file_nodes = [f for f in nodes if f.is_file()]
            files.extend(file_nodes)

        for file in files:
            relative_path = file.relative_to(self.root)
            key = '/'.join((prefix, relative_path.as_posix()))
            s3_hook.load_file(file, key, BUCKET_ARTIFACTS, replace=True)
            logger.info('File %s was loaded into S3', relative_path)

        return f's3a://{BUCKET_ARTIFACTS}/{prefix}'


@dag(
    dag_id='dag.ipca_hist',
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def ipca_hist():
    ''' Historical inflation in Brazil between 2020 and 2023. '''
    upload_artifacts_task = UploadDisposableArtifactsOperator(task_id='upload_artifacts', root=DAG_DIR, paths=['*.py'])

    spark_submit_command = [
        'spark-submit',
        '--name', 'spark.raw.tb_ipca_hist',
        '--conf', 'spark.cores.max=1',
        '--conf', 'spark.executor.cores=1',
        '--conf', 'spark.executor.memory=1g',
        's3a://{BUCKET_ARTIFACTS}/pyspark_raw_ipca_hist.py'
    ]

    spark_submit_task = DockerOperator(
        task_id='spark_submit',
        image='datalake-spark-image',
        command=spark_submit_command,
        **docker_operator_kwargs
    )

    upload_artifacts_task >> spark_submit_task


ipca_hist()
