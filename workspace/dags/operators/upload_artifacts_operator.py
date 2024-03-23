import logging
from pathlib import Path
from typing import List, Sequence

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable

logger = logging.getLogger()


class UploadArtifactsOperator(BaseOperator):
    template_fields: Sequence[str] = ('root_dir',)

    def __init__(self, root_dir: str, paths: List[str] = None, **kwargs) -> None:
        """ Uploads the disposable artifact files to S3.

        Args:
            root (str, optional): Root directory. Defaults to None.
            paths (List[str]): List of paths (using glob strategy).

        Returns:
            str: Path to the root of files on S3 (XCom).
        """
        super().__init__(**kwargs)
        self.root_dir = root_dir
        self.paths = paths or ["**/*"]

    def format_run_id(self, run_id: str):
        replaces = [':', '-', '+', '.']
        for r in replaces:
            run_id = run_id.replace(r, '_')
        return run_id

    def execute(self, context):
        s3_hook = S3Hook()
        files: List[Path] = []
        prefix = f"airflow/dag-run/{self.format_run_id(context['run_id'])}"
        bucket = Variable.get('bucket_datalake_artifacts')

        logging.info('Listing files in directory %s', self.root_dir)

        for path in self.paths:
            nodes = Path(self.root_dir).glob(path)
            file_nodes = [f for f in nodes if f.is_file()]
            files.extend(file_nodes)

        for file in set(files):
            relative_path = file.relative_to(self.root_dir)
            key = '/'.join((prefix, relative_path.as_posix()))
            s3_hook.load_file(file, key, bucket, replace=True)
            logger.info('File %s was loaded into S3', relative_path)

        return f's3a://{bucket}/{prefix}'
