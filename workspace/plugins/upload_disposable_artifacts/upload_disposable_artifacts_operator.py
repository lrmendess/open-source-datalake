import logging
from pathlib import Path
from typing import List

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.baseoperator import BaseOperator

logger = logging.getLogger()


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