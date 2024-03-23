import re
import logging
from pathlib import Path
from typing import List, Sequence

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable

logger = logging.getLogger()


class UploadArtifactsOperator(BaseOperator):
    template_fields: Sequence[str] = ('root_dir', 'paths',)

    def __init__(self, root_dir: str = '/', paths: List[str] = None, **kwargs) -> None:
        """ Uploads the disposable artifact files to S3.

        Args:
            root (str, optional): Absolute path to the root directory. Defaults to '/'.
            paths (List[str], optional): Relative paths (glob). Defaults to None.

        Returns:
            str: Path to the root of files on S3 (XCom).
        """
        super().__init__(**kwargs)
        self.root_dir = str(root_dir)
        self.paths = paths or ['**/*']

    def validate(self):
        if len(self.root_dir) <= 0 or self.root_dir == '.' or '..' in self.root_dir:
            raise Exception(f'Non-relative pattern are not supported by "root_dir" variable.\n'
                            f'Current value: {self.root_dir}')

        if self.root_dir == '/' and '**/*' in self.paths:
            raise Exception('The path /**/* is not allowed.')

        if any(path.startswith('/') for path in self.paths):
            raise Exception('Non-relative patterns are not supported by the "paths" variable '
                            'unless the prefix is the same as "root_dir".')

    def execute(self, context):
        self.validate()

        s3_hook = S3Hook()
        files: List[Path] = []
        bucket: str = Variable.get('bucket_datalake_artifacts')
        context_id = re.sub(r'[^\d\w]', '_', context['run_id'])
        prefix = f'airflow/dag-run/{context_id}'

        logging.info('Listing files in directory "%s"', self.root_dir)

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
