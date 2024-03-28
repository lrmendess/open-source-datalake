import os
from typing import Tuple

from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup

from operators.upload_artifacts_operator import (UploadArtifactsOperator,
                                                 UploadSingleArtifactOperator)
from operators.zip_pyfiles_operator import ZipPyfilesOperator


def task_group_upload_spark_artifacts(group_id: str, job_dir: str) -> Tuple[TaskGroup, str, str]:
    """ Default Task Group for uploading spark files and their dependencies.

    Returns:
        Tuple[TaskGroup, str, str]: Task Group reference, pyspark src path and pyfiles zip path.
    """
    with TaskGroup(group_id) as group:
        tasks = []
        requirements = f'{job_dir}/requirements.txt'

        if os.path.exists(requirements):
            pyfiles_zip_task = ZipPyfilesOperator(
                task_id='pyfiles_zip',
                pip_requirements=requirements
            )

            tasks.append(pyfiles_zip_task)

            upload_pyfiles_task = UploadSingleArtifactOperator(
                task_id='upload_pyfiles',
                path=f"{{{{ti.xcom_pull(task_ids='{group_id}.pyfiles_zip')}}}}",
            )

            tasks.append(upload_pyfiles_task)

        upload_src_task = UploadArtifactsOperator(
            task_id='upload_src',
            root_dir=f'{job_dir}/src'
        )

        tasks.append(upload_src_task)

        result = (
            group,
            f"{{{{ti.xcom_pull(task_ids='{group_id}.upload_src')}}}}",
            f"{{{{ti.xcom_pull(task_ids='{group_id}.upload_pyfiles')}}}}"
        )

        chain(*tasks)
        return result
