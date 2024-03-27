import os
from pathlib import Path


def get_jobs_dir(job_name: str = None) -> Path:
    """ Retrieve jobs dir path or specific job path.

    Args:
        job_name (str, optional): Job name. Defaults to None.

    Returns:
        Path: Jobs dir path or specific job path.
    """
    jobs_dir = Path(os.getenv('AIRFLOW_HOME'), 'jobs')
    return jobs_dir.joinpath(job_name) if job_name else jobs_dir
