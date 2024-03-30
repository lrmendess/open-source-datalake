import logging

from typing import List, Tuple

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable

logger = logging.getLogger()


class PySparkDockerOperator(DockerOperator):
    def __init__(
        self,
        name: str,
        application: str,
        options: List[Tuple[str, str]] = None,
        args: List[str] = None,
        **kwargs
    ) -> None:
        """ Submit spark job.

        Args:
            name (str): Spark session name.
            application (str): Python main file to be executed.
            options (List[Tuple[str, str]], optional): Spark submit options. Defaults to None.
            args (List[str], optional): Python main file arguments. Defaults to None.
        """
        command = ['spark-submit']
        command.extend(('--name', name))
        for option in options or []:
            command.extend(option)
        command.append(application)
        command.extend(args or [])

        kwargs['command'] = command
        kwargs['task_id'] = kwargs.get('task_id', name)
        kwargs['api_version'] = kwargs.get('api_version', 'auto')
        kwargs['image'] = kwargs.get('image', Variable.get('spark_image'))
        kwargs['docker_url'] = kwargs.get('docker_url', Variable.get('docker_url'))
        kwargs['network_mode'] = kwargs.get('network_mode', Variable.get('network_mode'))

        super().__init__(**kwargs)
