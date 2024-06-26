import logging

from airflow.operators.bash import BashOperator

logger = logging.getLogger()


class ZipPyfilesOperator(BashOperator):
    def __init__(self, pip_requirements: str, name: str = 'pyfiles.zip', **kwargs):
        """ Installs pip dependencies from a requirements.txt file and creates a zip file of them.

        Args:
            pip_requirements (str): Absolute path to the requirements file.
            name (str, optional): Zip file name. Defaults to 'pyfiles.zip'.

        Returns:
            str: Zip file location on local file system.
        """
        kwargs['do_xcom_push'] = True
        kwargs['bash_command'] = f'''
            tempdir=$(mktemp -d) && \\
            mkdir -p $tempdir/pyfiles && \\
            cd $(dirname {pip_requirements}) && \\
            pip install -r $(basename {pip_requirements}) -t $tempdir/pyfiles && \\
            cd $tempdir/pyfiles && \\
            zip -r {name} * && \\
            echo $tempdir/pyfiles/{name}
        '''
        super().__init__(**kwargs)
