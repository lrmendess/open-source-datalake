import logging

from airflow.operators.bash import BashOperator

logger = logging.getLogger()


class ZipPyfilesOperator(BashOperator):
    template_fields = ('pip_requirements',)

    def __init__(self, pip_requirements: str, **kwargs):
        self.pip_requirements = pip_requirements
        command = f"""
            tempdir=$(mktemp -d) && \\
            mkdir -p $tempdir/pyfiles && \\
            pip install -r {pip_requirements} -t $tempdir/pyfiles && \\
            cd $tempdir/pyfiles && \\
            zip -r pyfiles.zip * && \\
            echo $tempdir/pyfiles/pyfiles.zip
        """
        kwargs['bash_command'] = command
        kwargs['do_xcom_push'] = True
        super().__init__(**kwargs)
