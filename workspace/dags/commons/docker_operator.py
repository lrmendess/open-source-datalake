from airflow.models import Variable

docker_operator_kwargs = {
    'api_version': 'auto',
    'docker_url': Variable.get('docker_url'),
    'network_mode': Variable.get('network_mode'),
    'environment': {
        'BUCKET_DATALAKE_LANDING': Variable.get('bucket_datalake_landing')
    }
}
