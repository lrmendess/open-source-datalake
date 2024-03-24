from airflow.models import Variable

docker_operator_spark_kwargs = {
    'api_version': 'auto',
    'image': 'datalake-spark-image',
    'docker_url': Variable.get('docker_url'),
    'network_mode': Variable.get('network_mode'),
    'environment': {
        'BUCKET_DATALAKE_LANDING': Variable.get('bucket_datalake_landing')
    }
}
