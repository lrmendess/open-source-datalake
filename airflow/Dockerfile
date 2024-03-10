FROM apache/airflow:2.7.3

RUN pip install --no-cache-dir \
    apache-airflow==${AIRFLOW_VERSION} \
    apache-airflow-providers-docker \
    apache-airflow-providers-sqlite \
    psycopg2-binary