FROM apache/airflow:2.7.3

USER root

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y zip

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow==${AIRFLOW_VERSION} \
    apache-airflow-providers-docker \
    apache-airflow-providers-sqlite \
    psycopg2-binary