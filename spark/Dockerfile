# Variable substitution over spark-defaults.conf

FROM alpine:3.18.3 AS alpine-envsubst

ARG SPARK_MASTER_URL
ARG HIVE_METASTORE_URI
ARG S3_ENDPOINT
ARG S3_ACCESS_KEY
ARG S3_SECRET_KEY

COPY ./spark-defaults.conf /tmp/spark-defaults.conf.template

RUN apk add gettext && \
    export SPARK_MASTER_URL=${SPARK_MASTER_URL} && \
    export HIVE_METASTORE_URI=${HIVE_METASTORE_URI} && \
    export S3_ENDPOINT=${S3_ENDPOINT} && \
    export S3_ACCESS_KEY=${S3_ACCESS_KEY} && \
    export S3_SECRET_KEY=${S3_SECRET_KEY} && \
    envsubst < /tmp/spark-defaults.conf.template > /tmp/spark-defaults.conf

# Get the Hive jar files

FROM apache/hive:3.1.3 AS hive-jars

# Preparing spark image

FROM bitnami/spark:3.5.0 AS spark
USER root

RUN touch ${SPARK_HOME}/conf/hadoop-metrics2-s3a-file-system.properties

COPY --from=alpine-envsubst /tmp/spark-defaults.conf ${SPARK_HOME}/conf/
COPY --from=hive-jars /opt/hive/lib/* /opt/hive/lib/

USER 1001