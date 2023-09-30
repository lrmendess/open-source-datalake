FROM trinodb/trino:426
USER root

RUN apt-get update && \
    apt-get install -y jq

COPY --chmod=0755 ./default-schemas.sh /

USER trino:trino
