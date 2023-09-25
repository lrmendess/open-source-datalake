FROM python:3.11.5-slim-bookworm

WORKDIR /usr/app

RUN apt update && \
    apt install -y git --fix-missing --no-install-recommends && \
    apt clean

RUN pip install --upgrade pip && \
    pip install dbt-trino==1.6.1

COPY ./profiles.yml /root/.dbt/profiles.yml

ENTRYPOINT [ "dbt" ]
