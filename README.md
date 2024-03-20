# open-source-datalake
This project consists of a learning initiative in creating a Big Data cluster using open source tools.

Feel free to study or replicate the content here.

## Prerequisites (tested)
- Docker Compose >= 2.24.7
- Terraform >= 1.17.4

## Available services
- Hive Metastore (HMS)
- MinIO
- Trino
- Apache Spark
- dbt Core
- Terraform
- Airflow

## Getting started
Below we have a step-by-step guide on how to perform the services available in the project.

### Environment variables

Copy the `.env.default` file to `.env` and fill in the blank variables (in general, they are just credentials for some services).
- POSTGRES_USER
- POSTGRES_PASSWORD
- MINIO_ROOT_USER
- MINIO_ROOT_PASSWORD (requires a moderate password)

### Common services

Create a network named datalake-network (only on first run).

```bash
docker network create datalake-network
```

Initialize all cluster services.

``` bash
docker compose up -d [--scale trino-worker=<num>] [--scale spark-worker=<num>]
```

> After the first startup, if you stop the service and want to start it again, you must prefix the variable `IS_RESUME=true` when invoking the `docker-compose up` command again.

Create MinIO Buckets (only on first run).

``` bash
terraform init
terraform plan
terraform apply -auto-approve
```

Create HMS schemas (only on first run).
``` bash
docker container exec datalake-trino-coordinator --execute "$(cat trino/schemas.sql | xargs)"
```

### Airflow (optional)
``` bash
docker compose -f docker-compose.airflow.yml up airflow-init
# Wait until airflow-init finishes running (it is only necessary on the first run)
docker compose -f docker-compose.airflow.yml up -d
```
