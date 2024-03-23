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
- Terraform
- Airflow

## Getting started
Below we have a step-by-step guide on how to perform the services available in the project.

### Environment variables
Copy the `.env.default` file to `.env` and fill in the blank variables.
- POSTGRES_USER
- POSTGRES_PASSWORD
- MINIO_ROOT_USER
- MINIO_ROOT_PASSWORD (requires a moderate password)

### Common services
Create a network named datalake-network (only on first run).

```bash
docker network create datalake-network
```

Initialize all cluster services (This step may take a while to complete on the first run).

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
docker container exec datalake-trino-coordinator trino --execute "$(cat trino/schemas.sql)"
```

### Airflow (optional)
First you need to run the database migrations and create a user account, to do this just run the command below:

``` bash
docker compose -f docker-compose.airflow.yml up airflow-init
```

If you want to customize the Airflow project directory, simply update the `AIRFLOW_PROJ_DIR` variable in the `.env` to a directory of interest.

> The default directory is `./workspace`

Once airflow-init is finished, we can actually run Apache Airflow.

``` bash
docker compose -f docker-compose.airflow.yml up -d
```

Access the URL [https://localhost:8080](https://localhost:8080) in your browser and log in using `airflow` as username and `airflow` as password (basically the default user).

For more details, see the official [Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

### Access links to service interfaces
|Service|URL|Auth|
|---|---|---|
|Airflow|http://localhost:8080|airflow:airflow|
|Trino UI|http://localhost:8081|Any username, no password is required|
|Spark UI|http://localhost:8082|None|
|MinIO|http://localhost:9001|`${MINIO_ROOT_USER}`:`${MINIO_ROOT_PASSWORD}`|
