#!/bin/bash

attempts_cnt=1
max_attempts=10

while [ $attempts_cnt -le $max_attempts ]; do
    sleep 5

    echo "$(date -Iseconds -u) :: [$attempts_cnt] Waiting for the Trino service to start"
    
    info=$(curl -sf http://localhost:8080/v1/info | jq .starting)
    echo "$(date -Iseconds -u) :: [$attempts_cnt] Trino response (starting?): $info"

    if [[ "$info" == false ]]; then
        echo "$(date -Iseconds -u) :: [$attempts_cnt] Trino service is available"
        break
    fi

    attempts_cnt=$((attempts_cnt + 1))
done

if [ $attempts_cnt -gt $max_attempts ]; then
    echo "$(date -Iseconds -u) :: [$attempts_cnt] An error occurred while creating the default schemas via the Trino service"
    exit 0
fi

trino_exit_code=0
echo "$(date -Iseconds -u) :: Creating default schemas..."

trino --execute "
    CREATE SCHEMA IF NOT EXISTS hive.raw WITH (location='s3a://raw/');
    CREATE SCHEMA IF NOT EXISTS hive.trusted WITH (location='s3a://trusted/');
    CREATE SCHEMA IF NOT EXISTS hive.refined WITH (location='s3a://refined/');
"

if [ $? -eq 0 ]; then
    echo "$(date -Iseconds -u) :: Default schemas are created successfully!"
else
    echo "$(date -Iseconds -u) :: Something went wrong when creating the default schemas!"
fi
