#!/bin/bash

DOCKER_IMAGE=$(uuidgen):test
PROJECT_DIR=$(realpath -s $(dirname $0))

rmi() {
    docker image rm $DOCKER_IMAGE
    exit ${exit_code:-1}
}

trap rmi EXIT

docker build --tag $DOCKER_IMAGE $PROJECT_DIR
docker run -t --rm $DOCKER_IMAGE python -m pytest tests $@

exit_code=$?