#!/bin/bash

$HIVE_HOME/bin/schematool -dbType $DB_DRIVER -upgradeSchema 2> /dev/null

if [ $? -eq 0 ]; then
    IS_RESUME=true bash -c /entrypoint.sh
else
    bash -c /entrypoint.sh
fi
