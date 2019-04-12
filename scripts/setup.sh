#!/bin/bash

# compile
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ${SCRIPT_DIR}/..
mvn package

# copy eval keys
cp resources/eval_keys/* /tmp/
