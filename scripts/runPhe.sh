#!/bin/bash

if [ -z "${SPARK_HOME}" ]; then
    echo "SPARK_HOME not set"
    exit 1
fi

if [ -z "${CUTTLEFISH_HOME}" ]; then
    echo "CUTTLEFISH_HOME not set"
    exit 1
fi

${SPARK_HOME}/bin/spark-submit --class edu.purdue.cuttlefish.evaluation.tpch.Query ${CUTTLEFISH_HOME}/target/cuttlefish-0.0.1-SNAPSHOT.jar phe


