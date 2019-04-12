#!/bin/bash

if [ -z "${SPARK_HOME}" ]; then
    echo "SPARK_HOME not set"
    exit 1
fi

if [ -z "${CUTTLEFISH_HOME}" ]; then
    echo "CUTTLEFISH_HOME not set"
    exit 1
fi

# generate tables
echo "Generating TPC-H tables"
cd ${CUTTLEFISH_HOME}/resources/tpch-2.18/dbgen
make
./dbgen -f -s 0.01
cp *.tbl ${CUTTLEFISH_HOME}/resources


cd ${CUTTLEFISH_HOME}
echo "Loading Tables"
${SPARK_HOME}/bin/spark-submit --class edu.purdue.cuttlefish.evaluation.tpch.LoadTables ${CUTTLEFISH_HOME}/target/cuttlefish-0.0.1-SNAPSHOT.jar

echo "Encrypting Tables"
${SPARK_HOME}/bin/spark-submit --class edu.purdue.cuttlefish.evaluation.tpch.EncryptTables ${CUTTLEFISH_HOME}/target/cuttlefish-0.0.1-SNAPSHOT.jar