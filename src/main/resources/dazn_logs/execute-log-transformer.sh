#!/bin/bash

MAIN_CLASS1=ptv.content.akamailogs.staged.parquetindexer.SparkAkamaiLogIndexer
APP_JAR=parquetindexer-1.0-TPOC-jar-with-dependencies.jar
EXTRA_JAR=spark-csv_2.10-1.5.0.jar

MASTER_URL=yarn
DEPLOY_MODE=client

# Additional for VMs
NUM_EXECUTORS=2
DRIVER_MEMORY=3g
EXECUTOR_MEMORY=2G
EXECUTOR_CORES=2

# submit from client to produce staged log akamai data
echo "Input source folder hdfs:///data/raw/akamai/dazn_logs/dt=2017-03-11/id=0/hh=0 "
DATE_STRING=2017-03-11
#hdfs dfs -mkdir -p /data/staged/akamai/spark
#hdfs dfs -rm -r -skipTrash /data/staged/akamai/spark/dazn_logs/*
#hdfs dfs -rm -r /data/staged/akamai/spark/dazn_logs

#spark-submit  --class ${MAIN_CLASS1} --master ${MASTER_URL} --deploy-mode ${DEPLOY_MODE}  --num-executors ${NUM_EXECUTORS} --driver-memory ${DRIVER_MEMORY} --executor-memory ${EXECUTOR_MEMORY} --executor-cores ${EXECUTOR_CORES} --jars ${APP_JAR} ${EXTRA_JAR} --date ${DATE_STRING} # --packages com.databricks:spark-csv_2.10:1.5.0

spark-submit  --class ${MAIN_CLASS1} --master ${MASTER_URL} --deploy-mode ${DEPLOY_MODE} --jars ${APP_JAR} ${EXTRA_JAR} --date ${DATE_STRING}
