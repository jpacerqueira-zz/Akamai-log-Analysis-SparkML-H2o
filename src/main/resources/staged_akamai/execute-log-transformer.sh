#!/bin/bash

MAIN_CLASS1=ptv.content.akamailogs.staged.parquetindexer.SparkAkamaiLogIndexer
APP_JAR=parquetindexer-1.0-TPOC-jar-with-dependencies.jar
EXTRA_JAR=spark-csv_2.10-1.5.0.jar

MASTER_URL=yarn
DEPLOY_MODE=client
NUM_EXECUTORS=2
DRIVER_MEMORY=3g
EXECUTOR_MEMORY=2G
EXECUTOR_CORES=2

# submit from client to produce staged log akamai data
echo "Input source folder hdfs:///data/raw/akamai/dazn_logs/dt=0/id=0/hh=0 "
DATE_STRING=2017-03-12
IN_PATH=data/raw/akamai/dazn_logs/dt=${DATE_STRING}/id=*/hh=*/*
OUT_PATH=data/staged/akamai/spark/dazn_logs/
echo "Clean  hdfs:///user/joao.cerqueira/data/raw/akamai/dazn_logs/dt=${DATE_STRING}/id=0/hh=10,11"
hdfs dfs -rm -r -skipTrash  data/staged/akamai/spark/dazn_logs/dt=2017-03-12/hh=11/*
hdfs dfs -rm -r -skipTrash  data/staged/akamai/spark/dazn_logs/dt=2017-03-12/hh=10/*

#hdfs dfs -mkdir -p /data/staged/akamai
#hdfs dfs -rm -r -skipTrash /data/staged/akamai/dazn_logs/*
#hdfs dfs -rm -r /data/staged/akamai/dazn_logs

#spark-submit  --class ${MAIN_CLASS1} --master ${MASTER_URL} --deploy-mode ${DEPLOY_MODE}  --num-executors ${NUM_EXECUTORS} \
# --driver-memory ${DRIVER_MEMORY} --executor-memory ${EXECUTOR_MEMORY} --executor-cores ${EXECUTOR_CORES} --jars ${APP_JAR} \
# ${EXTRA_JAR} --dthr ${DATE_STRING} # --packages com.databricks:spark-csv_2.10:1.5.0

spark-submit  --class ${MAIN_CLASS1} --master ${MASTER_URL} --deploy-mode ${DEPLOY_MODE} --jars ${APP_JAR} ${EXTRA_JAR} --date ${DATE_STRING} \
   --inputpath ${IN_PATH} --outputpath ${OUT_PATH} # \
# --packages com.databricks:spark-csv_2.10:1.5.0  --num-executors ${NUM_EXECUTORS} --driver-memory ${DRIVER_MEMORY}
# --executor-memory ${EXECUTOR_MEMORY} --executor-cores ${EXECUTOR_CORES}

