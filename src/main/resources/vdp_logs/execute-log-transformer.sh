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

# submit from client to produce staged log vdp data
DATE_STRING='2017-07-17'
IN_PATH=/data/raw/content/vdp/dummy/dt=${DATE_STRING}/*
OUT_PATH='/data/staged/content/vdp/dummy/'

echo "Input source folder ${IN_PATH} "
echo "Delete output to avoid duplicates : ${OUT_PATH}* "
hdfs dfs -rm -r -skipTrash  ${OUT_PATH}*

spark-submit  --class ${MAIN_CLASS1} --master ${MASTER_URL} --deploy-mode ${DEPLOY_MODE} --jars ${APP_JAR} ${EXTRA_JAR} --date ${DATE_STRING} \
   --inputpath ${IN_PATH} --outputpath ${OUT_PATH} # \
#  --num-executors ${NUM_EXECUTORS} --driver-memory ${DRIVER_MEMORY} \
#  --executor-memory ${EXECUTOR_MEMORY} --executor-cores ${EXECUTOR_CORES}
