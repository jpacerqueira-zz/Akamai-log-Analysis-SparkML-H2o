#!/usr/bin/env bash
#
#  Used only once to create table in Apache Hive with Spark2-shell default Parquet encoding
#
#

# Additional for Spark container 

SCRIPT_LOCAL_FILE="$1"

MASTER_URL=yarn
DEPLOY_MODE=client

NUM_EXECUTORS=20
DRIVER_MEMORY=2g
EXECUTOR_MEMORY=1g
EXECUTOR_CORES=2


echo "$(<${SCRIPT_LOCAL_FILE})" | spark2-shell  --master ${MASTER_URL} --deploy-mode ${DEPLOY_MODE} --num-executors ${NUM_EXECUTORS}  --driver-memory ${DRIVER_MEMORY} --executor-memory ${EXECUTOR_MEMORY} --executor-cores ${EXECUTOR_CORES}  2>output1.log& 

#
# 
