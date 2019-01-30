#!/usr/bin/env bash
echo "<<beeline -f>>   !!__!! <<hive -f>> !!__!! "EXECUTION=
PASSWORDFILE=$1
EXECUTIONFILE=$2
beeline -n oracle -p ${PASSWORDFILE} -u jdbc:hive2://bigdatalite.localdomain:10000 -f ${EXECUTIONFILE}
