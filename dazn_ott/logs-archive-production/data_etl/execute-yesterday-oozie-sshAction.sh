#!/usr/bin/env bash
JOB_PWD=/home/oracle/projects/dazn_ott/logs-archive-production/data_etl
#JOB_PWD=$(pwd)
YES_DAT=$(date --date=' 1 days ago' '+%y%d%m')
mkdir -p $JOB_PWD/list_files
rm $JOB_PWD/list_files/yesterday.txt
touch $JOB_PWD/list_files/yesterday.txt
echo "$YES_DAT" >> $JOB_PWD/list_files/yesterday.txt
cat  $JOB_PWD/list_files/yesterday.txt
bash -x $JOB_PWD/execute_list_script.sh $JOB_PWD/list_file/yesterday.txt $JOB_PWD/workArroundDI-transform_rawjson2parquet-ixpbda09
echo 1
