#!/usr/bin/env bash
echo "<<bash -f>>  LISTFILE EXECUTIONFILE "

if [ -z "$PATH_JOB_DIR" ]; then
    PATH_JOB_DIR=/home/oracle/projects/massive_elb_logs_archive_production/data/raw/ott_dazn/docomo_investigations/logs-archive-production/data_loading
fi

LISTFILE=$PATH_JOB_DIR/$1
EXECUTIONFILE=$PATH_JOB_DIR/$2
echo " |||| ----- EXECUTION PRINTED WITH PRINT COMMANDS ----- |||| "
while IFS='' read -r line || [[ -n "$line" ]]; do
    ONEOF_FILE="$line"
    bash -x $EXECUTIONFILE $ONEOF_FILE 
    sleep 5
done < "$LISTFILE"
echo 1
