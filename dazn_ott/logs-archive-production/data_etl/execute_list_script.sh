#!/usr/bin/env bash
echo "<<bash -f>>  LISTFILE EXECUTIONFILE "
LISTFILE=$(pwd)/$1
EXECUTIONFILE=$(pwd)/$2
echo " |||| ----- EXECUTION PRINTED WITH PRINT COMMANDS ----- |||| "
echo "Sleeps Spark Job call for 10 minutes beween schedulling is a workarrounds" 
while IFS='' read -r line || [[ -n "$line" ]]; do
    ONEOF_FILE="$line"
    bash -x $EXECUTIONFILE $ONEOF_FILE
    sleep 540
done < "$LISTFILE"
