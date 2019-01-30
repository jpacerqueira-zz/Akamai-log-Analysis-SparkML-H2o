#!/usr/bin/env bash
echo "<<beeline -f>>  PASSWORDFILE EXECUTIONFILE "
PASSWORDFILE=$(pwd)/$1
EXECUTIONFILE=$(pwd)/$2
USERNAME1='oracle'
echo " |||| ----- EXECUTION PRINTED WITH CONNECTION SCTRING ----- |||| "
while IFS='' read -r line || [[ -n "$line" ]]; do
    LOCAL_PASSWORD="$line"
    LOCAL_CONNECT=$( echo " !connect jdbc:hive2://ixpbda04:10000/staged_akamai;user=${USERNAME1};password=${LOCAL_PASSWORD};principal=hive/ixpbda04.prod.ix.perform.local@BDA.PERFORMGROUP.COM ;")
    sed  -i "1i ${LOCAL_CONNECT}" ${EXECUTIONFILE}
    beeline -f $EXECUTIONFILE
    sed -i '1d' ${EXECUTIONFILE}
done < "$PASSWORDFILE"
