#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    FULLFILENAME="$line"
    echo $FULLFILENAME
    EXPFILENAME=$(echo $FULLFILENAME | rev | cut -c4- | rev)
    #echo $EXPFILENAME >> newfile_list.txt
    #gunzip $FULLFILENAME
    bash -x sedfiles $EXPFILENAME 
    bash -x copyfromlocal $EXPFILENAME 
done < "$1"
