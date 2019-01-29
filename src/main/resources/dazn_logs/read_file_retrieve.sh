#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    FILENAME="$line"
    bash -x retrieve $FILENAME
done < "$1"
