#!/bin/bash
# 
MYUSER="$(whoami)"
#
if [ $MYUSER = "root" ]
then
 exit 0
fi
#
/opt/cloudera/parcels/Anaconda/bin/python3.6 /opt/cloudera/parcels/Anaconda/bin/pip install awscli --upgrade --user
#
exit 0
