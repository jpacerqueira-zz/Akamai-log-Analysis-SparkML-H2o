#!/bin/bash
#
##
### MODEL: x ngrams75
#
DATE_V1=$1
if [ -z "$DATE_V1" ]
then
 DATE_V1=20190101
fi
#
echo $DATE_V1
# Make sure security Kerberos is cached
#
if [ -f ~/.keytabs/$(whoami).keytab ]; then
  # for any:any credentials
  export KRB5CCNAME=/tmp/krb5cc_$(id -u)

  kinit -kt ~/.keytabs/$(whoami).keytab $(whoami)/chpbda@BDA2.PERFORMGROUP.COM -c /tmp/krb5cc_$(id -u)
  klist /tmp/krb5cc_$(id -u)
else
  echo "No Kerberos here"
fi
#
#   GENERIC ACTION STARTS HERE
#
# hdfs dfs -mkdir -p hdfs:///data/raw/ott_dazn/logs-archive-production/dt=20180924
#
# hdfs dfs -mkdir -p hdfs:///data/raw/ott_dazn/logs-archive-production/dt=20190120
#
##
MY_FOLDER="/home/siemanalyst/projects/logs-archive-production/fraud-canada-tokenizedwords"
spark2-submit --master yarn --deploy-mode client --conf spark.debug.maxToStringFields=1500 $MY_FOLDER/execute-fraud-canada-tokenizedwords-ngram85-stagedparquet.py --datev1 ${DATE_V1} > $MY_FOLDER/logs/execute-fraud-canada-tokenizedwords-ngram85-stagedparquet-lastrun-${DATE_V1}.log 2>&1
echo 0