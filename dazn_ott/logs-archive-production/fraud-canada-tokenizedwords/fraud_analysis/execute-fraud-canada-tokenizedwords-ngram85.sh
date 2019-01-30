#!/bin/bash
MY_FOLDER="/home/siemanalyst/projects/logs-archive-production/fraud-canada-tokenizedwords"
##
### MODEL: x ngrams75
#
DATE_V1=$1
if [ -z "$DATE_V1" ]
then
 DATE_V1=20190101
fi
# Make sure security Kerberos is cached
#
if [ -f ~/.keytabs/$(whoami).keytab ]; then
  # for any:any credentials
  export KRB5CCNAME=/tmp/krb5cc_$(id -u)
  #
  kinit -kt ~/.keytabs/$(whoami).keytab $(whoami)/chpbda@BDA2.PERFORMGROUP.COM -c /tmp/krb5cc_$(id -u)
  klist /tmp/krb5cc_$(id -u)
else
  echo "No Kerberos here"
fi
#
#
echo "RAW d=${DATE_V1} FILES COUNT : "
hdfs dfs -ls hdfs:///data/raw/ott_dazn/logs-archive-production/dt=${DATE_V1}/*.log.gz | wc -l

#
## Atomic All or Nothing
####Clear staged
#
hdfs dfs -rm -r -f -skipTrash hdfs:///data/staged/ott_dazn/fraud-canada-tokenizedwords/dt=${DATE_V1}
hdfs dfs -rm -r -f -skipTrash hdfs:///data/staged/ott_dazn/fraud-canada-tokenizedwords-ngrams-85/dt=${DATE_V1}
#
JOB_LOG_FILE=$MY_FOLDER/logs/execute-fraud-canada-tokenizedwords-ngram85-stagedparquet-lastrun-${DATE_V1}.log
#
# Old Slow JOB
#spark2-submit --master yarn --deploy-mode client --conf spark.debug.maxToStringFields=1500 $MY_FOLDER/execute-fraud-canada-tokenizedwords-ngram85.py --datev1 ${DATE_V1} > $JOB_LOG_FILE 2>&1
#
# New Faster JOB
spark2-submit --master yarn --deploy-mode client --conf spark.debug.maxToStringFields=1500 $MY_FOLDER/execute-fraud-canada-tokenizedwords-ngram85-stagedparquet.py --datev1 ${DATE_V1} > $JOB_LOG_FILE 2>&1
## Stage url_ml_score_predict
#
# Export Log file for Local Analysis
echo "Export Log file for Local Analysis" >> $JOB_LOG_FILE
hdfs dfs -copyToLocal hdfs:///data/staged/ott_dazn/fraud-canada-tokenizedwords/dt=${DATE_V1}/*.json  $MY_FOLDER/fraud_data/
#
mv -f $MY_FOLDER/fraud_data/part-*.json $MY_FOLDER/fraud_data/${DATE_V1}.json
ls $MY_FOLDER/fraud_data/${DATE_V1}.json >> $JOB_LOG_FILE
NUM_FRAUD_RECORDS=$(cat $MY_FOLDER/fraud_data/${DATE_V1}.json | wc -l )
echo " : " $JOB_LOG_FILE
echo "$NUM_FRAUD_RECORDS" >> $JOB_LOG_FILE 
#
if [ -z "$NUM_FRAUD_RECORDS" ] || [ "$NUM_FRAUD_RECORDS" = "0" ]
then
  # for any:any credentials
  hdfs dfs -mkdir -p hdfs:///data/staged/ott_dazn/fraud-canada-email/dt=${DATE_V1}
  hdfs dfs -copyFromLocal $JOB_LOG_FILE hdfs:///data/staged/ott_dazn/fraud-canada-email/dt=${DATE_V1}
  #
    exit 1
else
  # for any:any credentials
  hdfs dfs -mkdir -p hdfs:///data/staged/ott_dazn/fraud-canada-email/dt=${DATE_V1}
  hdfs dfs -copyFromLocal $MY_FOLDER/fraud_data/${DATE_V1}.json hdfs:///data/staged/ott_dazn/fraud-canada-email/dt=${DATE_V1}
  hdfs dfs -copyFromLocal $JOB_LOG_FILE hdfs:///data/staged/ott_dazn/fraud-canada-email/dt=${DATE_V1}
  #
  exit 0
fi