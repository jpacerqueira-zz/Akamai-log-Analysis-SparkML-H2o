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
JOB_LOG_FILE=$MY_FOLDER/logs/execute-fraud-canada-tokenizedwords-ngram85-stagedparquet-lastrun-${DATE_V1}.log
#
MY_HDFS_FOLDER=hdfs:///data/staged/ott_dazn/fraud-canada-tokenizedwords
MY_EMAIL_HDFS_FOLDER=hdfs:///data/staged/ott_dazn/fraud-canada-email
#
hdfs dfs -rm -r -f -skipTrash ${MY_HDFS_FOLDER}/dt=${DATE_V1} > $JOB_LOG_FILE 2>&1
#
# Old Slow JOB
#spark2-submit --master yarn --deploy-mode client --conf spark.debug.maxToStringFields=1500 $MY_FOLDER/execute-fraud-canada-tokenizedwords-ngram85.py --datev1 ${DATE_V1} > $JOB_LOG_FILE 2>&1
#
# 1.)  JOB # Search Video Playback Fraud Pattern Search
echo "Pyspark :-: execute-fraud-canada-tokenizedwords-ngram85-stagedparquet.py" >> $JOB_LOG_FILE 2>&1
spark2-submit --master yarn --deploy-mode client --conf spark.debug.maxToStringFields=1500 $MY_FOLDER/execute-fraud-canada-tokenizedwords-ngram85-stagedparquet.py --datev1 ${DATE_V1} > $JOB_LOG_FILE 2>&1
## Stage url_ml_score_predict
#
# Export Log file for Local Analysis
echo "Export Log file for Local Analysis" >> $JOB_LOG_FILE
hdfs dfs -cp ${MY_HDFS_FOLDER}/dt=${DATE_V1}/part-*.json ${MY_HDFS_FOLDER}/dt=${DATE_V1}/${DATE_V1}.json >> $JOB_LOG_FILE
hdfs dfs -rm ${MY_HDFS_FOLDER}/dt=${DATE_V1}/part-*.json >> $JOB_LOG_FILE
hdfs dfs -copyToLocal ${MY_HDFS_FOLDER}/dt=${DATE_V1}/${DATE_V1}.json  $MY_FOLDER/fraud_data/ >> $JOB_LOG_FILE 
#
ls $MY_FOLDER/fraud_data/${DATE_V1}.json >> $JOB_LOG_FILE
NUM_FRAUD_RECORDS=$(cat $MY_FOLDER/fraud_data/${DATE_V1}.json | wc -l )
echo " : " $JOB_LOG_FILE
echo "$NUM_FRAUD_RECORDS" >> $JOB_LOG_FILE 
#
if [ -z "$NUM_FRAUD_RECORDS" ] || [ "$NUM_FRAUD_RECORDS" = "0" ]
then
  # for any:any credentials
  hdfs dfs -mkdir -p ${MY_EMAIL_HDFS_FOLDER}/dt=${DATE_V1} >> $JOB_LOG_FILE
  rm -r -f $MY_FOLDER/fraud_data/${DATE_V1}.json  >> $JOB_LOG_FILE
  hdfs dfs -copyFromLocal -f $JOB_LOG_FILE ${MY_EMAIL_HDFS_FOLDER}/dt=${DATE_V1}
  rm -r -f $JOB_LOG_FILE
  #
  exit 1
else
  # for any:any credentials
  hdfs dfs -mkdir -p ${MY_EMAIL_HDFS_FOLDER}/dt=${DATE_V1}
  hdfs dfs -cp ${MY_HDFS_FOLDER}/dt=${DATE_V1}/${DATE_V1}.json ${MY_EMAIL_HDFS_FOLDER}/dt=${DATE_V1}
  ## 2.) Job expand Pattern in Source for the Same ViwerID ( last 8 words are the same )
  echo "Pyspark :-: execute-fraud-canada-tokenizedwords-expand-viewer-ngram85-stagedparquet.py" >> $JOB_LOG_FILE 2>&1
  spark2-submit --master yarn --deploy-mode client --conf spark.debug.maxToStringFields=1500 $MY_FOLDER/execute-fraud-canada-tokenizedwords-expand-viewer-ngram85-stagedparquet.py --datev1 ${DATE_V1} >> $JOB_LOG_FILE 2>&1
  hdfs dfs -cp -f ${MY_HDFS_FOLDER}/dt=${DATE_V1}/part-*.json ${MY_HDFS_FOLDER}/dt=${DATE_V1}/expand-${DATE_V1}.json >> $JOB_LOG_FILE
  hdfs dfs -rm -r -f ${MY_HDFS_FOLDER}/dt=${DATE_V1}/part-*.json >> $JOB_LOG_FILE
  hdfs dfs -cp ${MY_HDFS_FOLDER}/dt=${DATE_V1}/expand-${DATE_V1}.json ${MY_EMAIL_HDFS_FOLDER}/dt=${DATE_V1} >> $JOB_LOG_FILE
  rm -r -f $MY_FOLDER/fraud_data/${DATE_V1}.json  >> $JOB_LOG_FILE
  hdfs dfs -copyFromLocal -f $JOB_LOG_FILE ${MY_EMAIL_HDFS_FOLDER}/dt=${DATE_V1}
  rm -r -f $JOB_LOG_FILE
  #
  exit 0
fi