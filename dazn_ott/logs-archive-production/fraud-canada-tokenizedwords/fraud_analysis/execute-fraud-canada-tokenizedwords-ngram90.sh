#!/bin/bash
MY_FOLDER="/home/siemanalyst/projects/logs-archive-production"
##
### MODEL: x ngrams90
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
hdfs dfs -rm -r -f -skipTrash hdfs:///data/staged/ott_dazn/fraud-canada-tokenizedwords-ngrams-90/dt=${DATE_V1}
##
spark2-submit --master yarn --deploy-mode client --conf spark.debug.maxToStringFields=1500 $MY_FOLDER/execute-fraud-canada-tokenizedwords-ngram90.py --datev1 ${DATE_V1} > $MY_FOLDER/logs/execute-fraud-canada-tokenizedwords-ngram90-lastrun-${DATE_V1}.log 2>&1 || true
## Stage url_ml_score_predict
#
# Export Log file for Local Analysis
echo "Export Log file for Local Analysis" >> $MY_FOLDER/logs/execute-fraud-canada-tokenizedwords-ngram90-lastrun-${DATE_V1}.log
hdfs dfs -copyToLocal hdfs:///data/staged/ott_dazn/fraud-canada-tokenizedwords/dt=${DATE_V1}/*.json  $MY_FOLDER/fraud_data/
#
mv -f $MY_FOLDER/fraud_data/part-*.json $MY_FOLDER/fraud_data/${DATE_V1}.json
ls $MY_FOLDER/fraud_data/${DATE_V1}.json >> $MY_FOLDER/logs/execute-fraud-canada-tokenizedwords-ngram90-lastrun-${DATE_V1}.log
cat $MY_FOLDER/fraud_data/${DATE_V1}.json >> $MY_FOLDER/logs/execute-fraud-canada-tokenizedwords-ngram90-lastrun-${DATE_V1}.log
#
