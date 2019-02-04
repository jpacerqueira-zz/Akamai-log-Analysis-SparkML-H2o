#!/bin/bash
MY_FOLDER="/home/siemanalyst/projects/logs-archive-production/fraud-canada-tokenizedwords/notebooks"
##
### MODEL: x ngrams75
#
DATE_V1=$1
if [ -z "$DATE_V1" ]
then
 DATE_V1=20190122
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
## Stage urltopredict folder h2o
unset http_proxy
#
#
JOB_LOG_FILE=$MY_FOLDER/logs/execute-fraud-automl-lastrun-${DATE_V1}.log
#
echo "Process d=${DATE_V1} FILES COUNT : " > $JOB_LOG_FILE 2>&1
hdfs dfs -ls hdfs:///data/staged/ott_dazn/logs-archive-production/parquet/dt=${DATE_V1}/*.parquet | wc -l >> $JOB_LOG_FILE 2>&1 
echo "|___|" >> $JOB_LOG_FILE 2>&1
#
## Atomic All or Nothing to Clear all staged on date
#
hdfs dfs -rm -r -f -skipTrash hdfs:///data/staged/ott_dazn/advanced-model-data/fraud-notfraud-canada-tokenizedwords-ngrams-7-features-85/dt=${DATE_V1} >> $JOB_LOG_FILE 2>&1
hdfs dfs -rm -r -f -skipTrash hdfs:///data/staged/ott_dazn/advanced-model-data/label-fraud-notfraud-data-model/dt=${DATE_V1} >> $JOB_LOG_FILE 2>&1
hdfs dfs -rm -r -f -skipTrash hdfs:///data/staged/ott_dazn/advanced-model-data/not-fraud-canada-tokenizedwords/dt=${DATE_V1} >> $JOB_LOG_FILE 2>&1
hdfs dfs -rm -r -f -skipTrash hdfs:///data/staged/ott_dazn/advanced-model-data/the-most-frequent-fraud-hash_message/dt=${DATE_V1} >> $JOB_LOG_FILE 2>&1
hdfs dfs -rm -r -f -skipTrash hdfs:///data/staged/ott_dazn/advanced-model-data/the-most-frequent-notfraud-hash_message/dt=${DATE_V1} >> $JOB_LOG_FILE 2>&1
#
# x1 Job Clean Not-Fraud and Ngrams/Features/Vectors
echo "|" >> $JOB_LOG_FILE 2>&1
echo "Pyspark :-: x1-FraudCanada-Model-NGrams-CountVectorizer-KL-KS-Entropy-Model-CleanData.py" >> $JOB_LOG_FILE 2>&1
echo "|" >> $JOB_LOG_FILE 2>&1
spark2-submit --master yarn --deploy-mode client --conf spark.debug.maxToStringFields=1500 $MY_FOLDER/x1-FraudCanada-Model-NGrams-CountVectorizer-KL-KS-Entropy-Model-CleanData.py --datev1 ${DATE_V1} >> $JOB_LOG_FILE 2>&1
## x2 Job
echo "|" >> $JOB_LOG_FILE 2>&1
echo "Pyspark :-: x2-FraudCanada-Model-NGrams-CountVectorizer-KL-KS-Entropy-Model-Labeling.py" >> $JOB_LOG_FILE 2>&1
echo "|" >> $JOB_LOG_FILE 2>&1
spark2-submit --master yarn --deploy-mode client --conf spark.debug.maxToStringFields=1500 $MY_FOLDER/x2-FraudCanada-Model-NGrams-CountVectorizer-KL-KS-Entropy-Model-Labeling.py --datev1 ${DATE_V1} >> $JOB_LOG_FILE 2>&1
### X3 Job
echo "|" >> $JOB_LOG_FILE 2>&1
echo "Pyspark :-: x3-FraudCanada-AUTOML-Model-NGrams-CountVectorizer-KL-KS-Entropy.py" >> $JOB_LOG_FILE 2>&1
echo "|" >> $JOB_LOG_FILE 2>&1
spark2-submit --master yarn --deploy-mode client --conf spark.debug.maxToStringFields=1500 $MY_FOLDER/x3-FraudCanada-AUTOML-Model-NGrams-CountVectorizer-KL-KS-Entropy.py --datev1 ${DATE_V1} >> $JOB_LOG_FILE 2>&1
#
echo "| Move Files " >> $JOB_LOG_FILE 2>&1
hdfs dfs -mkdir -p hdfs:///data/staged/ott_dazn/advanced-model-data/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}
FILE_PREDICTION=$(find . -name file_prediction_${DATE_V1}.csv\*)
FILE_TRAIN=$(find . -name file_train_${DATE_V1}.csv\*)
hdfs dfs -moveFromLocal -f ${FILE_PREDICTION} ${FILE_TRAIN} hdfs:///data/staged/ott_dazn/advanced-model-data/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}
echo ${FILE_PREDICTION} >> $JOB_LOG_FILE 2>&1
echo ${FILE_TRAIN} >> $JOB_LOG_FILE 2>&1
#
### X4 Job
echo "|" >> $JOB_LOG_FILE 2>&1
hdfs dfs -rm -r -f -skipTrash hdfs:///data/staged/ott_dazn/advanced-model-data/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}/merged_prediction_train >> $JOB_LOG_FILE 2>&1
rm -f ${MY_FOLDER}/product_model_prediction/merge_prediction_train_${DATE_V1}.csv
echo "Pyspark :-: x4-FraudCanada-Merge-Scoring-Data-From-Model.py" >> $JOB_LOG_FILE 2>&1
echo "|" >> $JOB_LOG_FILE 2>&1
spark2-submit --master yarn --deploy-mode client --conf spark.debug.maxToStringFields=3500 $MY_FOLDER/x4-FraudCanada-Merge-Scoring-Data-From-Model.py --datev1 ${DATE_V1} >> $JOB_LOG_FILE 2>&1
#
hdfs dfs -cp hdfs:///data/staged/ott_dazn/advanced-model-data/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}/merged_prediction_train/part-*.csv hdfs:///data/staged/ott_dazn/advanced-model-data/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}/merged_prediction_train/merge_prediction_train_${DATE_V1}.csv >> $JOB_LOG_FILE 2>&1
#
### X5 Job
echo "|" >> $JOB_LOG_FILE 2>&1
hdfs dfs -rm -r -f -skipTrash hdfs:///data/staged/ott_dazn/advanced-model-data/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}/no_match_hash_message >> $JOB_LOG_FILE 2>&1
rm -f ${MY_FOLDER}/product_model_prediction/no_match_hash_message_${DATE_V1}.csv
echo "Pyspark :-: x5-search-discover-values-hash_message.py" >> $JOB_LOG_FILE 2>&1
echo "|" >> $JOB_LOG_FILE 2>&1
spark2-submit --master yarn --deploy-mode client --conf spark.debug.maxToStringFields=3500 $MY_FOLDER/x5-search-discover-values-hash_message.py --datev1 ${DATE_V1} >> $JOB_LOG_FILE 2>&1
#
hdfs dfs -cp hdfs:///data/staged/ott_dazn/advanced-model-data/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}/no_match_hash_message/part-*.csv hdfs:///data/staged/ott_dazn/advanced-model-data/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}/no_match_hash_message/no_match_hash_message_${DATE_V1}.csv >> $JOB_LOG_FILE 2>&1
#
echo "| Job Finished  ___;" >> $JOB_LOG_FILE 2>&1
#