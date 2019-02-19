#!/bin/bash
MY_FOLDER="/home/siemanalyst/projects/logs-archive-production/fraud-canada-tokenizedwords/notebooks"
MY_FOLDER_AUX="/home/siemanalyst/projects/logs-archive-production/fraud-canada-tokenizedwords/"
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
MY_HDFS_ADVANCED_FOLDER=hdfs:///data/staged/ott_dazn/advanced-model-data
hdfs dfs -rm -r -f -skipTrash $MY_HDFS_ADVANCED_FOLDER/fraud-notfraud-canada-tokenizedwords-ngrams-7-features-85/dt=${DATE_V1} >> $JOB_LOG_FILE 2>&1
hdfs dfs -rm -r -f -skipTrash $MY_HDFS_ADVANCED_FOLDER/label-fraud-notfraud-data-model/dt=${DATE_V1} >> $JOB_LOG_FILE 2>&1
hdfs dfs -rm -r -f -skipTrash $MY_HDFS_ADVANCED_FOLDER/not-fraud-canada-tokenizedwords/dt=${DATE_V1} >> $JOB_LOG_FILE 2>&1
hdfs dfs -rm -r -f -skipTrash $MY_HDFS_ADVANCED_FOLDER/the-most-frequent-fraud-hash_message/dt=${DATE_V1} >> $JOB_LOG_FILE 2>&1
hdfs dfs -rm -r -f -skipTrash $MY_HDFS_ADVANCED_FOLDER/the-most-frequent-notfraud-hash_message/dt=${DATE_V1} >> $JOB_LOG_FILE 2>&1
#
# x1 Job Clean Not-Fraud and Ngrams/Features/Vectors
MY_HDFS_TOKEN_FOLDER=hdfs:///data/staged/ott_dazn/fraud-canada-tokenizedwords
NUM_DAILY_FRAUD_RECORDS=$(hdfs dfs -cat $MY_HDFS_TOKEN_FOLDER/${DATE_V1}.json | wc -l )
echo "NUM_DAILY_FRAUD_RECORDS HDFS :" >> $JOB_LOG_FILE
echo "$NUM_DAILY_FRAUD_RECORDS" >> $JOB_LOG_FILE 
echo "|___|" >> $JOB_LOG_FILE 2>&1
echo "|" >> $JOB_LOG_FILE 2>&1
echo "Pyspark :-: x1-FraudCanada-Model-NGrams-CountVectorizer-KL-KS-Entropy-Model-CleanData.py" >> $JOB_LOG_FILE 2>&1
echo "|" >> $JOB_LOG_FILE 2>&1
spark2-submit --master yarn --deploy-mode client --conf spark.debug.maxToStringFields=1500 $MY_FOLDER/x1-FraudCanada-Model-NGrams-CountVectorizer-KL-KS-Entropy-Model-CleanData.py --datev1 ${DATE_V1} --fraudnrdaily $NUM_DAILY_FRAUD_RECORDS >> $JOB_LOG_FILE 2>&1
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
hdfs dfs -mkdir -p ${MY_HDFS_ADVANCED_FOLDER}/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}
#
FILE_PREDICTION_TEST=$(find . -name file_prediction_test_${DATE_V1}.csv\*)
FILE_PREDICTION_TRAIN=$(find . -name file_prediction_train_${DATE_V1}.csv\*)
FILE_TEST=$(find . -name file_test_${DATE_V1}.csv\*)
FILE_TRAIN=$(find . -name file_train_${DATE_V1}.csv\*)
#
hdfs dfs -moveFromLocal -f ${FILE_PREDICTION_TEST} ${FILE_PREDICTION_TRAIN} ${FILE_TEST} ${FILE_TRAIN} ${MY_HDFS_ADVANCED_FOLDER}/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}
echo ${FILE_PREDICTION_TEST} >> $JOB_LOG_FILE 2>&1
echo ${FILE_PREDICTION_TRAIN} >> $JOB_LOG_FILE 2>&1
echo ${FILE_TEST} >> $JOB_LOG_FILE 2>&1
echo ${FILE_TRAIN} >> $JOB_LOG_FILE 2>&1
#
### X4 Job
echo "|" >> $JOB_LOG_FILE 2>&1
echo "Pyspark :-: x4-FraudCanada-Merge-Scoring-Data-From-Model.py" >> $JOB_LOG_FILE 2>&1
hdfs dfs -rm -r -f -skipTrash ${MY_HDFS_ADVANCED_FOLDER}/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}/merged_prediction_train >> $JOB_LOG_FILE 2>&1
rm -f ${MY_FOLDER}/product_model_prediction/merge_prediction_train_${DATE_V1}.csv
echo "|" >> $JOB_LOG_FILE 2>&1
spark2-submit --master yarn --deploy-mode client --conf spark.debug.maxToStringFields=3500 $MY_FOLDER/x4-FraudCanada-Merge-Scoring-Data-From-Model.py --datev1 ${DATE_V1} >> $JOB_LOG_FILE 2>&1
#
hdfs dfs -cp ${MY_HDFS_ADVANCED_FOLDER}/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}/merged_prediction_train/part-*.csv ${MY_HDFS_ADVANCED_FOLDER}/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}/merged_prediction_train/merge_prediction_train_${DATE_V1}.csv >> $JOB_LOG_FILE 2>&1
#
### X5 Job
echo "|" >> $JOB_LOG_FILE 2>&1
echo "Pyspark :-: x5-search-discover-values-hash_message.py" >> $JOB_LOG_FILE 2>&1
hdfs dfs -rm -r -f -skipTrash ${MY_HDFS_ADVANCED_FOLDER}/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}/no_match_hash_message >> $JOB_LOG_FILE 2>&1
rm -f ${MY_FOLDER}/product_model_prediction/no_match_hash_message_${DATE_V1}.csv
echo "|" >> $JOB_LOG_FILE 2>&1
spark2-submit --master yarn --deploy-mode client --conf spark.debug.maxToStringFields=3500 $MY_FOLDER/x5-search-discover-values-hash_message.py --datev1 ${DATE_V1} >> $JOB_LOG_FILE 2>&1
#
hdfs dfs -cp ${MY_HDFS_ADVANCED_FOLDER}/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}/no_match_hash_message/part-*.csv ${MY_HDFS_ADVANCED_FOLDER}/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}/no_match_hash_message/TEMP_no_match_hash_message_${DATE_V1}.csv >> $JOB_LOG_FILE 2>&1
#
NUM_ML_FRAUD_RECORDS=$(hdfs dfs -cat hdfs:///data/staged/ott_dazn/advanced-model-data/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}/no_match_hash_message/TEMP_no_match_hash_message_${DATE_V1}.csv | wc -l )
echo "File Lines : NO_MATCH_NUM_FRAUD_RECORDS = "  >> $JOB_LOG_FILE 2>&1
echo "$NUM_ML_FRAUD_RECORDS"  >> $JOB_LOG_FILE 2>&1
#
# Only Fire Message Out if There is also Linear Fraud Detection in Filter
#
if ([ -z "$NUM_ML_FRAUD_RECORDS" ] || [ "$NUM_ML_FRAUD_RECORDS" = "0" ]) && ( [ -z "$NUM_DAILY_FRAUD_RECORDS" ] || [ "$NUM_DAILY_FRAUD_RECORDS" = "0" ] )
then
  # for zero records fraud and zero linear
  echo "| NUM_ML_FRAUD_RECORDS=$NUM_ML_FRAUD_RECORDS & NUM_DAILY_FRAUD_RECORDS=$NUM_DAILY_FRAUD_RECORDS"
  echo "| Job Finished  ___;" >> $JOB_LOG_FILE 2>&1
  # Remove Training model as there are no Fraudulent discoveries
  rm -rf $MY_FOLDER/product_model_bin/ngrams7_features85_m50/v${DATE_V1} >> $JOB_LOG_FILE 2>&1
  hdfs dfs -copyFromLocal -f $JOB_LOG_FILE $MY_HDFS_ADVANCED_FOLDER/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1} >> $JOB_LOG_FILE 2>&1
  rm -r -f $JOB_LOG_FILE
  exit 1
else
  # for any records fraud
  hdfs dfs -cp ${MY_HDFS_ADVANCED_FOLDER}/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}/no_match_hash_message/TEMP_no_match_hash_message_${DATE_V1}.csv ${MY_HDFS_ADVANCED_FOLDER}/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}/no_match_hash_message/no_match_hash_message_${DATE_V1}.csv >> $JOB_LOG_FILE 2>&1
  echo "| NUM_ML_FRAUD_RECORDS=$NUM_ML_FRAUD_RECORDS & NUM_DAILY_FRAUD_RECORDS=$NUM_DAILY_FRAUD_RECORDS"
  echo "| Job Finished  ___;" >> $JOB_LOG_FILE 2>&1
  hdfs dfs -copyFromLocal -f $JOB_LOG_FILE $MY_HDFS_ADVANCED_FOLDER/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt=${DATE_V1}
  rm -r -f $JOB_LOG_FILE
  exit 0
fi
#