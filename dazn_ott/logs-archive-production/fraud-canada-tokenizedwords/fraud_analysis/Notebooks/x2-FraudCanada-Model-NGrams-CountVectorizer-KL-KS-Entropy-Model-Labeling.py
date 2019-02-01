#
import findspark
findspark.init()
#
import pyspark
from pyspark.sql import functions as pfunc
from pyspark.sql import SQLContext
from pyspark.sql import Window, types
#
import re
import pandas as pd
import numpy as np
from pandas import DataFrame
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from scipy.stats import kstest
from scipy import stats
#
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import RegexTokenizer
#
#import org.apache.spark.ml.feature.NGram
from pyspark.ml.feature import NGram
#
from collections import Counter
#
from pyspark.ml.feature import NGram
#
from pyspark.ml.feature import NGram, CountVectorizer, VectorAssembler
from pyspark.ml import Pipeline
#
from pyspark.mllib.linalg import SparseVector, DenseVector
#
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors
#
#  FILTER with PySpark SQL Functions F.
from pyspark.sql import functions as F
# FUNCTIONS
# 
##
## Register Generic UDF Functions
# -----------------------------------------------------------------------------
# KL : Kullback-Leibler Divergence
# KS : Kolmogorov-Smirnov ( Sample sizes can be different)
# -----------------------------------------------------------------------------    
## NGRAM hash_mesage KL from URL TO Default NGRAM KL
def func_kl_ngram_msg(var1,var_match):
    ##Making sure the analysis of has_message SHA_512 is linear.
    ##Making sure the analysis is over the words list on same size
    def KL(P,Q):
        epsilon = 0.00001
        P = P+epsilon
        Q = Q+epsilon
        divergence = np.sum(P*np.log(P/Q))
        return np.asscalar(divergence)
    ##Making sure the analysis ignore trash
    cleanvar=var1.strip('http://').strip('https://').strip('www.')
    len_var1=len(cleanvar)
    idx_var1=cleanvar[:4]# Use "root1" as baseline #cleanvar.split("/")[0]#cleanvar[:8]
    list_values1 = list(cleanvar)
    list_of_ord_values1 = [ord(char) for char in list_values1]
    values1 = np.asarray(list_of_ord_values1)
    # URLS with at least 4166 characters
    ## TODO : Complete this list with common Malware/phishing sites used in internal webtraffic
    list_values2 = var_match
    list_values2 += var_match
    ## Making sure the analysis start on the same webdomain name
    idx_var2 = list_values2.find(idx_var1)
    start_values2 = list_values2[idx_var2:(idx_var2+len_var1)]
    list_values2 = list(start_values2+list_values2)
    list_values2 = list_values2[:len_var1]
    list_of_ord_values2 = [ord(char) for char in list_values2]
    values2 = np.asarray(list_of_ord_values2)
    return KL(values1,values2)
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
## NGRAM hash_message "Kolmogorov-Smirnov test KS pvalue= ks_2samp function8.
def func_ks_ngram_msg(var1,var_match):
    ##Making sure the analysis of has_message SHA_512 is linear.
    ##Making sure the analysis is over the words list on same size
    cleanvar=var1.strip('http://').strip('https://').strip('www.')
    len_var1=len(cleanvar)
    idx_var1=cleanvar[:4]# Use "root1" as baseline #cleanvar.split("/")[0]#cleanvar[:8]
    list_values1 = list(cleanvar)
    list_of_ord_values1 = [ord(char) for char in list_values1]
    values1 = np.asarray(list_of_ord_values1)
    values1= np.sort(values1)
    # URLS with up to least 4166 characters
    list_values2 = var_match
    list_values2 += var_match
    ## Making sure the analysis start on the same messgae
    idx_var2 = list_values2.find(idx_var1)
    start_values2 = list_values2[idx_var2:(idx_var2+len_var1)]
    list_values2 = list(start_values2+list_values2)
    list_values2 = list_values2[:len_var1]
    list_of_ord_values2 = [ord(char) for char in list_values2]
    values2 = np.asarray(list_of_ord_values2)
    values2=np.sort(values2)
    (Darray,pvalue)=stats.ks_2samp(values1, values2)
    return np.asscalar(pvalue)
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
## Entropy TO hash_message function3.
def func_entropy_ngram_msg(var1,var_match):
    ##Making sure the analysis of has_message SHA_512 is linear.
    ##Making sure the analysis is over the words list ignore size
    #
    cleanvar=var1.strip('http://').strip('https://').strip('www.')
    len_var1=len(cleanvar)
    idx_var1=cleanvar[:4]# Use "root1" as baseline #cleanvar.split("/")[0]#cleanvar[:8]
    list_values1 = list(cleanvar)
    list_of_ord_values1 = [ord(char) for char in list_values1]
    values1 = np.asarray(list_of_ord_values1)
    # URLS with at least 4166 characters
    ## TODO : Complete this list with an NGRAM search
    list_values2 = var_match
    list_values2 += var_match
    ## Making sure the analysis start on point
    idx_var2 = list_values2.find(idx_var1)
    start_values2 = list_values2[idx_var2:(idx_var2+len_var1)]
    list_values2 = list(start_values2+list_values2)
    list_values2 = list_values2[:len_var1]
    list_of_ord_values2 = [ord(char) for char in list_values2]
    values2 = np.asarray(list_of_ord_values2)
    pvalue=stats.entropy(values1, values2)
    scalar_pvalue=np.asscalar(pvalue)
    return scalar_pvalue
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
func_kl_ngram_msg_udf = udf(func_kl_ngram_msg, FloatType())
func_ks_ngram_msg_udf = udf(func_ks_ngram_msg, FloatType())
func_entropy_ngram_msg_udf = udf(func_entropy_ngram_msg, FloatType())
#
# Arguments
#
import argparse
## Parse date_of execution
parser = argparse.ArgumentParser()
parser.add_argument("--datev1", help="Execution Date")
args = parser.parse_args()
if args.datev1:
    processdate = args.datev1
# GENERAL PREPARATION SCRIPT
#
#  Date in format YYYYMMDD
process_date = processdate
if not process_date:
    process_date = "20190122"
#
sc = pyspark.SparkContext(appName="FraudCanada-Model-NGrams-CountVectorizer-KL-KS-Entropy-Model-Labeling")
sqlContext = SQLContext(sc)
#
#
input_most_frequent_fraud_df="hdfs:///data/staged/ott_dazn/advanced-model-data/the-most-frequent-fraud-hash_message/dt="+process_date
#
input_most_frequent_notfraud_df="hdfs:///data/staged/ott_dazn/advanced-model-data/the-most-frequent-notfraud-hash_message/dt="+process_date
#
input_fraud="hdfs:///data/staged/ott_dazn/advanced-model-data/fraud-notfraud-canada-tokenizedwords-ngrams-7-features-85/dt="+process_date
#
input_file1="hdfs:///data/staged/ott_dazn/advanced-model-data/fraud-notfraud-canada-tokenizedwords-ngrams-7-features-85/dt="+process_date
#
output_file1="hdfs:///data/staged/ott_dazn/advanced-model-data/label-fraud-notfraud-data-model/dt="+process_date
#
preserve_training_output_file="hdfs:///data/staged/ott_dazn/advanced-model-data/preserve-training-output-automl-clean/dt="+process_date
#
# The most Frequent would the ones with the max frequency of NGrams85 tokens
pd.options.display.max_colwidth = 512
#
standard_fraud_ngram=sqlContext.read.json(input_most_frequent_fraud_df)\
.orderBy(col('count').desc()).select(col('hash_message')).limit(1).toPandas()['hash_message'][0]
print("Value UDF : standard_fraud_ngram=")
print(standard_fraud_ngram)
#
# Select Tokens/words form the max frequency of NGrams85 tokens hash_message
standard_fraud_ngram_words=sqlContext.read.json(input_fraud)
standard_fraud_ngram_words.printSchema()
#
standard_fraud_words_search=standard_fraud_ngram_words\
.withColumn('fraud_master_hash',lit(standard_fraud_ngram).cast('string'))\
.filter(" hash_message=fraud_master_hash ")
standard_fraud_words_search.printSchema()
#
standard_fraud_words=standard_fraud_words_search\
.withColumn('words_conc',F.concat_ws('',col('words')).cast('string'))\
.select(col('words_conc')).limit(1).toPandas()['words_conc'][0] 
#
print("Value UDF : standard_fraud_words=")
print(standard_fraud_words)
#
# Working now!
# Needs to be bigger than message twice should be tested!
#   Py4JJavaError: An error occurred while calling z:org.apache.spark.sql.functions.lit.
#      : java.lang.RuntimeException: Unsupported literal type class java.util.HashMap 
# {0=root15c466c8e6e8f9d17adb73426cd55c70f72b9f18e39e3455c9043a18b86b122b6requestmethodgeturlhttpsislcadazncommislv2playbackassetidfg5oon8sl71n1nfwuegbo8npgeventidarticleidfg5oon8sl71n1nfwuegbo8npgformatmpegdashplayeriddaznf3874e050812a853securetruelanguagecodeenlatitudenulllongitudenullplatformandroidtvmanufacturernvidiamodelnullmtalanguagecodeenclientip50100225179headersuseragentmozilla50linuxandroid800shieldandroidtvbuildopr6170623010wvapplewebkit53736khtmllikegeckoversion40chrome710357899mobilesafari53736fev1420typeinresponsestatuscode200reasonphraseokduration47jwtvieweridc2ebc25d8085deviceid993bf365c72c4b0b9168c2ebc25d8085f3874e050812a853userstatusactivepaid}
#
standard_notfraud_ngram=sqlContext.read.json(input_most_frequent_notfraud_df)\
.orderBy(col('count').desc()).select(col('hash_message')).limit(1).toPandas()['hash_message'][0]
print("Value UDF : standard_notfraud_ngram=")
print(standard_notfraud_ngram)
#
# Select Tokens/words form the max frequency of NGrams85 tokens hash_message
standard_notfraud_ngram_words=sqlContext.read.json(input_fraud)
standard_notfraud_ngram_words.printSchema()
#
standard_notfraud_words_search=standard_notfraud_ngram_words\
.withColumn('fraud_master_hash',lit(standard_notfraud_ngram).cast('string'))\
.filter(" hash_message=fraud_master_hash ")
standard_notfraud_words_search.printSchema()
#
standard_notfraud_words=standard_notfraud_words_search\
.withColumn('words_conc',F.concat_ws('',col('words')).cast('string'))\
.select(col('words_conc')).limit(1).toPandas()['words_conc'][0] 
#
print("Value UDF : standard_notfraud_words=")
print(standard_notfraud_words)
#
ngram7_fraud=sqlContext.read.json(input_file1)
ngram7_fraud.printSchema()
#
drop_phish_cols=['words','ngrams_7']
#
fraud_label_read_df=ngram7_fraud.filter("hash_message is not NULL")\
.withColumn('words_conc',F.concat_ws('',col('words')).cast('string'))\
.drop(*drop_phish_cols)\
.withColumn('kl_fraud_words',func_kl_ngram_msg_udf(col('words_conc'),lit(standard_fraud_words).cast('string')).cast('double'))\
.withColumn('ks_fraud_words',func_ks_ngram_msg_udf(col('words_conc'),lit(standard_fraud_words).cast('string')).cast('double'))\
.withColumn('entropy_fraud_words',func_entropy_ngram_msg_udf(col('words_conc'),lit(standard_fraud_words).cast('string')).cast('double'))\
.withColumn('kl_notfraud_words',func_kl_ngram_msg_udf(col('words_conc'),lit(standard_notfraud_words).cast('string')).cast('double'))\
.withColumn('ks_notfraud_words',func_ks_ngram_msg_udf(col('words_conc'),lit(standard_notfraud_words).cast('string')).cast('double'))\
.withColumn('entropy_notfraud_words',func_entropy_ngram_msg_udf(col('words_conc'),lit(standard_notfraud_words).cast('string')).cast('double'))\
.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
fraud_label_read_df.printSchema()
#
fraud_label_read_df.coalesce(1).write.json(output_file1)
#
fraud_label_read_df.unpersist()
#
sc.stop()
#
print("Model Data KL,KS, Entropy Done! NGrams Vectors Data Done!")
#