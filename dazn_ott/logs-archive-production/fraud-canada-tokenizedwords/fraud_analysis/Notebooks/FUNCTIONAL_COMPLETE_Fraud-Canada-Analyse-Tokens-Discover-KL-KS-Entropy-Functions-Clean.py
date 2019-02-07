
# coding: utf-8

# In[1]:


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
#
sc = pyspark.SparkContext(appName="FraudCanada-Model-NGrams-CountVectorizer-KL-KS-Entropy-Model-CleanData")
sqlContext = SQLContext(sc)
#
# Arguments
#
#import argparse
## Parse date_of execution
#parser = argparse.ArgumentParser()
#parser.add_argument("--datev1", help="Execution Date")
#args = parser.parse_args()
#if args.datev1:
#    processdate = args.datev1
# GENERAL PREPARATION SCRIPT
#
#  Date in format YYYYMMDD
#process_date = processdate
#if not process_date:
#    process_date = "20181231"
#
#
process_date="20190206"
#
input_file1_playback_fraud="hdfs:///data/staged/ott_dazn/fraud-canada-tokenizedwords/dt=*/*.json"
output_file1="hdfs:///data/staged/ott_dazn/advanced-model-data/fraud-notfraud-canada-tokenizedwords-ngrams-7-features-85/dt="+process_date
#
input_file2_playback_not_fraud="hdfs:///data/staged/ott_dazn/logs-archive-production/parquet/dt="+process_date+"/*.parquet"
output_file2="hdfs:///data/staged/ott_dazn/advanced-model-data/not-fraud-canada-tokenizedwords/dt="+process_date
input_file3=output_file2
#
input_file4="hdfs:///data/staged/ott_dazn/advanced-model-data/fraud-notfraud-canada-tokenizedwords-ngrams-7-features-85/dt="+process_date+"/*.*"
#
output_most_frequent_fraud_df="hdfs:///data/staged/ott_dazn/advanced-model-data/the-most-frequent-fraud-hash_message/dt="+process_date
#
output_most_frequent_notfraud_df="hdfs:///data/staged/ott_dazn/advanced-model-data/the-most-frequent-notfraud-hash_message/dt="+process_date
#
#  FILTER Non-Fraud AND LABEL
from pyspark.sql import functions as F
#
#
df2= sqlContext.read.parquet(input_file2_playback_not_fraud)
df2.printSchema()
#
df3 = df2.filter(" (message LIKE '%\"Url\":\"https://isl-ca.dazn.com/misl/v2/Playback%') AND (message LIKE '%,\"Response\":{\"StatusCode\":200,\"ReasonPhrase\":\"OK\",%') AND ( ( (message LIKE '%&Format=MPEG-DASH&%' OR message LIKE '%&Format=M3U&%') ) OR (message NOT LIKE '%\"User-Agent\":\"Mozilla/5.0,(Macintosh; Intel Mac OS X 10_12_6),AppleWebKit/605.1.75,(KHTML, like Gecko),Version/11.1.2,Safari/605.1.75\"},%')   )  ")
df3.printSchema()
df4 = df3.withColumn("messagecut", expr("substring(message, locate('|Livesport.WebApi.Controllers.Playback.PlaybackV2Controller|',message)+60 , length(message)-1)"))
#
# val regexTokenizer = new RegexTokenizer().setInputCol("messagecut").setOutputCol("words").setPattern("\\w+|").setGaps(false)
#
regexTokenizer = RegexTokenizer(minTokenLength=1, gaps=False, pattern='\\w+|', inputCol="messagecut", outputCol="words", toLowercase=True)
#
tokenized = regexTokenizer.transform(df4)
tokenized.printSchema()
tokenized.coalesce(1).write.json(output_file2)
# Tokenize NON-Fraud-LABEL
# hash the message de-duplicate those records
notfraud_file=sqlContext.read.json(input_file3).repartition(500)
notfraud_file.printSchema()
#
notfraud_df=notfraud_file.filter("message IS NOT NULL").filter("words IS NOT NULL").withColumn('fraud_label',lit(0).cast('int')).withColumn('hash_message',F.sha2(col('message'),512)).groupby(col('hash_message')).agg(F.first(col('fraud_label')).alias('fraud_label'),F.first(col('words')).alias('words'),F.first(col('message')).alias('message')).persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
notfraud_df.printSchema()
# Only the Not-Fraud are randomly sorted
#
from pyspark.sql.functions import rand
#
df_notfraud_words = notfraud_df.filter("message IS NOT NULL").select(col('fraud_label'),col('hash_message'),col('words')).orderBy(rand()).persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
df_notfraud_words.printSchema()
#
# FILTER FRAUD AND LABEL 
# Join with Internal Curation Data in urltopredict staged folder
# hash the message de-duplicate those records
fraud_file=sqlContext.read.json(input_file1_playback_fraud).repartition(500)
fraud_file.printSchema()
#
fraud_df=fraud_file.filter("message IS NOT NULL").filter("words IS NOT NULL").withColumn('fraud_label',lit(1).cast('int')).withColumn('hash_message',F.sha2(col('message'),512)).groupby(col('hash_message')).agg(F.first(col('fraud_label')).alias('fraud_label'),F.first(col('words')).alias('words'),F.first(col('message')).alias('message')).persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
fraud_df.printSchema()
#
df_words = fraud_df.filter("message IS NOT NULL").select(col('fraud_label'),col('hash_message'),col('words')).persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
df_words.printSchema()
#
# Limit to 250,000 Daily Not-Fraud Records input in the nGrams Graph analysis
#
result_fraud_nofraud_words = df_words.union(df_notfraud_words).limit(150000).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
## Register Generic Functions
# -----------------------------------------------------------------------------
# Build ngrams 75 90 n=6 
# support : https://stackoverflow.com/questions/51473703/pyspark-ml-ngrams-countvectorizer-sorted-based-on-count-weights
# -----------------------------------------------------------------------------
def build_ngrams_part(inputCol="words", n=6):
    ngrams = [ 
        NGram(n=i, inputCol="words", outputCol="ngrams_{0}".format(i)) 
        for i in range(7, n + 1) ]
    vectorizers = [ 
        CountVectorizer(inputCol="ngrams_{0}".format(i), outputCol="ngramscounts_{0}".format(i)) 
        for i in range(7, n + 1) ]
    return Pipeline(stages=ngrams + vectorizers)
#    assembler = [VectorAssembler( inputCols=["ngramscounts_{0}".format(i) for i in range(1, n + 1)], outputCol="features" )]
#    return Pipeline(stages=ngrams + DenseVector(SparseVector(vectorizers).toArray()))
#
# 
# -----------------------------------------------------------------------------
#ngram = build_ngrams_part().fit(df_words)
#ngramDataFrame = ngram.transform(df_words)
#ngramDataFrame.coalesce(1).write.json(output_file1)
#
ngram = NGram(n=7, inputCol="words", outputCol="ngrams_7")
countvector = CountVectorizer(inputCol="ngrams_7", outputCol="ngramscounts_7")
# fit a CountVectorizerModel from the corpus.
countvModel = CountVectorizer(inputCol="words", outputCol="features_85", vocabSize=85, minDF=2.0)
# fit a PCA Dimensionality reduction into 7/3=2.x components from ngramscounts_7 ## Too Heavy 1st PCA
pcaNgrams = PCA(k=3, inputCol="ngramscounts_7", outputCol="pcaweightngrams")
# fit a PCA Dimensionality reduction into 85/17=5 components from words
pcaWords = PCA(k=5, inputCol="features_85", outputCol="pcaweightwords")  ## Too Heavy 2nd PCA
#
ngram_fraud_DF = ngram.transform(result_fraud_nofraud_words)
ngram_vc_fraud_DF = countvector.fit(ngram_fraud_DF).transform(ngram_fraud_DF).persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
ngram_vc_fraud_DF.printSchema()
#
# Trial of PCA now with Less Data
#modelPCA_ngram_fraud_DF = pcaNgrams.fit(ngram_vc_fraud_DF).transform(ngram_vc_fraud_DF)\
#.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
#modelPCA_ngram_fraud_DF.printSchema()
##
############# ISSUE - NGRAMS ARE TOO BIG ############
#Traceback (most recent call last):
#  File "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/python/pyspark/sql/utils.py", line 63, in deco
#    return f(*a, **kw)
#  File "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/python/lib/py4j-0.10.6-src.zip/py4j/protocol.py", line 320, in get_return_value
#py4j.protocol.Py4JJavaError: An error occurred while calling o200.fit.
#: java.lang.IllegalArgumentException: Argument with more than 65535 cols: 262144
#	at org.apache.spark.mllib.linalg.distributed.RowMatrix.checkNumColumns(RowMatrix.scala:133)
#	at org.apache.spark.mllib.linalg.distributed.RowMatrix.computeCovariance(RowMatrix.scala:332)
#	at org.apache.spark.mllib.linalg.distributed.RowMatrix.computePrincipalComponentsAndExplainedVariance(RowMatrix.scala:387)
#	at org.apache.spark.mllib.feature.PCA.fit(PCA.scala:53)
#	at org.apache.spark.ml.feature.PCA.fit(PCA.scala:99)
#	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
#	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
#	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
#	at java.lang.reflect.Method.invoke(Method.java:498)
#	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
#	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
#	at py4j.Gateway.invoke(Gateway.java:282)
#	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
#	at py4j.commands.CallCommand.execute(CallCommand.java:79)
#	at py4j.GatewayConnection.run(GatewayConnection.java:214)
#	at java.lang.Thread.run(Thread.java:748)
#
#
#During handling of the above exception, another exception occurred:
#
#Traceback (most recent call last):
#  File "/home/siemanalyst/projects/logs-archive-production/fraud-canada-tokenizedwords/notebooks/x1-FraudCanada-Model-NGrams-CountVectorizer-KL-KS-Entropy-Model-CleanData.py", line 173, in <module>
#####################################################
#
#result_ngrams_words_fraud_DF = countvModel.fit(modelPCA_ngram_fraud_DF).transform(modelPCA_ngram_fraud_DF)\
result_ngrams_words_fraud_DF = countvModel.fit(ngram_vc_fraud_DF).transform(ngram_vc_fraud_DF).persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
result_ngrams_words_fraud_DF.printSchema()
#
#modelPCA_features_ngram_fraud_DF = pcaWords.fit(result_ngrams_words_fraud_DF).transform(result_ngrams_words_fraud_DF)\
#.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
#modelPCA_features_ngram_fraud_DF.printSchema()
#
#modelPCA_features_ngram_fraud_DF.coalesce(1).write.json(output_file1)
result_ngrams_words_fraud_DF.coalesce(1).write.json(output_file1)
#
print("Calculation of most frequent fraud_ngram notfraud_ngram - Start!")
#
#  CALCULATE KL,KS COEF. Label Data fraud/not_fraud
###################
# Obtain the most frequent word on each position 
# Compose the standard_fraud_ngram from the most common positions
# Calculate the standard_fraud_ngram
#
# https://stackoverflow.com/questions/35218882/find-maximum-row-per-group-in-spark-dataframe 
# Using struct ordering:
#from pyspark.sql.functions import struct
#
#(cnts
#  .groupBy("id_sa")
#  .agg(F.max(struct(col("cnt"), col("id_sb"))).alias("max"))
#  .select(col("id_sa"), col("max.id_sb")))
#
####################
# FRAUD
ngram7_fraud=sqlContext.read.json(input_file4).filter("fraud_label=1")
ngram7_fraud.printSchema()
#
most_frequent_fraud_df=ngram7_fraud.withColumn("value_sum",F.explode("ngramscounts_7.values")).groupBy("hash_message").agg(F.sum("value_sum").alias('count')).persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
most_frequent_fraud_df.printSchema()
#
most_frequent_fraud_df.coalesce(1).write.json(output_most_frequent_fraud_df)
#
# The most Frequent would the the max
standard_fraud_ngram=most_frequent_fraud_df.orderBy(col('count').desc()).select(col('hash_message')).limit(1).toPandas()
#
print("Value Print: standard_fraud_ngram=")
print(standard_fraud_ngram)
#
####################
# NOT FRAUD
ngram7_notfraud=sqlContext.read.json(input_file4).filter("fraud_label=0")
ngram7_notfraud.printSchema()
#
most_frequent_notfraud_df=ngram7_notfraud.withColumn("value_sum",F.explode("ngramscounts_7.values")).groupBy("hash_message").agg(F.sum("value_sum").alias('count')).persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
most_frequent_notfraud_df.printSchema()
#
most_frequent_notfraud_df.coalesce(1).write.json(output_most_frequent_notfraud_df)
#
# The most Frequent would the the max
standard_notfraud_ngram=most_frequent_notfraud_df.orderBy(col('count').desc()).select(col('hash_message')).limit(1).toPandas()
#
print("Value Print: standard_notfraud_ngram=")
print(standard_notfraud_ngram)
#
print("Calculation of most frequent fraud_ngram notfraud_ngram - Done!")
#
sc.stop()
#
print("Preparation of Data Done!")
#


# In[4]:


sc.stop()


# In[8]:


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
#import argparse
## Parse date_of execution
#parser = argparse.ArgumentParser()
#parser.add_argument("--datev1", help="Execution Date")
#args = parser.parse_args()
#if args.datev1:
#    processdate = args.datev1
# GENERAL PREPARATION SCRIPT
#
#  Date in format YYYYMMDD
#process_date = processdate
#if not process_date:
#    process_date = "20190122"
#
process_date = "20190206"
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
standard_fraud_ngram=sqlContext.read.json(input_most_frequent_fraud_df).orderBy(col('count').desc()).select(col('hash_message')).limit(1).toPandas()['hash_message'][0]
print("Value UDF : standard_fraud_ngram=")
print(standard_fraud_ngram)
#
# Select Tokens/words form the max frequency of NGrams85 tokens hash_message
standard_fraud_ngram_words=sqlContext.read.json(input_fraud)
standard_fraud_ngram_words.printSchema()
#
standard_fraud_words_search=standard_fraud_ngram_words.withColumn('fraud_master_hash',lit(standard_fraud_ngram).cast('string')).filter(" hash_message=fraud_master_hash ")
standard_fraud_words_search.printSchema()
#
standard_fraud_words=standard_fraud_words_search.withColumn('words_conc',F.concat_ws('',col('words')).cast('string')).select(col('words_conc')).limit(1).toPandas()['words_conc'][0] 
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
standard_notfraud_ngram=sqlContext.read.json(input_most_frequent_notfraud_df).orderBy(col('count').desc()).select(col('hash_message')).limit(1).toPandas()['hash_message'][0]
print("Value UDF : standard_notfraud_ngram=")
print(standard_notfraud_ngram)
#
# Select Tokens/words form the max frequency of NGrams85 tokens hash_message
standard_notfraud_ngram_words=sqlContext.read.json(input_fraud)
standard_notfraud_ngram_words.printSchema()
#
standard_notfraud_words_search=standard_notfraud_ngram_words.withColumn('fraud_master_hash',lit(standard_notfraud_ngram).cast('string')).filter(" hash_message=fraud_master_hash ")
standard_notfraud_words_search.printSchema()
#
standard_notfraud_words=standard_notfraud_words_search.withColumn('words_conc',F.concat_ws('',col('words')).cast('string')).select(col('words_conc')).limit(1).toPandas()['words_conc'][0] 
#
print("Value UDF : standard_notfraud_words=")
print(standard_notfraud_words)
#
ngram7_fraud=sqlContext.read.json(input_file1)
ngram7_fraud.printSchema()
#
drop_phish_cols=['words','ngrams_7']
#
fraud_label_read_df=ngram7_fraud.filter("hash_message is not NULL").withColumn('words_conc',F.concat_ws('',col('words')).cast('string')).drop(*drop_phish_cols).withColumn('kl_fraud_words',func_kl_ngram_msg_udf(col('words_conc'),lit(standard_fraud_words).cast('string')).cast('double')).withColumn('ks_fraud_words',func_ks_ngram_msg_udf(col('words_conc'),lit(standard_fraud_words).cast('string')).cast('double')).withColumn('entropy_fraud_words',func_entropy_ngram_msg_udf(col('words_conc'),lit(standard_fraud_words).cast('string')).cast('double')).withColumn('kl_notfraud_words',func_kl_ngram_msg_udf(col('words_conc'),lit(standard_notfraud_words).cast('string')).cast('double')).withColumn('ks_notfraud_words',func_ks_ngram_msg_udf(col('words_conc'),lit(standard_notfraud_words).cast('string')).cast('double')).withColumn('entropy_notfraud_words',func_entropy_ngram_msg_udf(col('words_conc'),lit(standard_notfraud_words).cast('string')).cast('double')).persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
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


# In[7]:


sc.stop()


# In[9]:


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
#
# Arguments
#
import argparse
## Parse date_of execution
#parser = argparse.ArgumentParser()
#parser.add_argument("--datev1", help="Execution Date")
#args = parser.parse_args()
#if args.datev1:
#    processdate = args.datev1
# GENERAL PREPARATION SCRIPT
#
#  Date in format YYYYMMDD
#process_date = processdate
#if not process_date:
#    process_date = "20181231"
#
process_date="20190206"
#
sc = pyspark.SparkContext(appName="FraudCanada-AUTOML-Model-NGrams-CountVectorizer-KL-KS-Entropy")
sqlContext = SQLContext(sc)
#
input_most_frequent_df="hdfs:///data/staged/ott_dazn/advanced-model-data/the-most-frequent-fraud-hash_message/dt="+process_date
input_fraud="hdfs:///data/staged/ott_dazn/advanced-model-data/fraud-notfraud-canada-tokenizedwords-ngrams-5-features-85/dt="+process_date
#
input_file1="hdfs:///data/staged/ott_dazn/advanced-model-data/fraud-notfraud-canada-tokenizedwords-ngrams-5-features-85/dt="+process_date
output_file1="hdfs:///data/staged/ott_dazn/advanced-model-data/label-fraud-notfraud-data-model/dt="+process_date
preserve_training_input_file="hdfs:///data/staged/ott_dazn/advanced-model-data/preserve-training-output-automl-clean/dt="+process_date
#
import h2o
from h2o.automl import H2OAutoML
#
import subprocess
subprocess.run('unset http_proxy', shell=True)
#
# Start an H2O virtual cluster that uses 6 gigs of RAM and 6 cores
h2o.init(ip="localhost",port=54321,max_mem_size = "6g", nthreads = 6) 
#
# Clean up the h2o cluster just in case
h2o.remove_all()
#
#  TRAINING PROCESS
#
print("Start Training Model NGrams Vectors KS KL Entropty")
#
# Horrible code :: close your eyes, is ugly
#
fraud_label_read_file=sqlContext.read.json(output_file1)
fraud_label_read_file.printSchema()
#
fraud_label_read_df=fraud_label_read_file.select(col('hash_message').cast('string'),col('fraud_label').cast('int'),        col('kl_fraud_words').cast('double'),col('ks_fraud_words').cast('double'),        col('entropy_fraud_words').cast('double'),        col('kl_notfraud_words').cast('double'), col('ks_notfraud_words').cast('double'),        col('entropy_notfraud_words').cast('double'),        col('features_85.type').alias('features85_type').cast('long'),        col('features_85.size').alias('features85_size').cast('long'),        col('features_85.indices').alias('features85_indices'),        col('features_85.values').alias('features85_values'),        col('ngramscounts_7.type').alias('ngramscounts7_type').cast('long'),        col('ngramscounts_7.size').alias('ngramscounts7_size').cast('long'),        col('ngramscounts_7.indices').alias('ngramscounts7_indices'),        col('ngramscounts_7.values').alias('ngramscounts7_values'))
fraud_label_read_df.printSchema()
#
# ABOVE ARE CASE ISSUES on struct Struct of features_85 and ngramscounts_7 
# Both cares conversion to DF valide type list
# Flat vars for each, individually and seperately from the original struct
#
# https://stackoverflow.com/questions/47401418/pyspark-conversion-to-array-types?rq=1 
#
#
fraud_fraud_label_read1_df=fraud_label_read_df.filter("fraud_label=1").persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
notfraud_fraud_label_read1_df=fraud_label_read_df.filter("fraud_label=0").persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
#
fraud_fraud_label_read1_df.printSchema()
notfraud_fraud_label_read1_df.printSchema()
#
drop_list_cols=['features85_indices','features85_values','ngramscounts7_indices','ngramscounts7_values']
#
### 1.) https://stackoverflow.com/questions/38610559/convert-spark-dataframe-column-to-python-list
###    list(spark_df.select('mvv').toPandas()['mvv'])
### 2.) http://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.assign.html
###    df.assign(temp_f=lambda x: x['temp_c'] * 9 / 5 + 32,temp_k=lambda x: (x['temp_f'] +  459.67) * 5 / 9)
### 3.) https://stackoverflow.com/questions/43216411/pandas-flatten-a-list-of-list-within-a-column
###    df['var2'] = df['var2'].apply(np.ravel)
### 4.) Random xxx rows
###    df.orderBy(rand()).limit(n)
from pyspark.sql.functions import rand
#
fraud_label_train_pd_rand=fraud_fraud_label_read1_df.limit(100000).orderBy(rand()).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
#
fraud_label_train_pd=fraud_label_train_pd_rand.orderBy(col('kl_fraud_words')).limit(800).union( fraud_label_train_pd_rand.orderBy(col('kl_fraud_words').desc()).limit(500)).toPandas().assign(features85_list_indices=lambda x: x['features85_indices'].apply(np.ravel),        features85_list_values=lambda x: x['features85_values'].apply(np.ravel),        ngramscounts7_list_indices=lambda x: x['ngramscounts7_indices'].apply(np.ravel),        ngramscounts7_list_values=lambda x: x['ngramscounts7_values'].apply(np.ravel)).drop(drop_list_cols, axis=1, inplace=False)
#
fraud_label_test_pd_rand=fraud_fraud_label_read1_df.orderBy(col('kl_fraud_words')).limit(100000).orderBy(rand()).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
#
fraud_label_test_pd=fraud_label_test_pd_rand.orderBy(col('kl_fraud_words')).limit(500).union(fraud_label_test_pd_rand.orderBy(col('kl_fraud_words').desc()).limit(300)).toPandas().assign(features85_list_indices=lambda x: x['features85_indices'].apply(np.ravel),        features85_list_values=lambda x: x['features85_values'].apply(np.ravel),        ngramscounts7_list_indices=lambda x: x['ngramscounts7_indices'].apply(np.ravel),        ngramscounts7_list_values=lambda x: x['ngramscounts7_values'].apply(np.ravel)).drop(drop_list_cols, axis=1, inplace=False)
#
fraud_label_train=h2o.H2OFrame(fraud_label_train_pd)
fraud_label_test=h2o.H2OFrame(fraud_label_test_pd)
#
not_fraud_label_train_pd_rand=notfraud_fraud_label_read1_df.limit(100000).orderBy(rand()).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
#
not_fraud_label_train_pd=not_fraud_label_train_pd_rand.orderBy(col('kl_notfraud_words')).limit(1600).union(not_fraud_label_train_pd_rand.orderBy(col('kl_notfraud_words').desc()).limit(1400)).toPandas().assign(features85_list_indices=lambda x: x['features85_indices'].apply(np.ravel),        features85_list_values=lambda x: x['features85_values'].apply(np.ravel),        ngramscounts7_list_indices=lambda x: x['ngramscounts7_indices'].apply(np.ravel),        ngramscounts7_list_values=lambda x: x['ngramscounts7_values'].apply(np.ravel)).drop(drop_list_cols, axis=1, inplace=False)
#
not_fraud_label_test_pd_rand=notfraud_fraud_label_read1_df.limit(100000).orderBy(rand()).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
#
not_fraud_label_test_pd=not_fraud_label_test_pd_rand.orderBy(col('kl_notfraud_words')).limit(500).union(not_fraud_label_test_pd_rand.orderBy(col('kl_notfraud_words').desc()).limit(300)).toPandas().assign(features85_list_indices=lambda x: x['features85_indices'].apply(np.ravel),        features85_list_values=lambda x: x['features85_values'].apply(np.ravel),        ngramscounts7_list_indices=lambda x: x['ngramscounts7_indices'].apply(np.ravel),        ngramscounts7_list_values=lambda x: x['ngramscounts7_values'].apply(np.ravel)).drop(drop_list_cols, axis=1, inplace=False)
#.orderBy(rand())\
#.sort(notfraud_fraud_label_read1_df.kl_notfraud_words.desc())\
#
not_fraud_label_train=h2o.H2OFrame(not_fraud_label_train_pd)
not_fraud_label_test=h2o.H2OFrame(not_fraud_label_test_pd)
#
################# Use Two DataFrames ##################### - rbind() H2o Frames issue
#
#
###### TRAINING PROCESS ############
# RBIND "Merge" all of vars internal subset of data with fraud and with not_fraud
# function merge() doesn't work if both H2O/dataframes have same variables
#
train = fraud_label_train.rbind(not_fraud_label_train)
test = fraud_label_test.rbind(not_fraud_label_test)
#
# Clear Cache from Spark Context as 
# From here all operations are with H2ODataFrame and H2o.ai Context
sqlContext.clearCache()
#
print("train")
print(train.head(10))
print("test")
print(test.head(10))
#
# Identify predictors and response
x = train.columns
#
# Fraud Label to be learned in the model from the atrributes of the ngram85 learned words
#
y= 'fraud_label'
x.remove(y)
#
# For binary classification, response should be a factor
train[y] = train[y].asfactor()
test[y] = test[y].asfactor()
#
# http://docs.h2o.ai/h2o/latest-stable/h2o-docs/automl.html
# Balance Classes to compensate unbalanced data
# Run AutoML for 25 base models (limited to 1 hour max runtime by default)
aml = H2OAutoML(max_models=30, seed=1999, exclude_algos=["DRF","GLM"])
aml.train(x=x, y=y, training_frame=train)
#
#preserve_training_output.write.json(preserve_training_output_file)
#
print("AutoML Modeling Done!")
#
# View the AutoML Leaderboard
lb = aml.leaderboard
lb.head(rows=lb.nrows)  # Print all rows instead of default (10 rows)
#
# The leader model is stored here
aml.leader
#
# Get model ids for all models in the AutoML Leaderboard
model_ids = list(aml.leaderboard['model_id'].as_data_frame().iloc[:,0])
print(model_ids)
# Get the "All Models" Stacked Ensemble model
se = h2o.get_model([mid for mid in model_ids if "StackedEnsemble_AllModels" in mid][0])
print(se)
# Get the Stacked Ensemble metalearner model
#metalearner = h2o.get_model(aml.leader.metalearner()['name'])
#metalearner.coef_norm()
#%matplotlib inline
#metalearner.std_coef_plot()
# If you need to generate predictions on a test set, you can make
# predictions directly on the `"H2OAutoML"` object, or on the leader
# model object directly

#preds = aml.predict(test)
# or:
preds = aml.leader.predict(test)
print("test")
print(test.head(10))
print("prediction")
print(preds.head(10))
#
#
print("Save Model For Future Usage")
aml.leader.download_mojo(path = "./projects/logs-archive-production/fraud-canada-tokenizedwords/notebooks/product_model_bin/ngrams7_features85_m30/v"+process_date+"/mojo", get_genmodel_jar = True)
# If you need to generate predictions on a test set, you can make
# predictions directly on the `"H2OAutoML"` object, or on the leader
# model object directly

#preds = aml.predict(test)
# or:
preds = aml.leader.predict(test)
print("test")
print(test.tail(10))
print("prediction")
print(preds.tail(10))
# If you need to generate predictions on a test set, you can make
# predictions directly on the `"H2OAutoML"` object, or on the leader
# model object directly

#preds = aml.predict(test)
# or:
preds_over_all_hf = aml.leader.predict(train)
path_out_file1="./projects/logs-archive-production/fraud-canada-tokenizedwords/notebooks/product_model_prediction/file_prediction_"+process_date+".csv"
output_pred_file=h2o.export_file(frame=preds_over_all_hf, path=path_out_file1, force=False)
#
train_over_all_hf = train
path_out_file2="./projects/logs-archive-production/fraud-canada-tokenizedwords/notebooks/product_model_prediction/file_train_"+process_date+".csv"
output_pred_file=h2o.export_file(frame=train_over_all_hf, path=path_out_file2, force=False)
#
sc.stop()
#


# In[10]:


sc.stop()


# In[11]:


# View the AutoML Leaderboard
lb = aml.leaderboard
lb.head(rows=lb.nrows)  # Print all rows instead of default (10 rows)


# In[12]:


# The leader model is stored here
aml.leader


# In[13]:


# If you need to generate predictions on a test set, you can make
# predictions directly on the `"H2OAutoML"` object, or on the leader
# model object directly

#preds = aml.predict(test)
# or:
preds = aml.leader.predict(test)
print("test")
print(test.head(10))
print("prediction")
print(preds.head(10))


# In[14]:


# Get model ids for all models in the AutoML Leaderboard
model_ids = list(aml.leaderboard['model_id'].as_data_frame().iloc[:,0])

print(model_ids)
# Get the "All Models" Stacked Ensemble model
se = h2o.get_model([mid for mid in model_ids if "GBM" in mid][0])
print(se)

# Get the Stacked Ensemble metalearner model
metalearner = h2o.get_model(aml.leader.metalearner()['name'])
metalearner.coef_norm()


# In[51]:


get_ipython().run_line_magic('matplotlib', 'inline')
metalearner.std_coef_plot()


# In[15]:


aml.leader.download_mojo(path = "./product_model_bin/ngrams7_features85_vectors_m25/v8/mojo", get_genmodel_jar = True)


# In[25]:


sc.stop()


# In[16]:


# If you need to generate predictions on a test set, you can make
# predictions directly on the `"H2OAutoML"` object, or on the leader
# model object directly

#preds = aml.predict(test)
# or:
preds = aml.leader.predict(test)
print("test")
print(test.tail(10))
print("prediction")
print(preds.tail(10))


# In[19]:


#preds = aml.predict(test)
# or:
preds = aml.leader.predict(test)
print("test")
print(test.tail(10))
print("prediction")
print(preds.tail(10))
# If you need to generate predictions on a test set, you can make
# predictions directly on the `"H2OAutoML"` object, or on the leader
# model object directly

#preds = aml.predict(test)
# or:
preds_over_all_hf = aml.leader.predict(train)
path_out_file1="product_model_prediction/file_prediction_"+process_date+".csv"
output_pred_file=h2o.export_file(frame=preds_over_all_hf, path=path_out_file1, force=False)
#
train_over_all_hf = train
path_out_file2="product_model_prediction/file_train_"+process_date+".csv"
output_pred_file=h2o.export_file(frame=train_over_all_hf, path=path_out_file2, force=False)
#


# In[22]:


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
# monotonically_increasing_id allows to use join to merge save size DF
# https://forums.databricks.com/questions/8180/how-to-merge-two-data-frames-column-wise-in-apache.html
from pyspark.sql.functions import monotonically_increasing_id
#
#  FILTER with PySpark SQL Functions F.
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

def match_func(predict, fraud_label):
    if predict != fraud_label:
        return "no_match"
    else:
        return "match"
match_udf = udf(match_func, StringType())    
#match_udf = udf(lambda (prediction,fraud_label): "no_match" if prediction!=fraud_label else "match", StringType())

#
# Arguments
#
import argparse
## Parse date_of execution
#parser = argparse.ArgumentParser()
#parser.add_argument("--datev1", help="Execution Date")
#args = parser.parse_args()
#if args.datev1:
#    processdate = args.datev1
# GENERAL PREPARATION SCRIPT
#
#  Date in format YYYYMMDD
#process_date = processdate
#if not process_date:
#    process_date = "20181231"
#
process_date = "20190206"
#
sc = pyspark.SparkContext(appName="FraudCanada-Merge-Scoring-Data-From-Model")
sqlContext = SQLContext(sc)
#
input_file_prediction_df="hdfs:///data/staged/ott_dazn/advanced-model-data/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85//dt="+process_date+"/file_prediction_"+process_date+".csv"
#
input_file_train_df="hdfs:///data/staged/ott_dazn/advanced-model-data/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85//dt="+process_date+"/file_train_"+process_date+".csv"
#
output_file1="hdfs:///data/staged/ott_dazn/advanced-model-data/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85//dt="+process_date+"/merged_prediction_train"
#
prediction_df=sqlContext.read.csv(input_file_prediction_df,header=True,inferSchema=True)
prediction_df = prediction_df.withColumn("id1", monotonically_increasing_id())
prediction_df.printSchema()
#
train_df=sqlContext.read.csv(input_file_train_df,header=True,inferSchema=True,multiLine=True)
train_df = train_df.withColumn("id1", monotonically_increasing_id())
train_df.printSchema()
#
merged_prediction_train_df3 = prediction_df.join(train_df, "id1", "outer") #.drop("id1")
#
merged_prediction_train_df3.withColumn("match_no_match",match_udf(col('predict').cast('int'),col('fraud_label').cast('int'))).sort(merged_prediction_train_df3.id1.desc()).coalesce(1).write.csv(output_file1,header=True)
#
sc.stop()
#


# In[21]:


sc.stop()


# In[26]:


#
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
# Arguments
#
import argparse
## Parse date_of execution
#parser = argparse.ArgumentParser()
#parser.add_argument("--datev1", help="Execution Date")
#args = parser.parse_args()
#if args.datev1:
#    processdate = args.datev1
# GENERAL PREPARATION SCRIPT
#
#  Date in format YYYYMMDD
#process_date = processdate
#if not process_date:
#    process_date = "20190113"
#
process_date = "20190206"
#
sc = pyspark.SparkContext(appName="FraudCanada-Search-Suspitious-hashmessage-matching_words-By-AUTOML-NGrams-CountVectorizer-KL-KS-Entropy")
sqlContext = SQLContext(sc)
#
input_fraud="hdfs:///data/staged/ott_dazn/advanced-model-data/fraud-notfraud-canada-tokenizedwords-ngrams-7-features-85/dt="+process_date
#
input_merged_prediction_train="hdfs:///data/staged/ott_dazn/advanced-model-data/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt="+process_date+"/merged_prediction_train/merge_prediction_train_"+process_date+".csv"
#
output_no_match="hdfs:///data/staged/ott_dazn/advanced-model-data/scoring-fraud-notfraud-kl-ks-entropy-ngrams7-features-85/dt="+process_date+"/no_match_hash_message"
#
# The most Frequent would the ones with the max frequency of NGrams85 tokens
pd.options.display.max_colwidth = 512
#
#
# Select Tokens/words form the max frequency of NGrams85 tokens hash_message
notfraud_df=sqlContext.read.json(input_fraud).select(col('hash_message'),col('words'))
notfraud_df.printSchema()
#
merged_df=sqlContext.read.csv(input_merged_prediction_train,header=True,inferSchema=True,multiLine=True).filter(" match_no_match = 'no_match' ")
merged_df.printSchema()
#
join_results_df=merged_df.join(notfraud_df, "hash_message" ).withColumn('words_conc',F.concat_ws('',col('words')).cast('string')).drop('words').persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
join_results_df.printSchema()
#
join_results_df.coalesce(1).write.csv(output_no_match,header=True)
#
### Argument from Merge_train_predict_YYYYMMDD.csv
#        #47bade7fcc6883b26ff81f0681ff8b824d97718c1f9d4584c66f2d58b925f4da9447f6215c62293565820492f7499efea8aed25ef1bf5ddeb36b3872d4ad243d
#var_hash_message='47bade7fcc6883b26ff81f0681ff8b824d97718c1f9d4584c66f2d58b925f4da9447f6215c62293565820492f7499efea8aed25ef1bf5ddeb36b3872d4ad243d'
# Select Tokens/words form the max frequency of NGrams85 tokens hash_message
#standard_notfraud_ngram_words=sqlContext.read.json(input_fraud)
#standard_notfraud_ngram_words.printSchema()
#
#standard_notfraud_words_search=standard_notfraud_ngram_words\
#.withColumn('fraud_search_hash',lit(var_hash_message).cast('string'))\
#.filter(" hash_message=fraud_search_hash ")
#standard_notfraud_words_search.printSchema()
#
#standard_notfraud_words=standard_notfraud_words_search\
#.withColumn('words_conc',F.concat_ws('',col('words')).cast('string'))\
#.select(col('words_conc')).limit(1).toPandas()['words_conc'][0] 
#
#print("From Identifyed Label=0, prediction p=1 : var_hash_message=")
#print(var_hash_message)
#print("Potential fraud value FROM : standard_notfraud_words=")
#print(standard_notfraud_words)
#
sc.stop()


# In[25]:


sc.stop()

