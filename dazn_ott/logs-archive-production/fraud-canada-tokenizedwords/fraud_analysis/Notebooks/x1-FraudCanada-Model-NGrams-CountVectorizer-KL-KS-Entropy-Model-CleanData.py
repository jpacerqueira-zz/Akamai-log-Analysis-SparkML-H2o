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
#process_date="20190122"
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
notfraud_df=notfraud_file\
.filter("message IS NOT NULL").filter("words IS NOT NULL")\
.withColumn('fraud_label',lit(0).cast('int'))\
.withColumn('hash_message',F.sha2(col('message'),512)).groupby(col('hash_message'))\
.agg(F.first(col('fraud_label')).alias('fraud_label'),F.first(col('words')).alias('words'),F.first(col('message')).alias('message'))\
.orderBy(rand())\
.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
notfraud_df.printSchema()
# Only the Not-Fraud are randomly sorted
#
from pyspark.sql.functions import rand
#
df_notfraud_words = notfraud_df.filter("message IS NOT NULL").select(col('fraud_label'),col('hash_message'),col('words'))\
.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
df_notfraud_words.printSchema()
#
# FILTER FRAUD AND LABEL 
# Join with Internal Curation Data in urltopredict staged folder
# hash the message de-duplicate those records
fraud_file=sqlContext.read.json(input_file1_playback_fraud).repartition(500)
fraud_file.printSchema()
#
fraud_df=fraud_file\
.filter("message IS NOT NULL").filter("words IS NOT NULL")\
.withColumn('fraud_label',lit(1).cast('int'))\
.withColumn('hash_message',F.sha2(col('message'),512)).groupby(col('hash_message'))\
.agg(F.first(col('fraud_label')).alias('fraud_label'),F.first(col('words')).alias('words'),F.first(col('message')).alias('message'))\
.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
fraud_df.printSchema()
#
df_words = fraud_df.filter("message IS NOT NULL").select(col('fraud_label'),col('hash_message'),col('words'))\
.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
df_words.printSchema()
#
# Limit to 250,000 Daily Not-Fraud Records input in the nGrams Graph analysis
# As the limit of Ngrams vectors is 264k "ngramscounts_7":{"type":0,"size":262144 ....
#
result_fraud_nofraud_words = df_words.union(df_notfraud_words).limit(250000)\
.persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
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
ngram_vc_fraud_DF = countvector.fit(ngram_fraud_DF).transform(ngram_fraud_DF)\
.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
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
result_ngrams_words_fraud_DF = countvModel.fit(ngram_vc_fraud_DF).transform(ngram_vc_fraud_DF)\
.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
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
most_frequent_fraud_df=ngram7_fraud\
.withColumn("value_sum",F.explode("ngramscounts_7.values"))\
.groupBy("hash_message").agg(F.sum("value_sum").alias('count'))\
.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
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
most_frequent_notfraud_df=ngram7_notfraud\
.withColumn("value_sum",F.explode("ngramscounts_7.values"))\
.groupBy("hash_message").agg(F.sum("value_sum").alias('count'))\
.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
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