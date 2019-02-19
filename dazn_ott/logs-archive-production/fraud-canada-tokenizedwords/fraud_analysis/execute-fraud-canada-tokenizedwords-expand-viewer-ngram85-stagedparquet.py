# /usr/env/spark2-submit
import sys
from random import random
from operator import add
#
import re
import pandas as pd
import numpy as np
from pandas import DataFrame
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
#
import pyspark
from pyspark.sql import functions as pfunc
from pyspark.sql import SQLContext
from pyspark.sql import Window, types
#
#import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import RegexTokenizer
#
#import org.apache.spark.ml.feature.NGram
from pyspark.ml.feature import NGram
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
    process_date = "20181231"
#
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Execute SparkSession Fraud-canada-tokenizedwords-ngram85.py
#
# Last 8 tokes identifies ViwerID, DeviceID and User Status
#
def last_8_tokens(words):
    return ''.join(words[-8:])
#       
last_8_tokens_udf = udf(last_8_tokens, StringType())  
#
# DeviceID via 3 tokens
#
def match_deviceid_3_tokens(words):
    return ''.join(words[-5:-3])
#       
match_deviceid_3_tokens_udf = udf(match_deviceid_3_tokens, StringType())  
# 
#
sc = pyspark.SparkContext(appName="Daily-TokenizedWords-ExpandViewer-Ngram-85-StagedParquet")
sqlContext = SQLContext(sc)
# 
#
#
input_file1="hdfs:///data/staged/ott_dazn/fraud-canada-tokenizedwords/dt="+process_date
#
output_file1="hdfs:///data/staged/ott_dazn/fraud-canada-tokenizedwords/dt="+process_date
#
input_file2="hdfs:///data/staged/ott_dazn/logs-archive-production/parquet/dt="+process_date+"/*.parquet"
#
df1=sqlContext.read.json(input_file1)
count_daily_tokenizer=df1.count()
#
# Close Job if Daily Tokenizer is Empty
if (count_daily_tokenizer == 0):
    sc.stop()
    print("Daily Counter Tokenizer "+str(count_daily_tokenizer))
else :
    print("Daily Counter Tokenizer "+str(count_daily_tokenizer))
#
df2=sqlContext.read.parquet(input_file2)
df2.printSchema()
#
df3 = df2.filter(df2.message.contains('\"Url\":\"https://isl-ca.dazn.com/misl/v2/Playback'))\
    .filter(df2.message.contains(',\"Response\":{\"StatusCode\":200,\"ReasonPhrase\":\"OK\",'))
df3.printSchema()
df4 = df3.withColumn("messagecut", expr("substring(message, locate('|Livesport.WebApi.Controllers.Playback.PlaybackV2Controller|',message)+60 , length(message)-1)"))
#
#val regexTokenizer = new RegexTokenizer().setInputCol("messagecut").setOutputCol("words").setPattern("\\w+|").setGaps(false)
regexTokenizer = RegexTokenizer(minTokenLength=1, gaps=False, pattern='\\w+|', inputCol="messagecut", outputCol="words", toLowercase=True)
#
## Extract Now DeviceID and not the Last 8 words
## 
tokenized = regexTokenizer.transform(df4)\
.withColumn('match_deviceid_3_tokens',match_deviceid_3_tokens_udf(col('words')))\
.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
#
tokenized.printSchema()
#
tokens_to_match=df1\
.withColumn('match_deviceid_3_tokens',match_deviceid_3_tokens_udf(col('words')))\
.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
#
tokens_to_match.printSchema()
##
### df1.join(df2, df1("col1") === df2("col1"), "left_outer")
##  a LEFT SEMI JOIN will only return one row from the left, even if there are multiple matches in the right. An INNER JOIN will return multiple rows if there are multiple matching on the right
#
new_expand_match=tokenized.join(tokens_to_match, tokenized.match_deviceid_3_tokens == tokens_to_match.match_deviceid_3_tokens , 'leftsemi')\
.select(tokenized.metadata, tokenized.logzio_id, tokenized.beat, tokenized.host, tokenized.it, tokenized.logzio_codec, tokenized.message, tokenized.offset, tokenized.source, tokenized.tags, tokenized.type, tokenized.messagecut , tokenized.words )
#
new_expand_match.printSchema()
new_expand_match.coalesce(1).write.mode('append').json(output_file1)
#
#df5 = sqlContext.read.json(input_file2).filter("message IS NOT NULL")
#
#ngram = NGram(n=85, inputCol="words", outputCol="ngrams")
#
#ngramDataFrame = ngram.transform(df5)
#ngramDataFrame.select("ngrams").coalesce(1).write.json(output_file2)
#
#
sc.stop()
#