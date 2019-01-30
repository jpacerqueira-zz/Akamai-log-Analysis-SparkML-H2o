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
#
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Execute SparkSession Fraud-canada-tokenizedwords-ngram85.py
#
# 
sc = pyspark.SparkContext(appName="Daily-TokenizedWords-Ngram-85-StagedParquet")
sqlContext = SQLContext(sc)
# 
#
#
input_file1="hdfs:///data/staged/ott_dazn/logs-archive-production/parquet/dt="+process_date+"/*.parquet"
output_file1="hdfs:///data/staged/ott_dazn/fraud-canada-tokenizedwords/dt="+process_date
#
#
input_file2="hdfs:///data/staged/ott_dazn/fraud-canada-tokenizedwords/dt="+process_date+"/*.json"
output_file2="hdfs:///data/staged/ott_dazn/fraud-canada-tokenizedwords-ngram-85/dt="+process_date
#
#
df2= sqlContext.read.parquet(input_file1)
df2.printSchema()
#
df3 = df2.filter(df2.message.contains('\"Url\":\"https://isl-ca.dazn.com/misl/v2/Playback'))\
    .filter(df2.message.contains('&Format=MPEG-DASH&'))\
    .filter(df2.message.contains('\"User-Agent\":\"Mozilla/5.0,(Macintosh; Intel Mac OS X 10_12_6),AppleWebKit/605.1.15,(KHTML, like Gecko),Version/11.1.2,Safari/605.1.15\"},'))\
    .filter(df2.message.contains(',\"Response\":{\"StatusCode\":200,\"ReasonPhrase\":\"OK\",'))
df3.printSchema()
df4 = df3.withColumn("messagecut", expr("substring(message, locate('|Livesport.WebApi.Controllers.Playback.PlaybackV2Controller|',message)+60 , length(message)-1)"))
#
#val regexTokenizer = new RegexTokenizer().setInputCol("messagecut").setOutputCol("words").setPattern("\\w+|").setGaps(false)
regexTokenizer = RegexTokenizer(minTokenLength=1, gaps=False, pattern='\\w+|', inputCol="messagecut", outputCol="words", toLowercase=True)
#
tokenized = regexTokenizer.transform(df4)
tokenized.printSchema()
tokenized.coalesce(1).write.json(output_file1)
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