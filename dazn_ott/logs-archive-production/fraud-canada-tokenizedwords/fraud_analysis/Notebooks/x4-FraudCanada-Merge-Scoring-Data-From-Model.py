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
merged_prediction_train_df3\
.withColumn("match_no_match",match_udf(col('predict').cast('int'),col('fraud_label').cast('int')))\
.sort(merged_prediction_train_df3.id1.desc()).coalesce(1).write.csv(output_file1,header=True)
#
sc.stop()
#