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
    process_date = "20190113"
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
notfraud_df=sqlContext.read.json(input_fraud)\
.select(col('hash_message'),col('words'))
notfraud_df.printSchema()
#
# Filter only the "'no_match_predict_fraud'"
#
merged_df=sqlContext.read.csv(input_merged_prediction_train,header=True,inferSchema=True,multiLine=True)\
.filter(" match_no_match = 'no_match_predict_fraud' ")
merged_df.printSchema()
#
join_results_df=merged_df.join(notfraud_df, "hash_message" )\
.withColumn('words_conc',F.concat_ws('',col('words')).cast('string')).drop('words')\
.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
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