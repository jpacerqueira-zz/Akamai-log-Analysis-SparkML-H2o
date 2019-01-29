import org.apache.spark.sql.functions._ ;
spark.sql("USE published_ott_dazn"); 
val df2=spark.sql(" select DISTINCT misldeviceid,mislviewerid from published_ott_dazn.viewer_token_investigation where date between '2017-02-01' and '2018-02-06' "); 
df2.printSchema() ; 
val df3=df2.filter(" misldeviceid IS NOT NULL AND mislviewerid IS NOT NULL ").groupBy("misldeviceid").agg(countDistinct(col("mislviewerid")).alias("count_mislviewerid")) ; 
df3.printSchema() ; 
df3.write.mode("append").parquet("/data/staged/ott_dazn/pair_misldeviceid_count_mislviewerid/parquet"); 
val df4=spark.read.parquet("/data/staged/ott_dazn/pair_misldeviceid_count_mislviewerid/parquet").printSchema() ;
sys.exit;
