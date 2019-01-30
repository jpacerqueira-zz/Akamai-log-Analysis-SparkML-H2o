spark.sql("USE published_ott_dazn ") ;
spark.sql(" DROP TABLE published_ott_dazn.pair_misldeviceid_count_mislviewerid ") ; 
spark.read.parquet("/data/staged/ott_dazn/pair_misldeviceid_count_mislviewerid/parquet").registerTempTable("pair_misldeviceid_count_mislviewerid_temp") ;
spark.sql("CREATE TABLE published_ott_dazn.pair_misldeviceid_count_mislviewerid USING PARQUET  AS SELECT * FROM pair_misldeviceid_count_mislviewerid_temp ") ;
sys.exit ;
