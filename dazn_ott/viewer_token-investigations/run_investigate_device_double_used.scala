spark.sql("USE published_ott_dazn"); spark.sql("select * from published_ott_dazn.pair_misldeviceid_count_mislviewerid where count_mislviewerid > 1 and misldeviceid is not null ").collect(); sys.exit;
