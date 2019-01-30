spark.sql("USE published_ott_dazn"); spark.sql("select * from published_ott_dazn.pair_misldeviceid_count_mislviewerid where count_mislviewerid > 1 and misldeviceid is not null ").collect(); sys.exit;
# spark.sql("select distinct misldeviceid,mislviewerid from published_ott_dazn.viewer_token_investigation where misldeviceid in ('0012400000mFrENAA0-004CE09825','0012400000kXfH1AAK-002594E3D6') 
