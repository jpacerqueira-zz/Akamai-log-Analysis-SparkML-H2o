spark.sql("USE published_ott_dazn");
spark.sql(" INSERT INTO published_ott_dazn.misldeviceid_mislviewerid_status_token  select distinct date,misldeviceid,mislviewerid,misluserstatus, split(correlationtoken,'\\u124')[8] as correlationid  from published_ott_dazn.viewer_token_investigation where date > '2017-12-11' and date < '2018-02-06'  and misldeviceid IS NOT NULL and misluserstatus IS NOT NULL ").collect();
sys.exit;

