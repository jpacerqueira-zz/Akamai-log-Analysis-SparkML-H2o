spark.sql("USE published_ott_dazn");
spark.sql(" DROP TABLE published_ott_dazn.misldeviceid_mislviewerid_status_token ") ;
spark.sql(" CREATE TABLE published_ott_dazn.misldeviceid_mislviewerid_status_token  select distinct date,misldeviceid,mislviewerid,misluserstatus, split(correlationtoken,'\\u124')[8] as correlationid  from published_ott_dazn.viewer_token_investigation where date > '2017-10-01' and date < '2017-12-12'  and misldeviceid IS NOT NULL and misluserstatus IS NOT NULL ").collect(); 
sys.exit;
