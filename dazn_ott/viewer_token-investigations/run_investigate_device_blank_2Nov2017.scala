spark.sql("USE published_ott_dazn");
spark.sql("CREATE TABLE published_ott_dazn.blank_device_2Nov2017 select dt,metadata,timestamp,logzio_id,beat,input_type,it,logzio_codec,message,offset,source,tags,token,type FROM  published_ott_dazn.massive_elb_logs where dt in ('20171102') and message like '%Misl_Viewer_Id:%Misl_Device_Id: %' order by timestamp asc  limit 10000 ").collect(); 
sys.exit;
