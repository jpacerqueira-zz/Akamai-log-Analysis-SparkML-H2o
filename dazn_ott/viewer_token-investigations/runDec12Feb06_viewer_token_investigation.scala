spark.sql("USE published_ott_dazn"); 
spark.sql("SHOW tables"); 
spark.sql("insert into table published_ott_dazn.viewer_token_investigation SELECT split(message,' ')[0] as date , split( split(message,' ')[1] ,',')[0]  as time,  split(  split(message,':')[14], ',')[0] as mislviewerid, split(  split(message,':')[15], ',')[0] as misldeviceid, split(  split(message,':')[16], '}')[0] as misluserstatus,split( split(split( message,',')[1], ' ')[0], '\\u124')[0] as correlationtoken from published_ott_dazn.massive_elb_logs where dt between '20171212' and '20180206' and message like '201%-%PROD%Claims%viewerId%Ext-%deviceId%userstatus%' ").collect();
sys.exit;
