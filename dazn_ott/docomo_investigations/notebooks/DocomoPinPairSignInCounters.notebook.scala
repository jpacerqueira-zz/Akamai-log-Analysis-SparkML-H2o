
import org.apache.spark.sql.hive.HiveContext;
val sqlContext = new HiveContext(sc) ;
sqlContext.sql(" USE published_ott_dazn ").collect() ;


sqlContext.sql(" SHOW tables  ").collect() ;

//sqlContext.sql(" CREATE TABLE published_ott_dazn.docomo_daily_counters (counter int,type_rec string)  USING PARQUET  PARTITIONED BY (dt)  CLUSTERED BY (dt) INTO 200 buckets  AS  SELECT  count(*) as counter, 'PinPair' as type_rec from published_ott_dazn.massive_elb_logs where dt=20170201 and message like '%/jp/v1/Docomo/PinPair%' group by dt ") ;
sqlContext.sql(" SELECT  count(*) as counter, 'PinPair' as type_rec from published_ott_dazn.massive_elb_logs where dt=20170202 and message like '%/jp/v1/Docomo/PinPair%'  group by dt ").collect() ;


sqlContext.sql(" SELECT count(*) as counter ,'SignIn_00:00_00:10' as type_rec from published_ott_dazn.massive_elb_logs where dt=20170617  and timestamp between '2017-06-17T00:00:00.000+0000' and '2017-06-17T00:10:00.000+0000' and message like '%/jp/v1/Docomo/SignIn%'  group by dt ").collect() ;

sqlContext.sql(" SELECT count(*) as counter ,'PinPair_00:00_00:10' as type_rec from published_ott_dazn.massive_elb_logs where dt=20170617  and timestamp between '2017-06-17T00:00:00.000+0000' and '2017-06-17T00:10:00.000+0000' and message like '%/jp/v1/Docomo/PinPair%'  group by dt ").collect() ;

sqlContext.sql(" SELECT count(*) as counter ,'Total_SignIn_00:00_+24:00' as type_rec from published_ott_dazn.massive_elb_logs where ( dt=20170617 or dt=20170618) and timestamp between '2017-06-17T00:00:00.000+0000' and '2017-06-18T00:00:00.000+0000' and message like '%/jp/v1/Docomo/SignIn%'  group by dt ").collect() ;

sqlContext.sql(" SELECT count(*) as counter ,'Total_PinPair_00:00_+24:00' as type_rec from published_ott_dazn.massive_elb_logs where ( dt=20170617 or dt=20170618) and timestamp between '2017-06-17T00:00:00.000+0000' and '2017-06-18T00:00:00.000+0000' and message like '%/jp/v1/Docomo/PinPair%'  group by dt ").collect() ;

sqlContext.sql(" SELECT count(*) as counter, 'SignIn' as type_rec from published_ott_dazn.massive_elb_logs where dt=20170629 and message like '%/jp/v1/Docomo/SignIn%'  group by dt ").collect() ;

sqlContext.sql(" SELECT count(*) as counter, 'SignIn' as type_rec from published_ott_dazn.massive_elb_logs where dt=20170630 and message like '%/jp/v1/Docomo/SignIn%'  group by dt ").collect() ;

sys.exit

