#!/usr/bin/env bash
#
#  Used only once to create table in Apache Hive with Spark2-shell default Parquet encoding
#
#
bash -c "echo ' spark.sql("USE published_ott_dazn ") ; spark.sql(\" CREATE TABLE published_ott_dazn.docomo_daily_counters  USING PARQUET PARTITIONED BY (dt)   AS  SELECT  dt, count(*) as counter, 'PinPair' as type_rec from published_ott_dazn.massive_elb_logs where dt between 20170301 and 20170331 and message like '%/jp/v1/Docomo/PinPair%' group by dt \") ; sys.exit ' | spark2-shell 2>1&"
#
# 
