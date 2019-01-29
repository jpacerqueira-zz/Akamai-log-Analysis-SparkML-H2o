#!/usr/bin/env bash
#
#  Used only once to create table in Apache Hive with Spark2-shell default Parquet encoding
#
#
bash -c "echo ' spark.sql(\"USE published_ott_dazn \") ; spark.read.parquet(\"/data/staged/ott_dazn/docomo_investigations/logs-archive-production/parquet/\").registerTempTable(\"massive_elb_logs_temp\") ; spark.sql(\"CREATE TABLE published_ott_dazn.massive_elb_logs USING PARQUET PARTITIONED BY (dt) AS SELECT * FROM massive_elb_logs_temp where dt=20180423\") ; sys.exit ' | spark2-shell 2>1&"
#
# 
