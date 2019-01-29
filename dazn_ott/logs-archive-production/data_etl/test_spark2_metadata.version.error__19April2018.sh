#!/usr/bin/env bash


# Additional for Spark container 

MASTER_URL=yarn
DEPLOY_MODE=client

NUM_EXECUTORS=136
DRIVER_MEMORY=4g
EXECUTOR_MEMORY=2G
EXECUTOR_CORES=4

# Additional Logic workarround for new null token DI



#bash -c "echo ' spark.sql(\"USE published_ott_dazn\") ; val df1=spark.read.parquet(\"/data/staged/ott_dazn/docomo_investigations/logs-archive-production/parquet/dt=20180419\"); df1.printSchema(); df1.registerTempTable(\"massive_elb_logs_temp\") ; spark.sql(\"INSERT INTO TABLE published_ott_dazn.massive_elb_logs PARTITION (dt=20180419) SELECT metadata(BEAT,TYPE),timestamp,logzio_id,beat,input_type,it,logzio_codec,message,offset,source,tags,token,type  FROM massive_elb_logs_temp \")  ;  sys.exit ' | spark2-shell --master ${MASTER_URL} --deploy-mode ${DEPLOY_MODE}  --num-executors ${NUM_EXECUTORS}  --driver-memory ${DRIVER_MEMORY} --executor-memory ${EXECUTOR_MEMORY} --executor-cores ${EXECUTOR_CORES} 2>out_metadata.version.error.2.log&"

bash -c "echo ' spark.sql(\"USE published_ott_dazn\") ; val df1=spark.read.parquet(\"/data/staged/ott_dazn/docomo_investigations/logs-archive-production/parquet/dt=20180419\"); df1.printSchema();  sys.exit ' | spark2-shell --master ${MASTER_URL} --deploy-mode ${DEPLOY_MODE}  --num-executors ${NUM_EXECUTORS}  --driver-memory ${DRIVER_MEMORY} --executor-memory ${EXECUTOR_MEMORY} --executor-cores ${EXECUTOR_CORES} 2>out_metadata.version.error.2.log&"
