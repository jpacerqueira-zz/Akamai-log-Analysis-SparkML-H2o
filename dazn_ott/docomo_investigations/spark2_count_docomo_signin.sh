#!/usr/bin/env bash

# Additional for Spark container 

MASTER_URL=yarn
DEPLOY_MODE=client

bash -c "echo ' spark.sql(\"USE published_ott_dazn\") ; val  count_signin_1100_1110=spark.sql(\" SELECT count(*) as counter , 'SignIn_1100_1110'  from published_ott_dazn.massive_elb_logs where dt=20170617 and timestamp between '2017-06-17T11:00:00.000+0000' and '2017-06-17T11:10:00.000+0000' and message like '%/jp/v1/Docomo/SignIn%' group by dt \").collect() ; sys.exit ' | spark2-shell --master ${MASTER_URL} --deploy-mode ${DEPLOY_MODE} 2>1&"
#
