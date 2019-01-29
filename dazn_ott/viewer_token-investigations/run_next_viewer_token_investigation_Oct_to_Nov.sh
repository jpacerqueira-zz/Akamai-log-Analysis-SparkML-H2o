bash -x run_spark2-shell_as_script.sh  runOctNov_viewer_token_investigation.scala
sleep 420
hdfs dfs -rm -r -f -skipTrash /data/staged/ott_dazn/pair_misldeviceid_count_mislviewerid/parquet/*
bash -x run_spark2-shell_as_script.sh  run_groupDeviceID_ViewerID_investigation.scala
sleep 420
bash -x run_spark2-shell_as_script.sh  run_createtable_pair_misldeviceid_count_mislviewerid.scala
sleep 420
bash -x run_spark2-shell_as_script.sh   run_investigate_device_double_used3.scala
