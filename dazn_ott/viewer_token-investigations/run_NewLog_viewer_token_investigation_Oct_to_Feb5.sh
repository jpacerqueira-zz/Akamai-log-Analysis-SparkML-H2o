bash -x run_spark2-shell_as_script.sh  runDec12Feb06_viewer_token_investigation.scala
sleep 1800
hdfs dfs -rm -r -f -skipTrash /data/staged/ott_dazn/pair_misldeviceid_count_mislviewerid/parquet/*
bash -x run_spark2-shell_as_script.sh  run_groupDeviceID_ViewerID_investigation.scala
sleep 3600
bash -x run_spark2-shell_as_script.sh  run_createtable_pair_misldeviceid_count_mislviewerid.scala
sleep 1800
bash -x run_spark2-shell_as_script.sh   run_investigate_device_double_used3.scala
sleep 1800
bash -x run_spark2-shell_as_script.sh   run_investigate_device_double_used4.scala
