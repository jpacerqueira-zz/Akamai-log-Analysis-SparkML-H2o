bash -x run_spark2-shell_as_script.sh create_viewer_token_investigation.scala
sleep 180
bash -x run_spark2-shell_as_script.sh  runFeb_create_viewer_token_investigation.scala
sleep 420
bash -x run_spark2-shell_as_script.sh  runMarch_viewer_token_investigation.scala
sleep 420
bash -x run_spark2-shell_as_script.sh  runApril_viewer_token_investigation.scala
sleep 420
bash -x run_spark2-shell_as_script.sh  runMay_viewer_token_investigation.scala
sleep 420
bash -x run_spark2-shell_as_script.sh  runJune_viewer_token_investigation.scala
sleep 420
bash -x run_spark2-shell_as_script.sh  runJuly_viewer_token_investigation.scala
sleep 420
basx -x run_spark2-shell_as_script.sh  runAugust_viewer_token_investigation.scala
sleep 420
bash -x run_spark2-shell_as_script.sh  runSep_viewer_token_investigation.scala
sleep 420
bash -x run_spark2-shell_as_script.sh  runOct_viewer_token_investigation.scala
sleep 420
hdfs dfs -rm -r -f -skipTrash /data/staged/ott_dazn/pair_misldeviceid_count_mislviewerid/parquet/*
bash -x run_spark2-shell_as_script.sh  run_groupDeviceID_ViewerID_investigation.scala
sleep 420
bash -x run_spark2-shell_as_script.sh  run_createtable_pair_misldeviceid_count_mislviewerid.scala
sleep 420
bash -x run_spark2-shell_as_script.sh   run_investigate_device_double_used3.scala
