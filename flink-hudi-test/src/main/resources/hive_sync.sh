./run_sync_tool.sh  --jdbc-url jdbc:hive2:\/\/yh001:10000 \
--user hdp \
--pass '' \
--partitioned-by partition \
--base-path hdfs://yh001:9820/hudi/t2 \
--database default \
--table t2
