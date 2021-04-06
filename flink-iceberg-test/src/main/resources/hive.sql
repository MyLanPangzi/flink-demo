CREATE EXTERNAL TABLE users
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
LOCATION 'hdfs://yh001:9820/iceberg/default/users';
