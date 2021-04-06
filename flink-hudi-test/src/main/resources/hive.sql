add jar hdfs://nameservice1/user/xiebo/hudi-hadoop-mr-bundle-0.9.0-SNAPSHOT.jar;
drop table if exists t3;
CREATE EXTERNAL TABLE t3
(
    `_hoodie_commit_time` string,
    `_hoodie_commit_seqno` string,
    `_hoodie_record_key` string,
    `_hoodie_partition_path` string,
    `_hoodie_file_name` string,
    uuid STRING,
    name STRING,
    ts   STRING
) partitioned by (`partition` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hudi.hadoop.HoodieParquetInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION 'hdfs://nameservice1/user/xiebo/hudi/t3';
ALTER TABLE t3
    ADD
        PARTITION (`partition` = 'par1') LOCATION 'hdfs://nameservice1/user/xiebo/hudi/t3/par1';
show partitions t3;
SELECT * FROM t3;
