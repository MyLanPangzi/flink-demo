# flink-demo

sql 代码混合执行

```sql

call com.hiscat.flink.cdc.function.MysqlCdcSourceRegister;

CREATE TABLE ods_binlog_default
(
    `key`   STRING,
    `value` STRING,
    `topic` STRING METADATA FROM 'topic'
) WITH (
    'connector' = 'route-kafka',
    'topic' = 'ods_binlog_default',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'raw',
    'key.fields' = 'key',
    'value.format' = 'raw',
    'value.fields-include' = 'EXCEPT_KEY',
    'properties.compression.type' = 'gzip',
    'properties.linger.ms' = '1000'
 );

INSERT INTO ods_binlog_default
SELECT `key`, `value`, get_topic(`db`, `table`)
FROM cdc;

```