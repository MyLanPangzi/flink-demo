SET hostname=localhost;
SET port=3306;
SET username=root;
SET password=!QAZ2wsx;
SET database-name=cdc;
SET server-id=10000-10100;
SET scan.incremental.snapshot.chunk.size=80960;
SET split-key.even-distribution.factor.upper-bound=2.0;
SET scan.newly-added-table.enabled=true;
SET debezium.bigint.unsigned.handling.mode=long;
SET execution.checkpointing.interval=3s;

SET table-topic-mapping.cdc.t_withdraws_new_user_active_v3=t_withdraws_new_user_active_v3;
SET table-topic-mapping.cdc.test=test;

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
