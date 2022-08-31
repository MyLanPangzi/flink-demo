SET execution.checkpointing.interval=3s;
-- insert into f1 (id, f1) values (1,1);
-- delete from f1 where 1;

CREATE TABLE f1
(
    `id`       bigint,
    `f1`       bigint,

    db_name    STRING METADATA FROM 'database_name' VIRTUAL,
    table_name STRING METADATA FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    proctime AS PROCTIME(),
    primary key (id) NOT ENFORCED
)WITH (
    'connector' = 'mysql-cdc',
     'hostname' = 'localhost',
     'port' = '3306',
     'server-id'='5601-5700',
    'username' = 'root',
    'password' = '!QAZ2wsx',
    'database-name' = 'cdc',
    'table-name' = 'f1',
    'split-key.even-distribution.factor.upper-bound' = '2.0',
    'debezium.min.row.count.to.stream.result' = '10240',
    'scan.snapshot.fetch.size' = '10240',
    'scan.incremental.snapshot.chunk.size' = '80960',
    'debezium.bigint.unsigned.handling.mode' = 'long',
    'debezium.decimal.handling.mode' = 'double'
);
CREATE TABLE cdc_f1
(
    `user_id` BIGINT PRIMARY KEY NOT ENFORCED,
    `f1`      BIGINT
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'cdc_f1',
  'properties.bootstrap.servers' = 'localhost:9092',
  'value.format' = 'json',
  'value.fields-include' = 'EXCEPT_KEY',
  'key.format' = 'json'
);

INSERT INTO cdc_f1
SELECT id, COUNT(*)
FROM f1
GROUP BY id;