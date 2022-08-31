CREATE TABLE cdc_f1
(
    `topic`   STRING METADATA VIRTUAL,
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
CREATE TABLE test
(
    uid                 bigint primary key not enforced,
    f1                  bigint,
    f2                  bigint,
    prepareStatementTag STRING METADATA
) WITH(
    'connector' = 'partial-jdbc',
    'url' = 'jdbc:mysql://localhost:3306/cdc',
    'table-name' = 'test',
    'username' = 'root',
    'password' = '!QAZ2wsx',
    'prepare-statement.cdc_f1.fields' = 'uid,f1',
    'prepare-statement.f2.fields' = 'uid,f2',
    'sink.buffer-flush.interval' = '1s',
    'sink.buffer-flush.max-rows' = '10000'
);

INSERT INTO test
SELECT user_id, f1, CAST(NULL AS BIGINT), topic
FROM cdc_f1;