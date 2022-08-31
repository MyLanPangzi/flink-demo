SET execution.checkpointing.interval=1s;

CREATE TABLE mysql_t
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
    'prepare-statement.1.fields' = 'uid,f1',
    'prepare-statement.2.fields' = 'uid,f2'
--     'sink.buffer-flush.interval' = '10s',
--     'sink.buffer-flush.max-rows' = '1000'

);

CREATE TABLE test
(
    uid                 bigint,
    f1                  bigint,
    f2                  bigint,
    prepareStatementTag STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'test',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
 );

-- SELECT * FROM test;

INSERT INTO mysql_t
SELECT 1, 1, CAST(null AS BIGINT), '1'
UNION ALL
SELECT 1, CAST(null AS BIGINT), 1, '2';

