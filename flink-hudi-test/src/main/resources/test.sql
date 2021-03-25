--sql.path file:///E:/github/flink-demo/flink-hudi-test/src/main/resources/test.sql
SET table.exec.mini-batch.enabled=true;
SET table.exec.mini-batch.allow-latency=5 s;
SET table.exec.mini-batch.size=5000;
SET table.exec.resource.default-parallelism=1;
SET table.local-time-zone=Asia/Shanghai;
-- SET table.exec.source.idle-timeout=60000 ms;
-- SET table.exec.state.ttl=600000 ms;
-- SET table.optimizer.agg-phase-strategy=TWO_PHASE;
-- SET table.optimizer.distinct-agg.split.enabled=true;
-- SET table.generated-code.max-length=64000;
-- SET table.dynamic-table-options.enabled=true;
-- SET table.sql-dialect=hive;

CREATE TABLE t1
(
    uuid        VARCHAR(20),
    name        VARCHAR(10),
    age         INT,
    ts          TIMESTAMP(3),
    `partition` VARCHAR(20)
) PARTITIONED BY (`partition`) WITH (
    'connector' = 'hudi',
    'path' = 'hdfs://yh001:9820/hudi/t1',
    'write.tasks' = '1',
    'read.tasks' = '1'
);

INSERT INTO t1
VALUES ('id1', 'Danny', 23, TIMESTAMP '1970-01-01 00:00:01', '2021/03/25'),
       ('id2', 'Stephen', 33, TIMESTAMP '1970-01-01 00:00:02', '2021/03/24'),
       ('id3', 'Julian', 53, TIMESTAMP '1970-01-01 00:00:03', '2021/03/23'),
       ('id4', 'Fabian', 31, TIMESTAMP '1970-01-01 00:00:04', '2021/03/25'),
       ('id5', 'Sophia', 18, TIMESTAMP '1970-01-01 00:00:05', '2021/03/24'),
       ('id6', 'Emma', 20, TIMESTAMP '1970-01-01 00:00:06', '2021/03/23'),
       ('id7', 'Bob', 44, TIMESTAMP '1970-01-01 00:00:07', '2021/03/25'),
       ('id8', 'Han', 56, TIMESTAMP '1970-01-01 00:00:08', '2021/03/24');

CREATE TABLE print(
                      uuid        VARCHAR(20),
                      name        VARCHAR(10),
                      age         INT,
                      ts          TIMESTAMP(3),
                      `partition` VARCHAR(20)
) WITH('connector'='print');

-- first, comment this
-- INSERT INTO print SELECT * FROM t1;
-- uncomment above sql after insert, running again