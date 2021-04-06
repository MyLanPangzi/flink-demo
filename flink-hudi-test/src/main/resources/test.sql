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

CREATE TABLE  orders(
    id BIGINT PRIMARY KEY NOT ENFORCED,
    uid BIGINT,
    name VARCHAR(20),
    amount DECIMAL(10, 2),
    ts TIMESTAMP(3)
) WITH (
 'connector' = 'mysql-cdc',
 'hostname' = 'yh002',
 'port' = '3306',
 'username' = 'root',
 'password' = 'Yh002Yh002@',
 'server-time-zone' = 'Asia/Shanghai',
 'database-name' = 'flink',
 'table-name' = 'orders'
);

CREATE TABLE hudi_orders
(
    id BIGINT PRIMARY KEY NOT ENFORCED,
    uid BIGINT,
    name VARCHAR(20),
    amount DECIMAL(10, 2),
    ts TIMESTAMP(3),
    `partition` VARCHAR(20)
) PARTITIONED BY (`partition`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'path' = 'hdfs://yh001:9820/hudi/orders',
    'write.tasks' = '1',
    'read.tasks' = '1',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '10'
);

CREATE TABLE print
(
    uid BIGINT,
    amount DECIMAL(10, 2)
) WITH('connector'='print');

INSERT INTO hudi_orders SELECT *, DATE_FORMAT(ts, 'yyyy/MM/dd') FROM orders;

-- first, comment this, uncomment after insert, running again
-- INSERT INTO print SELECT * FROM mysql_users;
INSERT INTO print SELECT uid, SUM(amount) FROM hudi_orders GROUP BY uid;
