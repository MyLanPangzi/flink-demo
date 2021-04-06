-- file:///E:/github/flink-demo/flink-iceberg-test/src/main/resources/test.sql
-- SET table.exec.mini-batch.enabled=true;
-- SET table.exec.mini-batch.allow-latency=5 s;
-- SET table.exec.mini-batch.size=5000;
SET table.exec.resource.default-parallelism=1;
SET table.dynamic-table-options.enabled=true;
SET table.local-time-zone=Asia/Shanghai;

CREATE TABLE mysql_users
(
    id bigint primary key NOT ENFORCED ,
    name varchar(20),
    birthday timestamp ,
    ts timestamp
) WITH (
 'connector' = 'mysql-cdc',
 'hostname' = 'yh002',
 'port' = '3306',
 'username' = 'root',
 'password' = 'Yh002Yh002@',
 'database-name' = 'flink',
 'table-name' = 'users'
);

DROP TABLE IF EXISTS iceberg.`default`.users;
CREATE TABLE iceberg.`default`.users WITH(
    'property-formatVersion' = '2'
) LIKE mysql_users(
    EXCLUDING ALL
    INCLUDING CONSTRAINTS
    );

CREATE TABLE print WITH('connector' = 'print') LIKE mysql_users(EXCLUDING ALL);
INSERT INTO iceberg.`default`.users SELECT * FROM mysql_users;
-- INSERT INTO print SELECT * FROM iceberg.`default`.users /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ;
