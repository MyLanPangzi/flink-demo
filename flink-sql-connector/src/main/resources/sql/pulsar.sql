--sql.path /Users/xiebo/IdeaProjects/flink-demo/flink-sql-connector/src/main/resources/sql/pulsar.sql
SET table.exec.resource.default-parallelism=1;
SET table.local-time-zone=Asia/Shanghai;
SET table.exec.source.cdc-events-duplicate=true;

CREATE TABLE test
(
    id          INT PRIMARY KEY,
    name        STRING,
    create_time TIMESTAMP(3)
) WITH (
 'connector' = 'mysql-cdc',
 'hostname' = 'localhost',
 'port' = '3306',
 'username' = 'root',
 'password' = '000000',
 'database-name' = 'test',
 'table-name' = 'test'
);

CREATE TABLE pulsar_test WITH (
  'connector' = 'upsert-pulsar',
  'topic' = 'persistent://public/default/test_json_4',
  'key.format' = 'json',
  'value.format' = 'json',
  'service-url' = 'pulsar://localhost:6650',
  'admin-url' = 'http://localhost:8080'
--   'scan.startup.mode' = 'earliest'
)LIKE test(EXCLUDING ALL INCLUDING CONSTRAINTS);

CREATE TABLE kafka_test WITH (
      'connector' = 'upsert-kafka',
      'properties.bootstrap.servers' = 'dev:9092',
      'properties.group.id' = 'test',
      'topic' = 'test2',
      'key.format' = 'json',
      'value.format' = 'json'
      )LIKE test(EXCLUDING ALL INCLUDING CONSTRAINTS);

CREATE TABLE print WITH('connector' = 'print') LIKE test(EXCLUDING ALL);

INSERT INTO pulsar_test
SELECT *
FROM test;

INSERT INTO print
SELECT *
FROM pulsar_test;
