--sql.path file:///E:/github/flink-demo/flink-sql-connector/src/main/resources/sql/test.sql
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

CREATE TABLE test
(
    id          BIGINT,
    name        STRING,
    ts          BIGINT,
    `timestamp` TIMESTAMP(3) METADATA ,
    `headers` 	MAP<STRING, BYTES> METADATA
) WITH (
  'connector' = 'kafka',
  'topic' = 'test',
  'properties.bootstrap.servers' = 'yh001:9092',
  'properties.group.id' = 'testGroup',
  'format' = 'csv'
);

CREATE TABLE datagen WITH (
      'connector' = 'datagen',
      'rows-per-second' = '1',
      'fields.id.min' = '1',
      'fields.id.max' = '90000',
      'fields.name.length' = '6'
) LIKE test (EXCLUDING ALL);

INSERT INTO test
SELECT id, name, UNIX_TIMESTAMP() * 1000, PROCTIME(), MAP['hello', ENCODE('wolrd','UTF-8')]
FROM datagen;
