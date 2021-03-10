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

CREATE TABLE test (
    id BIGINT PRIMARY KEY NOT ENFORCED,
    name STRING,
    ts BIGINT
) WITH (
  'connector' = 'elasticsearch-6',
  'hosts' = 'http://yh001:9200',
  'document-id.key-delimiter' = '_',
--   'username' = 'test',
--   'password' = 'test',
--   'sink.flush-on-checkpoint' = 'true',
--   'sink.bulk-flush.max-actions' = '1000',
--   'sink.bulk-flush.max-size' = '2mb',
--   'sink.bulk-flush.interval' = '1s',
  'sink.bulk-flush.backoff.strategy' = 'EXPONENTIAL', -- CONSTANT
  'sink.bulk-flush.backoff.max-retries' = '10',
  'sink.bulk-flush.backoff.delay' = '500ms',
  'connection.max-retry-timeout' = '5s',
--   'connection.path-prefix' = '',
--   'failure-handler' = 'retry_rejected', -- ignore
--   'index' = 'test_${ts}',
--   'index' = 'test_${ts|yyyy-MM-dd}',
  'index' = 'test',
  'document-type' = '_doc'
);

CREATE TABLE datagen WITH(
    'connector' = 'datagen',
    'rows-per-second' = '10'
) LIKE test(EXCLUDING ALL);

INSERT INTO test
SELECT *
FROM datagen;
