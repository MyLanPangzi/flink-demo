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

INSERT INTO test
SELECT 4, 'iceberg', UNIX_TIMESTAMP();
