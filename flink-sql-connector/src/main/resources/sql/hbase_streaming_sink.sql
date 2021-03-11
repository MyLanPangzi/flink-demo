-- https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/elasticsearch.html
CREATE TABLE test (
                      id INT,
                      cf ROW(
                          name STRING,
                          ts BIGINT
                          ),
                      PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'hbase-2.2',
      'table-name' = 'default:test',
      'sink.parallelism' = '2',
--  'zookeeper.znode.parent' = '/hbase',
--  'null-string-literal' = 'null',
--   'sink.bulk-flush.max-actions' = '1000',
--   'sink.bulk-flush.max-size' = '2mb',
--   'sink.bulk-flush.interval' = '1s',
      'zookeeper.quorum' = 'yh001:2181'
      );
CREATE TABLE datagen WITH(
                         'connector' = 'datagen',
                         'rows-per-second' = '1'
                         ) LIKE test(EXCLUDING ALL);


INSERT INTO test
SELECT * FROM datagen;