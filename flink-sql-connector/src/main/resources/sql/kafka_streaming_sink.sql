-- https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/kafka.html
CREATE TABLE test
(
    id          BIGINT,
    name        STRING,
    ts          BIGINT,
    `timestamp` TIMESTAMP(3) METADATA,
    `headers`   MAP<STRING, BYTES> METADATA
) WITH (
      'connector' = 'kafka',
      'topic' = 'test',
      'properties.bootstrap.servers' = 'yh001:9092',
      'properties.group.id' = 'testGroup',
--   'sink.partitioner' = 'default',
--       'sink.semantic' = 'at-least-once',
--       'sink.parallelism' = '1',
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
SELECT id, name, UNIX_TIMESTAMP() * 1000, PROCTIME(), MAP['hello', ENCODE('world', 'UTF-8') ]
FROM datagen;
