-- https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/upsert-kafka.html
CREATE TABLE test
(
    id          BIGINT,
    name        STRING,
    ts          TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'test',
      'properties.bootstrap.servers' = 'yh001:9092',
      'properties.group.id' = 'testGroup1',
      'key.format' = 'csv',
--     'key.fields-prefix' = '',
      'value.format' = 'csv',
      'value.fields-include' = 'ALL'
      );

CREATE TABLE print WITH('connector' = 'print') LIKE test(EXCLUDING ALL);

INSERT INTO print
SELECT * FROM test;