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
      'properties.group.id' = 'testGroup',
      'key.format' = 'csv',
--     'key.fields-prefix' = '',
      'value.format' = 'csv',
      'value.fields-include' = 'ALL'
      );

CREATE TABLE datagen(
    WATERMARK FOR ts AS ts
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '3',
      'fields.id.min' = '1',
      'fields.id.max' = '10',
      'fields.name.length' = '6'
      ) LIKE test (EXCLUDING ALL);

INSERT INTO test
SELECT id, name, ts
FROM (
         SELECT *, ROW_NUMBER() over (PARTITION BY id ORDER BY ts DESC) rownum
         FROM datagen
     )t
WHERE t.rownum = 1;
