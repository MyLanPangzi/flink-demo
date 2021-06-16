CREATE TABLE dg
(
    id BIGINT,
    name STRING,
    birthday TIMESTAMP_LTZ
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1'
);

CREATE TABLE KafkaTable  WITH (
  'connector' = 'kafka',
  'topic' = 'test',
  'properties.bootstrap.servers' = 'dev:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv'
) LIKE dg (EXCLUDING ALL);

CREATE TABLE print  WITH (
  'connector' = 'print'
) LIKE dg (EXCLUDING ALL);



