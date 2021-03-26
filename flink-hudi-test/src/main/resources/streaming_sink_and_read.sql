CREATE TABLE t2
(
    uuid        VARCHAR(20),
    name        VARCHAR(10),
    age         INT,
    ts          TIMESTAMP(3),
    `partition` VARCHAR(20)
) PARTITIONED BY (`partition`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'path' = 'hdfs://yh001:9820/hudi/t2',
    'write.tasks' = '1',
    'read.tasks' = '1',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '1'
);

CREATE TABLE datagen
(
    uuid VARCHAR(20),
    name VARCHAR(10),
    age  INT,
    ts   TIMESTAMP(3)
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '1',
      'fields.uuid.length' = '6',
      'fields.name.length' = '6',
      'fields.age.max' = '20',
      'fields.age.min' = '10'
      );

CREATE TABLE print
(
    uuid        VARCHAR(20),
    name        VARCHAR(10),
    age         INT,
    ts          TIMESTAMP(3),
    `partition` VARCHAR(20)
) WITH('connector'='print');

INSERT INTO t2
SELECT *, DATE_FORMAT(ts, 'yyyy/MM/dd')
FROM datagen;

-- first, comment this
INSERT INTO print
SELECT *
FROM t2;
-- uncomment above sql after insert, running again