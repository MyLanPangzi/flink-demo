CREATE TABLE test
(
    id             BIGINT,
    name           STRING,
    ts             BIGINT,
    `timestamp`    TIMESTAMP(3) METADATA VIRTUAL,
    `headers`      MAP<STRING, BYTES> METADATA VIRTUAL,
    topic          STRING METADATA VIRTUAL,
    `partition`    INT METADATA VIRTUAL,
    leader_epoch   INT METADATA FROM 'leader-epoch',
    `offset`       BIGINT METADATA VIRTUAL,
    timestamp_type STRING METADATA FROM 'timestamp-type'
) WITH (
      'connector' = 'kafka',
      'topic' = 'test',
      'properties.bootstrap.servers' = 'yh001:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'earliest-offset', -- latest-offset earliest-offset timestamp specific-offsets
      'scan.topic-partition-discovery.interval' = '10s', -- latest-offset earliest-offset timestamp specific-offsets
--   'scan.startup.timestamp-millis' = '10000',
--   'scan.startup.specific-offsets' = 'partition:0,offset:0',
      'format' = 'csv'
      );

CREATE TABLE print
(
    id             BIGINT,
    name           STRING,
    ts             BIGINT,
    `timestamp`    TIMESTAMP(3),
    `headers`      MAP<STRING, BYTES>,
    world          STRING,
    topic          STRING,
    `partition`    INT,
    leader_epoch   INT,
    `offset`       BIGINT,
    timestamp_type STRING
) WITH ( 'connector' = 'print');

INSERT INTO print
SELECT id,
       name,
       ts,
       `timestamp`,
       headers,
       DECODE(headers['hello'], 'UTF-8'),
       topic,
       `partition`,
       leader_epoch,
       `offset`,
       timestamp_type
FROM test;
