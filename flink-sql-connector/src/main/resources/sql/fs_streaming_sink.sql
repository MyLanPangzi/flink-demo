-- env.enableCheckpointing(10*1000)
CREATE TABLE fs
(
    id   BIGINT,
    name STRING,
    ts   BIGINT,
    dt   STRING,
    h    BIGINT,
    m    BIGINT
) PARTITIONED BY (dt, h, m) WITH  (
    'connector'='filesystem',
    'path'='file:///E:/github/flink-demo/fs',
    'format'='csv',
    'sink.rolling-policy.file-size' = '100m',
    'sink.rolling-policy.rollover-interval' = '10 s',
    'sink.rolling-policy.check-interval' = '1 s',
    'auto-compaction'='true',
    'compaction.file-size'='128m',
    'partition.default-name' = 'default',
    'partition.time-extractor.kind' = 'default',
    'partition.time-extractor.timestamp-pattern' = 'yyyy-mm-dd hh:mm',
--     'sink.partition-commit.trigger'='partition-time',
    'sink.partition-commit.trigger'='process-time',
    'sink.shuffle-by-partition.enable' = 'false',
    'sink.partition-commit.delay'='1min',
    'sink.partition-commit.policy.kind'='success-file'
);
CREATE TABLE datagen
(
    id   BIGINT,
    name STRING,
    ts   BIGINT
) WITH(
      'connector' = 'datagen'
      );
INSERT INTO fs
SELECT id,
       name,
       UNIX_TIMESTAMP() * 1000,
       DATE_FORMAT(NOW(), 'yyyy-MM-dd'),
    HOUR(NOW()),
    MINUTE(NOW())
FROM datagen;