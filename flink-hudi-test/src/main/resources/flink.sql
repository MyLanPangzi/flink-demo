CREATE TABLE t3
(
    uuid        VARCHAR(20),
    weight      DOUBLE,
    ts          VARCHAR(20),
    `partition` VARCHAR(20)
) PARTITIONED BY (`partition`) WITH (
    'connector' = 'hudi',
    'path' = 'hdfs://nameservice1/user/xiebo/hudi/t3',
    'write.tasks' = '1',
    'read.tasks' = '1'
);

INSERT INTO t3  VALUES ('id1',  23.0,  '1970-01-01 00:00:01', 'par1');
