CREATE TABLE t1
(
    uuid        VARCHAR(20),
    name        VARCHAR(10),
    age         INT,
    ts          TIMESTAMP(3),
    `partition` VARCHAR(20)
) PARTITIONED BY (`partition`) WITH (
    'connector' = 'hudi',
    'path' = 'hdfs://yh001:9820/hudi/',
    'write.tasks' = '1',
    'read.tasks' = '1'
);

INSERT INTO t1
VALUES ('id1', 'Danny', 23, TIMESTAMP '1970-01-01 00:00:01', 'par1'),
       ('id2', 'Stephen', 33, TIMESTAMP '1970-01-01 00:00:02', 'par1'),
       ('id3', 'Julian', 53, TIMESTAMP '1970-01-01 00:00:03', 'par2'),
       ('id4', 'Fabian', 31, TIMESTAMP '1970-01-01 00:00:04', 'par2'),
       ('id5', 'Sophia', 18, TIMESTAMP '1970-01-01 00:00:05', 'par3'),
       ('id6', 'Emma', 20, TIMESTAMP '1970-01-01 00:00:06', 'par3'),
       ('id7', 'Bob', 44, TIMESTAMP '1970-01-01 00:00:07', 'par4'),
       ('id8', 'Han', 56, TIMESTAMP '1970-01-01 00:00:08', 'par4');

CREATE TABLE print(
                      uuid        VARCHAR(20),
                      name        VARCHAR(10),
                      age         INT,
                      ts          TIMESTAMP(3),
                      `partition` VARCHAR(20)
) WITH('connector'='print');

-- first, comment this
-- INSERT INTO print SELECT * FROM t1;
-- uncomment above sql after insert