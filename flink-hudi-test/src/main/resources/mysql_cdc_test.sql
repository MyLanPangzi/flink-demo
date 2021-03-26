
CREATE TABLE mysql_users (
                             id BIGINT PRIMARY KEY NOT ENFORCED ,
                             name STRING,
                             birthday TIMESTAMP(3),
                             ts TIMESTAMP(3)
) WITH (
      'connector' = 'mysql-cdc',
      'hostname' = 'yh002',
      'port' = '3306',
      'username' = 'root',
      'password' = 'Yh002Yh002@',
      'server-time-zone' = 'Asia/Shanghai',
      'database-name' = 'flink',
      'table-name' = 'users'
      );

CREATE TABLE hudi_users
(
    id BIGINT PRIMARY KEY NOT ENFORCED,
    name STRING,
    birthday TIMESTAMP(3),
    ts TIMESTAMP(3),
    `partition` VARCHAR(20)
) PARTITIONED BY (`partition`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'path' = 'hdfs://yh001:9820/hudi/hudi_users',
    'write.tasks' = '1',
    'read.tasks' = '1',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '1'
);

CREATE TABLE print
(
    id BIGINT PRIMARY KEY NOT ENFORCED,
    name STRING,
    birthday TIMESTAMP(3),
    ts TIMESTAMP(3),
    `partition` VARCHAR(20)
) WITH('connector'='print');

INSERT INTO hudi_users SELECT *, DATE_FORMAT(birthday, 'yyyy/MM/dd') FROM mysql_users;

-- first, comment this, uncomment after insert, running again
-- INSERT INTO print SELECT * FROM mysql_users;
INSERT INTO print SELECT * FROM hudi_users;
