SET execution.checkpointing.interval=10s;
SET table.exec.resource.default-parallelism=1;

CREATE TABLE test
(

    `id`   bigint PRIMARY KEY NOT ENFORCED,
    `name` varchar(20) ,
    `ts`   timestamp
) WITH (
     'connector' = 'mysql-cdc',
     'hostname' = 'localhost',
     'username' = 'root',
     'password' = '!QAZ2wsx',
     'database-name' = 'test',
     'table-name' = 'test');

CREATE TABLE p WITH('connector' = 'print')LIKE test(EXCLUDING ALL);

INSERT INTO p
SELECT *
FROM test;