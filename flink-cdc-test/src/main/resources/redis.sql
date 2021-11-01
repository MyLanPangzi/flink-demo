SET sql-client.execution.result-mode=TABLEAU;
SET execution.checkpointing.interval = 3s;

CREATE TABLE redis_users
(
    id   STRING PRIMARY KEY NOT ENFORCED,
    name STRING
) WITH (
     'connector' = 'redis',
     'host' = 'dev',
     'sink.key-prefix' = 'u',
    ''
    );

CREATE VIEW users
AS
SELECT 1 AS user_id, 'hello' AS name;

INSERT INTO redis_users
SELECT CAST(user_id AS STRING), name
FROM users;

CREATE VIEW orders
AS
SELECT 1          AS order_id,
       1          AS user_id,
       1          AS price,
       PROCTIME() AS proctime;

SELECT * FROM orders o
LEFT JOIN redis_users FOR SYSTEM_TIME AS OF o.proctime AS u ON CAST(o.user_id AS STRING) = u.id;
