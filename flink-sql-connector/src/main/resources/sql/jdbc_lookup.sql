
CREATE TABLE users
(
    id   BIGINT PRIMARY KEY NOT ENFORCED,
    name VARCHAR(30)
)WITH (
     'connector' = 'jdbc',
     'username' = 'root',
     'password' = 'Hdp10Hdp10@',
     'url' = 'jdbc:mysql://hdp10:3306/flink',
     'driver' = 'com.mysql.cj.jdbc.Driver',
     'lookup.cache.max-rows' = '10',
     'lookup.cache.ttl' = '10 s',
     'table-name' = 'users'
     );

CREATE TABLE orders
(
    id   BIGINT PRIMARY KEY NOT ENFORCED,
    name STRING,
    uid  BIGINT,
    proctime AS PROCTIME()
) WITH(
      'connector' = 'datagen',
      'rows-per-second' = '10',
      'fields.id.kind' = 'random',
      'fields.uid.kind' = 'random',
      'fields.uid.min' = '1',
      'fields.uid.max' = '10',
      'fields.name.length' = '6'
      );

CREATE TABLE print
(
    id         BIGINT PRIMARY KEY NOT ENFORCED,
    uid        BIGINT,
    order_name STRING,
    user_name  STRING
) WITH('connector' = 'print');

INSERT INTO print
SELECT o.id, u.id, o.name, u.name
FROM orders o
LEFT JOIN users FOR SYSTEM_TIME AS OF o.proctime AS u ON u.id = o.uid;